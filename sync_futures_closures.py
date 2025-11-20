#!/usr/bin/env python3
"""
sync_futures_closures.py

Sincroniza o FECHO de trades futures entre a Binance e a tabela 'signals' do Supabase.

Lógica:
- Procura sinais em 'signals' com:
    status = 'open'
    futures_entry_sent = true
    futures_symbol IS NOT NULL
- Para cada sinal:
    1) Verifica na Binance Futures (positionRisk) se ainda há posição nesse symbol.
       - Se ainda houver posição (positionAmt != 0): faz skip.
       - Se NÃO houver posição (positionAmt == 0):
            -> Vai buscar os userTrades para esse symbol a partir de created_at do sinal.
            -> Calcula:
                 - exit_price (VWAP dos trades de fecho)
                 - exit_time (timestamp do último trade de fecho)
                 - total_profit_usd (soma de realizedPnl dos trades dessa janela)
                 - profit_pct e r_multiple (usando entry e stop_pct do sinal)
            -> Atualiza a linha em 'signals':
                 status='closed', finalized=true,
                 exit_at, avg_exit_price, profit_pct, r_multiple, total_profit_usd,
                 exit_type/exit_level ('sl' ou 'manual', heurística pelo preço).

ENV necessários:

  SUPABASE_URL=...
  SUPABASE_SERVICE_ROLE=...

  SIGNALS_TABLE=signals             # opcional, default 'signals'

  BINANCE_FUTURES_API_KEY=...
  BINANCE_FUTURES_SECRET_KEY=...
  BINANCE_FUTURES_TESTNET=1         # 1 = demo-fapi.binance.com ; 0 = fapi.binance.com

  LOOP_SECONDS=10                   # intervalo entre varrimentos
"""

import os
import time
import hmac
import hashlib
import datetime as dt
from decimal import Decimal
from typing import Any, Dict, Optional, List, Tuple

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# --------------------------------------------------------------------------
# ENV & setup
# --------------------------------------------------------------------------
load_dotenv()

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
SUPABASE: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")

BINANCE_API_KEY = os.environ["BINANCE_FUTURES_API_KEY"]
BINANCE_SECRET_KEY = os.environ["BINANCE_FUTURES_SECRET_KEY"].encode()

BINANCE_TESTNET = os.getenv("BINANCE_FUTURES_TESTNET", "1") == "1"
if BINANCE_TESTNET:
    BINANCE_BASE_URL = "https://demo-fapi.binance.com"
    ENV_LABEL = "Binance FUTURES TESTNET"
else:
    BINANCE_BASE_URL = "https://fapi.binance.com"
    ENV_LABEL = "Binance FUTURES"

LOOP_SECONDS = int(os.getenv("LOOP_SECONDS", "10"))

REQUEST_TIMEOUT = 10


# --------------------------------------------------------------------------
# Helpers gerais
# --------------------------------------------------------------------------
def signed_request(method: str, path: str, params: Dict[str, Any]) -> requests.Response:
    """
    Faz uma chamada assinada à Binance Futures.
    Lança HTTPError se status != 2xx.
    """
    ts = int(time.time() * 1000)
    params["timestamp"] = ts

    query = "&".join(f"{k}={params[k]}" for k in sorted(params.keys()))
    signature = hmac.new(
        BINANCE_SECRET_KEY,
        query.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    query_with_sig = f"{query}&signature={signature}"
    url = f"{BINANCE_BASE_URL}{path}?{query_with_sig}"

    headers = {
        "X-MBX-APIKEY": BINANCE_API_KEY,
    }

    resp = requests.request(
        method=method,
        url=url,
        headers=headers,
        timeout=REQUEST_TIMEOUT,
    )

    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        print(f"Binance error: {resp.status_code} {resp.text}")
        raise

    return resp


def parse_iso_to_ms(iso_str: str) -> int:
    """
    Converte um timestamptz ISO vindo do Supabase para epoch ms (UTC).
    Aceita strings tipo '2025-11-19T18:20:00+00:00' ou '2025-11-19 18:20:00+00'.
    """
    s = iso_str.replace(" ", "T")
    if s.endswith("Z"):
        dt_obj = dt.datetime.fromisoformat(s.replace("Z", "+00:00"))
    else:
        if "+" not in s and "-" in s[10:]:
            # Falta timezone -> assume UTC
            s = s + "+00:00"
        dt_obj = dt.datetime.fromisoformat(s)
    return int(dt_obj.timestamp() * 1000)


def normalize_futures_symbol(spot_symbol: str) -> str:
    """
    Converte símbolo tipo 'SOL/USDT' em 'SOLUSDT', etc.
    """
    s = spot_symbol.replace("-", "").upper()
    if "/" in s:
        base, quote = s.split("/")
    else:
        if s.endswith("USDT") or s.endswith("USDC"):
            base = s[:-4]
            quote = s[-4:]
        else:
            raise ValueError(f"Invalid spot symbol format: {spot_symbol}")
    return f"{base}{quote}"


# --------------------------------------------------------------------------
# Binance: posição e trades
# --------------------------------------------------------------------------
def is_position_open(symbol: str) -> bool:
    """
    Verifica em /fapi/v2/positionRisk se há posição aberta nesse symbol.
    Se algum entry tiver positionAmt != 0 → posição aberta.
    """
    params: Dict[str, Any] = {"symbol": symbol}
    resp = signed_request("GET", "/fapi/v2/positionRisk", params)
    data = resp.json()

    if not isinstance(data, list):
        print(f"positionRisk unexpected response: {data}")
        return False

    for pos in data:
        amt_str = pos.get("positionAmt", "0")
        try:
            amt = Decimal(str(amt_str))
        except Exception:
            amt = Decimal("0")
        if amt != 0:
            return True

    return False


def fetch_user_trades_since(symbol: str, start_time_ms: int) -> List[Dict[str, Any]]:
    """
    Vai buscar os userTrades de futures para um symbol a partir de start_time_ms.
    Usa /fapi/v1/userTrades com 'startTime'.
    """
    trades: List[Dict[str, Any]] = []

    params: Dict[str, Any] = {
        "symbol": symbol,
        "startTime": start_time_ms,
        "limit": 1000,
    }

    resp = signed_request("GET", "/fapi/v1/userTrades", params)
    data = resp.json()

    if isinstance(data, list):
        trades.extend(data)
    else:
        print(f"userTrades unexpected response: {data}")

    trades.sort(key=lambda t: t.get("time", 0))
    return trades


# --------------------------------------------------------------------------
# Cálculo de fecho (exit_price, PnL, etc.)
# --------------------------------------------------------------------------
def compute_exit_from_trades(
    trades: List[Dict[str, Any]],
    direction: str,
    entry_price: Decimal,
    stop_loss: Optional[Decimal],
    stop_pct: Optional[Decimal],
) -> Optional[Dict[str, Any]]:
    """
    A partir da lista de trades e da direção (BUY/SELL),
    calcula:
      - exit_price (VWAP dos trades de fecho)
      - exit_time_ms
      - total_profit_usd (soma realizedPnl)
      - profit_pct
      - r_multiple
      - exit_type ('sl' ou 'manual', heurística)
    """
    if not trades:
        return None

    direction = direction.upper()
    if direction not in ("BUY", "SELL"):
        raise ValueError(f"Invalid direction: {direction}")

    closing_side = "SELL" if direction == "BUY" else "BUY"

    closes: List[Dict[str, Any]] = [
        t for t in trades if t.get("side") == closing_side
    ]

    if not closes:
        # Não há trades de fecho claros -> fallback: usar último trade
        closes = [trades[-1]]

    # VWAP dos trades de fecho
    vwap_num = Decimal("0")
    qty_sum = Decimal("0")
    exit_time_ms = 0

    total_realized_pnl = Decimal("0")

    for t in closes:
        price = Decimal(str(t.get("price", "0")))
        qty = Decimal(str(t.get("qty", "0")))
        vwap_num += price * qty
        qty_sum += qty

        time_ms = int(t.get("time", 0))
        exit_time_ms = max(exit_time_ms, time_ms)

        pnl_str = t.get("realizedPnl", "0")
        try:
            pnl = Decimal(str(pnl_str))
        except Exception:
            pnl = Decimal("0")
        total_realized_pnl += pnl

    if qty_sum > 0:
        exit_price = vwap_num / qty_sum
    else:
        # fallback extremo
        last_price = Decimal(str(closes[-1].get("price", "0")))
        exit_price = last_price

    # profit_pct
    if direction == "BUY":
        profit_pct = (exit_price - entry_price) / entry_price
    else:
        profit_pct = (entry_price - exit_price) / entry_price

    # stop_pct: usar o que vem do sinal; se não houver, calcula
    if stop_pct is None or stop_pct <= 0:
        if stop_loss is not None and stop_loss > 0:
            if direction == "BUY":
                sl_dist = (entry_price - stop_loss) / entry_price
            else:
                sl_dist = (stop_loss - entry_price) / entry_price
            stop_pct_calc = abs(sl_dist)
        else:
            stop_pct_calc = Decimal("0")
    else:
        stop_pct_calc = stop_pct

    if stop_pct_calc > 0:
        r_multiple = profit_pct / stop_pct_calc
    else:
        r_multiple = None

    # exit_type heurístico
    exit_type = "manual"
    exit_level = "manual"

    if stop_loss is not None and stop_loss > 0:
        # tolerância de 0.2% do entry para considerar SL
        tolerance = entry_price * Decimal("0.002")
        if abs(exit_price - stop_loss) <= tolerance:
            exit_type = "sl"
            exit_level = "sl"

    return {
        "exit_price": exit_price,
        "exit_time_ms": exit_time_ms,
        "total_profit_usd": total_realized_pnl,
        "profit_pct": profit_pct,
        "r_multiple": r_multiple,
        "exit_type": exit_type,
        "exit_level": exit_level,
    }


# --------------------------------------------------------------------------
# Supabase: fetch sinais abertos
# --------------------------------------------------------------------------
def fetch_open_futures_signals() -> List[Dict[str, Any]]:
    """
    Busca sinais com:
      status = 'open'
      futures_entry_sent = true
      futures_symbol IS NOT NULL
    """
    print("-> Fetching open futures signals from Supabase...")
    res = (
        SUPABASE.table(SIGNALS_TABLE)
        .select("*")
        .eq("status", "open")
        .eq("futures_entry_sent", True)
        .not_.is_("futures_symbol", "null")
        .order("created_at", desc=True)
        .limit(50)
        .execute()
    )

    data = res.data or []
    if not data:
        print("No open futures signals to check.")
    else:
        print(f"Found {len(data)} open futures signals.")
    return data


def update_signal_closed(
    signal_id: Any,
    exit_info: Dict[str, Any],
) -> None:
    """
    Atualiza o sinal para 'closed' com os dados de fecho.
    """
    exit_price: Decimal = exit_info["exit_price"]
    exit_time_ms: int = exit_info["exit_time_ms"]
    total_profit_usd: Decimal = exit_info["total_profit_usd"]
    profit_pct: Decimal = exit_info["profit_pct"]
    r_multiple = exit_info["r_multiple"]
    exit_type: str = exit_info["exit_type"]
    exit_level: str = exit_info["exit_level"]

    exit_at_iso = dt.datetime.utcfromtimestamp(exit_time_ms / 1000.0).isoformat() + "Z"

    update_payload: Dict[str, Any] = {
        "status": "closed",
        "exit_at": exit_at_iso,
        "exit_level": exit_level,
        "avg_exit_price": float(exit_price),
        "total_profit_usd": float(total_profit_usd),
        "profit_pct": float(profit_pct),
        "finalized": True,
        "exit_type": exit_type,
    }

    if r_multiple is not None:
        update_payload["r_multiple"] = float(r_multiple)

    print(f"-> Updating signal {signal_id} as closed in Supabase...")
    SUPABASE.table(SIGNALS_TABLE).update(update_payload).eq("id", signal_id).execute()
    print(f"-> Signal {signal_id} updated.")


# --------------------------------------------------------------------------
# MAIN loop
# --------------------------------------------------------------------------
def main() -> None:
    print("Starting sync_futures_closures")
    print(f"Environment: {ENV_LABEL}")
    print(f"Using Supabase table: {SIGNALS_TABLE}")

    while True:
        try:
            signals = fetch_open_futures_signals()
            if not signals:
                time.sleep(LOOP_SECONDS)
                continue

            for signal in signals:
                try:
                    sig_id = signal.get("id")
                    spot_symbol = str(signal.get("symbol"))
                    futures_symbol = signal.get("futures_symbol") or normalize_futures_symbol(
                        spot_symbol
                    )
                    direction = str(signal.get("direction", "")).upper()
                    entry = Decimal(str(signal.get("entry")))
                    stop_loss_raw = signal.get("stop_loss") or signal.get("stop_loss_initial")
                    stop_loss = (
                        Decimal(str(stop_loss_raw)) if stop_loss_raw is not None else None
                    )
                    stop_pct_raw = signal.get("stop_pct")
                    stop_pct = (
                        Decimal(str(stop_pct_raw)) if stop_pct_raw is not None else None
                    )

                    created_at = str(signal.get("created_at"))
                    created_ms = parse_iso_to_ms(created_at)

                    print(
                        f"\n--- Checking signal {sig_id} ({futures_symbol}, {direction}) ---"
                    )
                    print(f"  created_at: {created_at}")
                    print(f"  entry     : {entry}")
                    print(f"  stop_loss : {stop_loss}")

                    # 1) Verificar se ainda há posição aberta
                    print(
                        f"-> Checking position on {futures_symbol} in {ENV_LABEL} (positionRisk)..."
                    )
                    if is_position_open(futures_symbol):
                        print("   Position still OPEN on Binance. Skipping for now.")
                        continue

                    print("   No open position detected -> trade appears CLOSED on Binance.")

                    # 2) Buscar trades desde created_at
                    print("-> Fetching userTrades since signal creation...")
                    trades = fetch_user_trades_since(futures_symbol, created_ms)
                    print(f"   Retrieved {len(trades)} trades.")

                    if not trades:
                        print(
                            "   No trades found for this symbol after created_at. "
                            "Cannot compute exit, skipping."
                        )
                        continue

                    # 3) Calcular métricas de fecho
                    exit_info = compute_exit_from_trades(
                        trades=trades,
                        direction=direction,
                        entry_price=entry,
                        stop_loss=stop_loss,
                        stop_pct=stop_pct,
                    )

                    if not exit_info:
                        print("   Could not compute exit_info, skipping.")
                        continue

                    print("   Exit info computed:")
                    print(f"      exit_price       : {exit_info['exit_price']}")
                    print(f"      profit_pct       : {exit_info['profit_pct']}")
                    print(f"      r_multiple       : {exit_info['r_multiple']}")
                    print(f"      total_profit_usd : {exit_info['total_profit_usd']}")
                    print(f"      exit_type/level  : {exit_info['exit_type']}/{exit_info['exit_level']}")

                    # 4) Atualizar Supabase
                    update_signal_closed(sig_id, exit_info)

                except Exception as e_sig:
                    print(f"Error processing signal {signal.get('id')}: {e_sig}")
                    # continua com o próximo sinal

        except Exception as e:
            print(f"[FATAL LOOP ERROR] {e}")

        time.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    main()

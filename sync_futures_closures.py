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
            -> Usa também os orderId guardados no sinal:
                 - binance_sl_order_id / futures_sl_order_id
                 - binance_tp1_order_id / futures_tp1_order_id
                 - binance_tp2_order_id / futures_tp2_order_id
                 - binance_tp3_order_id / futures_tp3_order_id
               para decidir exit_type/exit_level:
                 'tp3' > 'tp2' > 'tp1' > 'sl' > 'manual'
            -> Atualiza a linha em 'signals':
                 status='close', finalized=true,
                 exit_at, avg_exit_price, profit_pct, r_multiple, total_profit_usd,
                 exit_type, exit_level, hit_level.

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
# Helpers gerais Binance
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
# Binance: posição, trades e ordens
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


def fetch_order_status(symbol: str, order_id: Any) -> Optional[str]:
    """
    Vai buscar a ordem por orderId em /fapi/v1/order e devolve o status (FILLED, NEW, etc.).
    Se não existir ou der erro -2013, devolve None.
    """
    if order_id is None:
        return None

    try:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "orderId": int(order_id),
        }
    except Exception:
        params = {
            "symbol": symbol,
            "orderId": order_id,
        }

    try:
        resp = signed_request("GET", "/fapi/v1/order", params)
        data = resp.json()
        status = data.get("status")
        print(f"   Order {order_id} on {symbol} -> status={status}")
        return status
    except requests.exceptions.HTTPError as e:
        r = getattr(e, "response", None)
        txt = r.text if r is not None else ""
        if "-2013" in txt or "Order does not exist" in txt:
            print(f"   Order {order_id} does not exist or not found on {symbol}.")
            return None
        print(f"   Error fetching order {order_id} on {symbol}: {txt}")
        raise


def infer_exit_from_orders(symbol: str, signal: Dict[str, Any]) -> Tuple[str, str]:
    """
    Usa os order_id do sinal para inferir exit_type/exit_level.
    Prioridade: tp3 > tp2 > tp1 > sl > manual
    Retorna (exit_type, exit_level).

    Tenta primeiro as colunas binance_* (atuais) e cai para futures_* (legacy) se existirem.
    """
    sl_id = (
        signal.get("binance_sl_order_id")
        or signal.get("futures_sl_order_id")
    )
    tp1_id = (
        signal.get("binance_tp1_order_id")
        or signal.get("futures_tp1_order_id")
    )
    tp2_id = (
        signal.get("binance_tp2_order_id")
        or signal.get("futures_tp2_order_id")
    )
    tp3_id = (
        signal.get("binance_tp3_order_id")
        or signal.get("futures_tp3_order_id")
    )

    sl_status = fetch_order_status(symbol, sl_id) if sl_id else None
    tp1_status = fetch_order_status(symbol, tp1_id) if tp1_id else None
    tp2_status = fetch_order_status(symbol, tp2_id) if tp2_id else None
    tp3_status = fetch_order_status(symbol, tp3_id) if tp3_id else None

    sl_filled = sl_status == "FILLED"
    tp1_filled = tp1_status == "FILLED"
    tp2_filled = tp2_status == "FILLED"
    tp3_filled = tp3_status == "FILLED"

    if tp3_filled:
        return "tp", "tp3"
    if tp2_filled:
        return "tp", "tp2"
    if tp1_filled:
        return "tp", "tp1"
    if sl_filled:
        return "sl", "sl"

    return "manual", "manual"


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
      - exit_type/exit_level heurísticos (podem ser sobrescritos via orders).
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
        closes = [trades[-1]]

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
        last_price = Decimal(str(closes[-1].get("price", "0")))
        exit_price = last_price

    if direction == "BUY":
        profit_pct = (exit_price - entry_price) / entry_price
    else:
        profit_pct = (entry_price - exit_price) / entry_price

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

    exit_type = "manual"
    exit_level = "manual"

    if stop_loss is not None and stop_loss > 0:
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
    Atualiza o sinal para 'close' com os dados de fecho.

    Regras para bater certo com os CHECK da tabela:
    - status só pode ser 'open','tp1','tp2','tp3','close'
    - exit_level só pode ser NULL, 'sl', 'tp1', 'tp2', 'tp3', 'be'
    - Para status='close', exit_level NÃO pode ser NULL.
    - 'manual' NUNCA entra em exit_level, só em exit_type (antes de normalizar).
    - Para fechos manuais:
        * se heurística deu 'sl' -> mantemos 'sl'
        * senão:
            - exit_level = 'tp1' se profit_pct >= 0
            - exit_level = 'sl'  se profit_pct < 0
    - Para satisfazer o signals_exit_level_check:
        * se exit_level_norm = 'sl'  -> exit_type_db = 'sl'
        * se exit_level_norm = 'tpX' -> exit_type_db = 'tp'
    """
    exit_price: Decimal = exit_info["exit_price"]
    exit_time_ms: int = exit_info["exit_time_ms"]
    total_profit_usd: Decimal = exit_info["total_profit_usd"]
    profit_pct: Decimal = exit_info["profit_pct"]
    r_multiple = exit_info["r_multiple"]
    exit_type_raw: str = exit_info["exit_type"]
    exit_level_raw: str = exit_info["exit_level"]

    # Tipo/nível “originais” (incluindo manual)
    exit_type_orig = (exit_type_raw or "").lower()
    exit_level_orig = (exit_level_raw or "").lower() if exit_level_raw is not None else None

    valid_levels = {"sl", "tp1", "tp2", "tp3", "be"}

    # 1) Normalizar exit_level
    if exit_type_orig == "manual":
        # se por acaso já veio 'sl' da heurística, respeitamos
        if exit_level_orig in valid_levels:
            exit_level_norm = exit_level_orig
        else:
            # manual e sem nível consistente -> decidimos pelo resultado
            exit_level_norm = "tp1" if profit_pct >= 0 else "sl"
    else:
        if exit_level_orig in valid_levels:
            exit_level_norm = exit_level_orig
        else:
            # fallback super conservador: sl
            exit_level_norm = "sl"

    # 2) Normalizar exit_type para bater certo com exit_level_norm
    if exit_level_norm == "sl":
        exit_type_db = "sl"
    else:
        # tp1 / tp2 / tp3 / be
        exit_type_db = "tp"

    hit_level = exit_level_norm

    # datetime consciente de timezone (evita DeprecationWarning)
    exit_at_iso = dt.datetime.fromtimestamp(
        exit_time_ms / 1000.0, tz=dt.timezone.utc
    ).isoformat()

    update_payload: Dict[str, Any] = {
        "status": "close",          # <- AQUI: 'close', não 'closed'
        "exit_at": exit_at_iso,
        "exit_level": exit_level_norm,
        "hit_level": hit_level,
        "avg_exit_price": float(exit_price),
        "total_profit_usd": float(total_profit_usd),
        "profit_pct": float(profit_pct),
        "finalized": True,
        "exit_type": exit_type_db,
    }

    if r_multiple is not None:
        update_payload["r_multiple"] = float(r_multiple)

    print(
        f"-> Updating signal {signal_id} as close in Supabase "
        f"(orig exit_type={exit_type_orig}, final exit_type={exit_type_db}, "
        f"exit_level={exit_level_norm})..."
    )
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

                    print(
                        f"-> Checking position on {futures_symbol} in {ENV_LABEL} (positionRisk)..."
                    )
                    if is_position_open(futures_symbol):
                        print("   Position still OPEN on Binance. Skipping for now.")
                        continue

                    print("   No open position detected -> trade appears CLOSED on Binance.")

                    print("-> Fetching userTrades since signal creation...")
                    trades = fetch_user_trades_since(futures_symbol, created_ms)
                    print(f"   Retrieved {len(trades)} trades.")

                    if not trades:
                        print(
                            "   No trades found for this symbol after created_at. "
                            "Cannot compute exit, skipping."
                        )
                        continue

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

                    print("-> Inferring exit_type/exit_level from order IDs...")
                    ord_exit_type, ord_exit_level = infer_exit_from_orders(
                        futures_symbol, signal
                    )

                    if ord_exit_type != "manual" or ord_exit_level != "manual":
                        exit_info["exit_type"] = ord_exit_type
                        exit_info["exit_level"] = ord_exit_level

                    print("   Exit info computed:")
                    print(f"      exit_price       : {exit_info['exit_price']}")
                    print(f"      profit_pct       : {exit_info['profit_pct']}")
                    print(f"      r_multiple       : {exit_info['r_multiple']}")
                    print(f"      total_profit_usd : {exit_info['total_profit_usd']}")
                    print(
                        f"      exit_type/level  : {exit_info['exit_type']}/"
                        f"{exit_info['exit_level']}"
                    )

                    update_signal_closed(sig_id, exit_info)

                except Exception as e_sig:
                    print(f"Error processing signal {signal.get('id')}: {e_sig}")

        except Exception as e:
            print(f"[FATAL LOOP ERROR] {e}")

        time.sleep(LOOP_SECONDS)


if __name__ == "__main__":
    main()

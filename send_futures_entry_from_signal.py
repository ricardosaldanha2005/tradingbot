#!/usr/bin/env python3
"""
send_futures_entry_from_signal.py

Lê sinais 'open' da tabela 'signals' (Supabase) e envia
ordens MARKET para a Binance Futures (TESTNET ou real),
calculando a quantidade com base no risco por trade e no stop loss.

Depois de a ordem de entrada ser preenchida, cria automaticamente:
- 1 STOP_MARKET de Stop Loss
- até 3 TAKE_PROFIT_MARKET (tp1, tp2, tp3) em modo reduceOnly

Corre em loop:
- procura sinais com status='open' e futures_entry_sent=false
- processa 1 sinal de cada vez
- depois marca futures_entry_sent=true (se a entrada + SL/TP forem bem sucedidos)

Campos usados na tabela 'signals':
- futures_entry_sent      (boolean)
- futures_symbol          (text)
- futures_entry_order_id  (text ou bigint)
- futures_entry_status    (text)

ENV necessários:

  SUPABASE_URL=...
  SUPABASE_SERVICE_ROLE=...

  SIGNALS_TABLE=signals             # opcional, default 'signals'

  BINANCE_FUTURES_API_KEY=...
  BINANCE_FUTURES_SECRET_KEY=...
  BINANCE_FUTURES_TESTNET=1         # 1 = demo-fapi.binance.com ; 0 = fapi.binance.com

  RISK_PER_TRADE_PCT=0.5            # percentagem da banca arriscada por trade (ex: 0.5)

  # Opcional:
  FUTURES_BALANCE_ASSET=USDT        # ativo base da conta futures, default 'USDT'
  DRY_RUN=0                         # 1 = não envia ordem, só mostra (mas marca como tratado)
"""

import os
import time
import hmac
import hashlib
from decimal import Decimal
from typing import Any, Dict, Tuple, Optional, List

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

# -----------------------------------------------------------------------------
# ENV & setup
# -----------------------------------------------------------------------------
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

RISK_PER_TRADE_PCT = Decimal(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
FUTURES_BALANCE_ASSET = os.getenv("FUTURES_BALANCE_ASSET", "USDT")
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Timeout de requests para a Binance
REQUEST_TIMEOUT = 10


# -----------------------------------------------------------------------------
# Helpers Binance (assinatura, requests, filtros, balance)
# -----------------------------------------------------------------------------
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


def get_symbol_filters(symbol: str) -> Tuple[Decimal, Decimal, int]:
    """
    Vai buscar:
      - LOT_SIZE.minQty
      - LOT_SIZE.stepSize
      - quantityPrecision  -> nº máximo de casas decimais permitido na QUANTITY

    Devolve:
      (min_qty, step_size, qty_decimals)
    """
    path = "/fapi/v1/exchangeInfo"
    params = {"symbol": symbol}

    resp = requests.get(
        f"{BINANCE_BASE_URL}{path}",
        params=params,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()

    symbols = data.get("symbols", [])
    if not symbols:
        raise RuntimeError(f"Symbol {symbol} not found in Binance exchangeInfo")

    info = symbols[0]

    lot_filter = None
    for f in info.get("filters", []):
        if f.get("filterType") == "LOT_SIZE":
            lot_filter = f
            break

    if lot_filter is None:
        raise RuntimeError(f"LOT_SIZE filter not found for symbol {symbol}")

    min_qty_str = lot_filter["minQty"]
    step_size_str = lot_filter["stepSize"]

    min_qty = Decimal(min_qty_str)
    step_size = Decimal(step_size_str)

    # Decimais deduzidos do step_size (fallback)
    step_str = step_size_str.rstrip("0")
    if "." in step_str:
        decimals_from_step = len(step_str.split(".")[1])
    else:
        decimals_from_step = 0

    # Se existir quantityPrecision, usamos isso como limite "oficial"
    qp = info.get("quantityPrecision")
    if qp is not None:
        qty_decimals = int(qp)
    else:
        qty_decimals = decimals_from_step

    print(f"  min_qty           : {min_qty_str}")
    print(f"  step_size         : {step_size_str}")
    print(f"  quantityPrecision : {info.get('quantityPrecision')}")
    print(f"  qty_decimals used : {qty_decimals}")

    return min_qty, step_size, qty_decimals


def get_futures_balance(asset: str = "USDT") -> Decimal:
    """
    Vai buscar o balance (wallet balance) no futures para o asset dado.
    Usa /fapi/v2/balance.
    """
    path = "/fapi/v2/balance"
    params: Dict[str, Any] = {}

    resp = signed_request("GET", path, params)
    data = resp.json()

    for entry in data:
        if entry.get("asset") == asset:
            bal_str = entry.get("balance") or entry.get("walletBalance") or "0"
            bal = Decimal(bal_str)
            print(f"  {asset} balance (futures): {bal}")
            return bal

    print(f"  {asset} balance (futures): 0 (not found)")
    return Decimal("0")


# -----------------------------------------------------------------------------
# Helpers de quantidade / símbolo
# -----------------------------------------------------------------------------
def normalize_futures_symbol(spot_symbol: str) -> str:
    """
    Converte símbolo tipo 'XPL/USDT' ou 'BTC/USDT' em 'XPLUSDT', 'BTCUSDT', etc.
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


def quantize_to_step(qty: Decimal, step_size: Decimal) -> Decimal:
    """
    Faz floor da quantidade para o múltiplo de step_size mais próximo.
    """
    if step_size <= 0:
        raise ValueError("step_size must be > 0")
    return (qty // step_size) * step_size


def format_quantity(qty: Decimal, qty_decimals: int) -> str:
    """
    Formata a quantity com exatamente qty_decimals casas decimais.
    """
    if qty_decimals <= 0:
        return str(int(qty))
    fmt = f"{{0:.{qty_decimals}f}}"
    return fmt.format(qty)


def calculate_position_size(
    balance: Decimal,
    entry: Decimal,
    stop_loss: Decimal,
    direction: str,
    risk_pct: Decimal,
    step_size: Decimal,
    qty_decimals: int,
) -> Tuple[Decimal, Decimal, str]:
    """
    Calcula a quantidade a partir do risco % e da distância até ao stop.
    Devolve (raw_qty, final_qty, final_qty_str)
    """
    if balance <= 0:
        raise ValueError("Futures balance is zero or negative")

    if entry <= 0 or stop_loss <= 0:
        raise ValueError("Entry/Stop must be positive")

    direction = direction.upper()
    if direction not in ("BUY", "SELL"):
        raise ValueError(f"Invalid direction: {direction}")

    if direction == "BUY":
        risk_per_unit = entry - stop_loss
    else:
        risk_per_unit = stop_loss - entry

    if risk_per_unit <= 0:
        raise ValueError(
            f"Invalid SL distance for {direction}: entry={entry}, stop={stop_loss}"
        )

    risk_capital = balance * (risk_pct / Decimal("100"))
    raw_qty = risk_capital / risk_per_unit

    adj_qty = quantize_to_step(raw_qty, step_size)

    if adj_qty <= 0:
        raise ValueError(
            f"Calculated qty <= 0 (raw={raw_qty}, step_size={step_size})"
        )

    qty_str = format_quantity(adj_qty, qty_decimals)

    print(f"-> Calculating quantity with RISK_PER_TRADE_PCT={risk_pct}%...")
    print(f"  raw_qty : {raw_qty}")
    print(f"  qty     : {qty_str}")

    return raw_qty, adj_qty, qty_str


# -----------------------------------------------------------------------------
# Helpers de ordens: esperar fill + bracket orders (SL + TPs)
# -----------------------------------------------------------------------------
def wait_for_fill(
    symbol: str,
    order_id: int,
    timeout_s: float = 60.0,
    poll_interval_s: float = 1.0,
) -> Optional[Dict[str, Any]]:
    """
    Faz polling à Binance Futures até a ordem ficar FILLED ou o timeout expirar.
    Devolve o JSON da ordem FILLED ou None se não encher (CANCELED / EXPIRED / timeout).
    """
    print(f"-> Waiting for fill of order_id={order_id} on {symbol} ...")
    deadline = time.time() + timeout_s

    last_status = None

    while time.time() < deadline:
        params: Dict[str, Any] = {
            "symbol": symbol,
            "orderId": order_id,
        }
        resp = signed_request("GET", "/fapi/v1/order", params)
        data = resp.json()
        status = data.get("status")
        if status != last_status:
            print(f"   current status: {status}")
            last_status = status

        if status == "FILLED":
            print("-> Entry order FILLED.")
            return data

        if status in ("CANCELED", "REJECTED", "EXPIRED", "PENDING_CANCEL"):
            print(f"-> Entry order ended with status={status}, aborting bracket.")
            return None

        time.sleep(poll_interval_s)

    print("-> Timeout waiting for fill; bracket orders NOT sent.")
    return None


def split_quantity_for_tps(
    total_qty: Decimal,
    min_qty: Decimal,
    step_size: Decimal,
) -> List[Decimal]:
    """
    Divide a quantidade total em até 3 partes (tp1, tp2, tp3).
    Garante que cada parte é >= min_qty; se não der, reduz nº de TPs.
    """

    def mk_part(fraction: Decimal) -> Decimal:
        return quantize_to_step(total_qty * fraction, step_size)

    # Tentativa 3 partes
    q1 = mk_part(Decimal("0.33"))
    q2 = mk_part(Decimal("0.33"))
    q3 = quantize_to_step(total_qty - q1 - q2, step_size)
    parts3 = [q for q in (q1, q2, q3) if q >= min_qty]

    if len(parts3) == 3 and sum(parts3) > 0:
        return parts3

    # Tentativa 2 partes
    q1 = mk_part(Decimal("0.5"))
    q2 = quantize_to_step(total_qty - q1, step_size)
    parts2 = [q for q in (q1, q2) if q >= min_qty]

    if len(parts2) == 2 and sum(parts2) > 0:
        return parts2

    # Fallback: 1 parte (tudo)
    total_adj = quantize_to_step(total_qty, step_size)
    if total_adj < min_qty:
        raise ValueError(
            f"Total quantity {total_qty} below min_qty {min_qty}, cannot place TP."
        )
    return [total_adj]


def place_bracket_orders(
    symbol: str,
    side_entry: str,
    qty: Decimal,
    stop_loss: Decimal,
    tp1: Decimal,
    tp2: Decimal,
    tp3: Decimal,
    step_size: Decimal,
    min_qty: Decimal,
    qty_decimals: int,
) -> None:
    """
    Cria:
      - 1 STOP_MARKET de SL (closePosition=True, reduceOnly=True)
      - até 3 TAKE_PROFIT_MARKET (tp1, tp2, tp3) em modo reduceOnly
    """
    side_entry = side_entry.upper()
    if side_entry not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side_entry: {side_entry}")

    side_close = "SELL" if side_entry == "BUY" else "BUY"

    print("-> Placing bracket orders (SL + TPs)...")

    # STOP LOSS
    sl_params: Dict[str, Any] = {
        "symbol": symbol,
        "side": side_close,
        "type": "STOP_MARKET",
        "stopPrice": str(stop_loss),
        "closePosition": True,
        "workingType": "CONTRACT_PRICE",
        "reduceOnly": True,
    }

    print("   Sending STOP_MARKET (SL) ...")
    sl_resp = signed_request("POST", "/fapi/v1/order", sl_params)
    print("   SL response:")
    print(sl_resp.json())

    # TP ORDERS
    tp_prices = [tp1, tp2, tp3]
    qty_parts = split_quantity_for_tps(qty, min_qty, step_size)
    usable_tps = tp_prices[: len(qty_parts)]

    for i, (part_qty, tp_price) in enumerate(zip(qty_parts, usable_tps), start=1):
        qty_str = format_quantity(part_qty, qty_decimals)
        tp_params: Dict[str, Any] = {
            "symbol": symbol,
            "side": side_close,
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(tp_price),
            "quantity": qty_str,
            "reduceOnly": True,
            "workingType": "CONTRACT_PRICE",
        }
        print(f"   Sending TAKE_PROFIT_MARKET TP{i} (qty={qty_str}, price={tp_price}) ...")
        tp_resp = signed_request("POST", "/fapi/v1/order", tp_params)
        print(f"   TP{i} response:")
        print(tp_resp.json())


# -----------------------------------------------------------------------------
# Supabase
# -----------------------------------------------------------------------------
def fetch_latest_open_signal() -> Optional[Dict[str, Any]]:
    """
    Busca o último sinal com status 'open' e futures_entry_sent = false.
    """
    print("-> Fetching latest signal from 'signals'...")

    res = (
        SUPABASE.table(SIGNALS_TABLE)
        .select("*")
        .eq("status", "open")
        .eq("futures_entry_sent", False)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    data = res.data or []
    if not data:
        print("No pending open signals found.")
        return None

    signal = data[0]

    print("Found signal:")
    print(f"  id        : {signal.get('id')}")
    print(f"  symbol    : {signal.get('symbol')}")
    print(f"  direction : {signal.get('direction')}")
    print(f"  status    : {signal.get('status')}")
    print(f"  entry     : {signal.get('entry')}")
    print(f"  tp1       : {signal.get('tp1')}")
    print(f"  stop_loss : {signal.get('stop_loss')}")

    return signal


def update_signal(id_: Any, fields: Dict[str, Any]) -> None:
    """
    Helper simples para fazer update na tabela de sinais.
    """
    SUPABASE.table(SIGNALS_TABLE).update(fields).eq("id", id_).execute()


# -----------------------------------------------------------------------------
# MAIN (loop infinito a processar sinais)
# -----------------------------------------------------------------------------
def main() -> None:
    print("Starting Container")
    print("-> Connecting to Supabase...")

    while True:
        try:
            signal = fetch_latest_open_signal()
            if not signal:
                time.sleep(5)
                continue

            signal_id = signal["id"]

            symbol_spot = str(signal["symbol"])
            direction = str(signal["direction"]).upper()
            entry = Decimal(str(signal["entry"]))
            stop_loss = Decimal(str(signal["stop_loss"]))

            tp1 = Decimal(str(signal["tp1"]))
            tp2 = Decimal(str(signal["tp2"]))
            tp3 = Decimal(str(signal["tp3"]))

            futures_symbol = normalize_futures_symbol(symbol_spot)
            print(f"-> Normalized futures symbol: {futures_symbol}")

            print(f"-> Fetching symbol filters from {ENV_LABEL}...")
            min_qty, step_size, qty_decimals = get_symbol_filters(futures_symbol)

            print(f"-> Fetching {FUTURES_BALANCE_ASSET} futures balance from {ENV_LABEL}...")
            balance = get_futures_balance(FUTURES_BALANCE_ASSET)

            raw_qty, adj_qty, qty_str = calculate_position_size(
                balance=balance,
                entry=entry,
                stop_loss=stop_loss,
                direction=direction,
                risk_pct=RISK_PER_TRADE_PCT,
                step_size=step_size,
                qty_decimals=qty_decimals,
            )

            print(
                "=== FUTURES ORDER TO SEND (TESTNET) ==="
                if BINANCE_TESTNET
                else "=== FUTURES ORDER TO SEND (LIVE) ==="
            )
            print(f"Symbol    : {futures_symbol}")
            print(f"Side      : {direction}")
            print(f"Entry     : {entry}")
            print(f"Stop Loss : {stop_loss}")
            print(f"Quantity  : {qty_str}")
            print("=======================================")

            if DRY_RUN:
                print("DRY_RUN=1 -> Ordem NÃO enviada, apenas simulação.")
                # Em DRY_RUN marcamos o sinal como tratado
                update_signal(
                    signal_id,
                    {
                        "futures_symbol": futures_symbol,
                        "futures_entry_status": "DRY_RUN",
                        "futures_entry_sent": True,
                    },
                )
                print(f"-> Marked signal {signal_id} as futures_entry_sent=true (DRY_RUN).")
                time.sleep(1)
                continue

            print(f"-> Sending MARKET order to {ENV_LABEL}...")

            params: Dict[str, Any] = {
                "symbol": futures_symbol,
                "side": direction,
                "type": "MARKET",
                "quantity": qty_str,
            }

            resp = signed_request("POST", "/fapi/v1/order", params)
            order = resp.json()

            print("Order response:")
            print(order)

            order_id = order.get("orderId")
            status = order.get("status")

            # Atualizar imediatamente informação da entrada no Supabase
            update_signal(
                signal_id,
                {
                    "futures_symbol": futures_symbol,
                    "futures_entry_order_id": str(order_id) if order_id is not None else None,
                    "futures_entry_status": status,
                },
            )

            if not isinstance(order_id, int):
                try:
                    order_id = int(order_id)
                except Exception:
                    print("-> Não consegui obter orderId válido; não envio bracket orders.")
                    # Marcamos como erro após entrada
                    update_signal(
                        signal_id,
                        {"futures_entry_status": "ERROR_INVALID_ORDER_ID"},
                    )
                    time.sleep(5)
                    continue

            filled_data = wait_for_fill(
                symbol=futures_symbol,
                order_id=order_id,
                timeout_s=60.0,
                poll_interval_s=1.0,
            )

            if not filled_data:
                # Ordem não chegou a FILLED; não criamos SL/TP, marcamos estado e NÃO repetimos
                update_signal(
                    signal_id,
                    {"futures_entry_status": "NOT_FILLED_TIMEOUT_OR_CANCELED"},
                )
                time.sleep(5)
                continue

            # Atualizar estado da ordem de entrada como FILLED
            filled_status = filled_data.get("status")
            update_signal(
                signal_id,
                {"futures_entry_status": filled_status},
            )

            # Enviar bracket orders (SL + TPs)
            place_bracket_orders(
                symbol=futures_symbol,
                side_entry=direction,
                qty=adj_qty,
                stop_loss=stop_loss,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                step_size=step_size,
                min_qty=min_qty,
                qty_decimals=qty_decimals,
            )

            # Se tudo correu bem, marcamos o sinal como tratado
            update_signal(
                signal_id,
                {"futures_entry_sent": True},
            )
            print(f"-> Marked signal {signal_id} as futures_entry_sent=true.")

        except Exception as e:
            print(f"Error processing signal: {e}")
            # Aqui não mexo em futures_entry_sent para poderes ver o erro e decidir
            time.sleep(5)

        # pequena pausa entre iterações
        time.sleep(1)


if __name__ == "__main__":
    main()

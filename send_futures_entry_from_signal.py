#!/usr/bin/env python3
# send_futures_entry_from_signal.py
#
# Lê o último sinal da tabela signals e abre uma posição em
# USDT-M Futures (Binance Futures TESTNET) com base em entry / stop_loss
# e um risco em % da banca (RISK_PER_TRADE_PCT).
#
# Por agora: só envia a ORDEM DE ENTRADA (MARKET) e grava o orderId.
# TP / SL / trailing fazemos no próximo passo.

import os
import time
import hmac
import hashlib
from urllib.parse import urlencode
from typing import Dict, Any, Tuple

import requests
from supabase import create_client, Client

# ---------- ENV / CONFIG ----------

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
supabase: Client = create_client(SB_URL, SB_KEY)

SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")

BINANCE_FUTURES_MODE = os.getenv("BINANCE_FUTURES_MODE", "TESTNET").upper()
if BINANCE_FUTURES_MODE not in ("TESTNET", "LIVE"):
    raise SystemExit(f"BINANCE_FUTURES_MODE must be TESTNET or LIVE, got: {BINANCE_FUTURES_MODE}")

if BINANCE_FUTURES_MODE == "TESTNET":
    BASE_URL = "https://demo-fapi.binance.com"
else:
    BASE_URL = "https://fapi.binance.com"

API_KEY = os.environ["BINANCE_FUTURES_API_KEY"]
API_SECRET = os.environ["BINANCE_FUTURES_API_SECRET"].encode("utf-8")

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))  # % da banca em USDT


# ---------- HELPERS BINANCE FUTURES ----------

def signed_request(method: str, path: str, params: Dict[str, Any]) -> Any:
    ts = int(time.time() * 1000)
    params = dict(params)  # copia
    params["timestamp"] = ts
    query = urlencode(params, doseq=True)

    signature = hmac.new(API_SECRET, query.encode("utf-8"), hashlib.sha256).hexdigest()
    query_with_sig = f"{query}&signature={signature}"

    headers = {"X-MBX-APIKEY": API_KEY}

    url = f"{BASE_URL}{path}"
    if method == "GET":
        r = requests.get(url, params=query_with_sig, headers=headers)
    elif method == "POST":
        r = requests.post(url, params=query_with_sig, headers=headers)
    else:
        raise ValueError(f"Unsupported method: {method}")

    try:
        r.raise_for_status()
    except Exception:
        print("Binance error:", r.status_code, r.text)
        raise

    return r.json()


def get_symbol_filters(symbol: str) -> Tuple[float, float]:
    """
    Vai ao /fapi/v1/exchangeInfo para buscar LOT_SIZE do symbol.
    Devolve (min_qty, step_size).
    """
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    r = requests.get(url, params={"symbol": symbol})
    r.raise_for_status()
    data = r.json()

    if "symbols" not in data or not data["symbols"]:
        raise ValueError(f"No exchangeInfo for symbol {symbol}")

    f_map = {f["filterType"]: f for f in data["symbols"][0]["filters"]}
    lot = f_map.get("LOT_SIZE")
    if not lot:
        raise ValueError(f"LOT_SIZE filter not found for {symbol}")

    min_qty = float(lot["minQty"])
    step_size = float(lot["stepSize"])
    return min_qty, step_size


def get_futures_usdt_balance() -> float:
    """
    Lê saldo de USDT em USDT-M Futures: GET /fapi/v2/balance
    """
    data = signed_request("GET", "/fapi/v2/balance", {})
    for item in data:
        if item.get("asset") == "USDT":
            return float(item.get("balance", 0.0))
    return 0.0


def round_step(qty: float, step: float) -> float:
    """
    Arredonda qty para baixo no múltiplo de step.
    """
    if step <= 0:
        return qty
    precision = max(0, len(str(step).split(".")[-1].rstrip("0")))
    # evitar erros de float:
    return float(f"{{:.{precision}f}}".format((qty // step) * step))


# ---------- LADO SUPABASE ----------

def fetch_last_signal() -> Dict[str, Any] | None:
    """
    Busca o último sinal (por created_at desc).
    Podes filtrar por status se quiseres (ex: status='open').
    """
    print("-> Fetching latest signal from 'signals'...")
    res = (
        supabase.table(SIGNALS_TABLE)
        .select("*")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = res.data or []
    if not rows:
        print("No signals found.")
        return None
    s = rows[0]
    print("Found signal:")
    print(f"  id        : {s.get('id')}")
    print(f"  symbol    : {s.get('symbol')}")
    print(f"  direction : {s.get('direction')}")
    print(f"  status    : {s.get('status')}")
    print(f"  entry     : {s.get('entry')}")
    print(f"  tp1       : {s.get('tp1')}")
    print(f"  stop_loss : {s.get('stop_loss')}")
    return s


def update_signal_after_order(signal_id: str, futures_symbol: str, order_id: int, status: str) -> None:
    print("-> Updating signal with futures order info...")
    supabase.table(SIGNALS_TABLE).update(
        {
            "futures_symbol": futures_symbol,
            "futures_entry_order_id": order_id,
            "futures_entry_status": status,
        }
    ).eq("id", signal_id).execute()


# ---------- LÓGICA DE RISCO E ORDEM ----------

def calc_quantity_from_risk(entry: float, stop_loss: float, balance_usdt: float) -> float:
    """
    Risco em USDT = balance * RISK_PER_TRADE_PCT/100.
    PnL por unidade ≈ |entry - stop_loss|.
    qty = risco_usdt / |entry - stop_loss|.
    """
    price_diff = abs(entry - stop_loss)
    if price_diff <= 0:
        raise ValueError("entry e stop_loss iguais ou inválidos")

    risk_usdt = balance_usdt * (RISK_PER_TRADE_PCT / 100.0)
    if risk_usdt <= 0:
        raise ValueError("RISK_PER_TRADE_PCT ou balance_usdt inválidos")

    qty = risk_usdt / price_diff
    return qty


def main():
    print("Starting Container")
    print("-> Connecting to Supabase...")

    signal = fetch_last_signal()
    if not signal:
        return

    symbol_raw = signal["symbol"]  # ex: "ETH/USDT"
    direction = signal["direction"]  # "BUY" ou "SELL"
    entry = float(signal["entry"])
    stop_loss = float(signal["stop_loss"])

    # Normalizar símbolo para Futures: ETH/USDT -> ETHUSDT
    futures_symbol = (signal.get("futures_symbol")
                      or symbol_raw.replace("/", ""))

    print(f"-> Normalized futures symbol: {futures_symbol}")

    # 1) filtros do símbolo
    print("-> Fetching symbol filters from Binance FUTURES TESTNET...")
    min_qty, step_size = get_symbol_filters(futures_symbol)
    print(f"  min_qty   : {min_qty}")
    print(f"  step_size : {step_size}")

    # 2) saldo USDT
    print("-> Fetching USDT futures balance from Binance FUTURES TESTNET...")
    balance = get_futures_usdt_balance()
    print(f"  USDT balance (futures): {balance}")

    # 3) qty pela % de risco
    print(f"-> Calculating quantity with RISK_PER_TRADE_PCT={RISK_PER_TRADE_PCT}%...")
    raw_qty = calc_quantity_from_risk(entry, stop_loss, balance)
    qty = max(min_qty, round_step(raw_qty, step_size))
    print(f"  raw_qty : {raw_qty}")
    print(f"  qty     : {qty}")

    side = "BUY" if direction == "BUY" else "SELL"
    print("")
    print("=== FUTURES ORDER TO SEND (TESTNET) ===")
    print(f"Symbol    : {futures_symbol}")
    print(f"Side      : {side}")
    print(f"Entry     : {entry}")
    print(f"Stop Loss : {stop_loss}")
    print(f"Quantity  : {qty}")
    print("=======================================")

    # 4) Enviar ordem MARKET
    print("-> Sending MARKET order to Binance FUTURES TESTNET...")
    params = {
        "symbol": futures_symbol,
        "side": side,
        "type": "MARKET",
        "quantity": f"{qty:.8f}",
    }
    resp = signed_request("POST", "/fapi/v1/order", params)
    print("=== BINANCE RESPONSE ===")
    print(resp)
    print("=======================================")

    order_id = resp.get("orderId")
    status = resp.get("status", "UNKNOWN")

    if order_id is not None:
        update_signal_after_order(signal["id"], futures_symbol, order_id, status)
    else:
        print("WARNING: no orderId in Binance response, not updating signal.")


if __name__ == "__main__":
    main()

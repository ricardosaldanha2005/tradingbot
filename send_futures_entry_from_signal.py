#!/usr/bin/env python3
"""
send_futures_entry_from_signal.py

- Vai ao Supabase buscar o último sinal "open" na tabela SIGNALS_TABLE.
- Calcula a quantidade a usar com base em RISK_PER_TRADE_PCT e distância até ao stop.
- Envia uma ordem de mercado em FUTURES TESTNET.
- Depois de filled, cria 3 TPs e 1 SL como bracket orders (reduceOnly).

ENV obrigatórias:
  SUPABASE_URL
  SUPABASE_SERVICE_ROLE
  SIGNALS_TABLE           (ex: signals)

  BINANCE_API_KEY
  BINANCE_API_SECRET
  BINANCE_FUTURES_TESTNET   (1 para usar testnet, senão usa mainnet)

  RISK_PER_TRADE_PCT      (ex: 0.5  => 0.5% do saldo USDT)
  FUTURES_BALANCE_ASSET   (default: USDT)
"""

import os
import time
import math
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
from supabase import create_client, Client

try:
    # binance-futures-connector
    from binance.um_futures import UMFutures
except ImportError:
    UMFutures = None  # type: ignore


load_dotenv()

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")

supabase: Client = create_client(SB_URL, SB_KEY)

BINANCE_API_KEY = os.environ["BINANCE_API_KEY"]
BINANCE_API_SECRET = os.environ["BINANCE_API_SECRET"]
FUTURES_TESTNET = os.getenv("BINANCE_FUTURES_TESTNET", "1") == "1"

FUTURES_BASE_URL_TESTNET = "https://testnet.binancefuture.com"

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))  # percent
FUTURES_BALANCE_ASSET = os.getenv("FUTURES_BALANCE_ASSET", "USDT")


def log(msg: str) -> None:
    print(msg, flush=True)


def get_futures_client() -> Any:
    if UMFutures is None:
        raise RuntimeError(
            "binance-futures-connector não está instalado. "
            "Instala com: pip install binance-futures-connector"
        )
    if FUTURES_TESTNET:
        return UMFutures(
            key=BINANCE_API_KEY,
            secret=BINANCE_API_SECRET,
            base_url=FUTURES_BASE_URL_TESTNET,
        )
    else:
        return UMFutures(
            key=BINANCE_API_KEY,
            secret=BINANCE_API_SECRET,
        )


def normalize_futures_symbol(spot_symbol: str) -> str:
    # "ETH/USDT" -> "ETHUSDT"
    return spot_symbol.replace("/", "").upper()


def get_symbol_filters(client: Any, symbol: str) -> Tuple[float, float, int, int]:
    """
    Devolve:
      min_qty, step_size, price_decimals, qty_decimals
    """
    info = client.exchange_info()
    sym_info = None
    for s in info["symbols"]:
        if s["symbol"] == symbol:
            sym_info = s
            break
    if sym_info is None:
        raise ValueError(f"Symbol {symbol} não encontrado em exchange_info()")

    min_qty = 0.0
    step_size = 0.0
    price_decimals = 2
    qty_decimals = 3

    for f in sym_info["filters"]:
        if f["filterType"] == "LOT_SIZE":
            min_qty = float(f["minQty"])
            step_size = float(f["stepSize"])
        if f["filterType"] == "PRICE_FILTER":
            tick_size = f["tickSize"]
            if "." in tick_size:
                price_decimals = len(tick_size.split(".")[1].rstrip("0"))
            else:
                price_decimals = 0

    if step_size <= 0:
        raise ValueError(f"step_size inválido para {symbol}")

    if "." in str(step_size):
        qty_decimals = len(str(step_size).split(".")[1].rstrip("0"))
    else:
        qty_decimals = 0

    log(f"  min_qty   : {min_qty}")
    log(f"  step_size : {step_size}")

    return min_qty, step_size, price_decimals, qty_decimals


def round_down(value: float, step: float) -> float:
    return math.floor(value / step) * step


def get_futures_balance(client: Any, asset: str) -> float:
    balances = client.balance()
    for b in balances:
        if b["asset"] == asset:
            return float(b["balance"])
    raise ValueError(f"Asset {asset} não encontrado em futures balance()")


def fetch_latest_open_signal() -> Optional[Dict[str, Any]]:
    log("-> Fetching latest signal from 'signals'...")
    resp = (
        supabase.table(SIGNALS_TABLE)
        .select("*")
        .eq("status", "open")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    data = resp.data or []
    if not data:
        return None
    return data[0]


def wait_fill_market_order(
    client: Any,
    symbol: str,
    order_id: int,
    poll_interval: float = 0.25,
) -> Dict[str, Any]:
    log(f"-> Waiting for fill of order_id={order_id} on {symbol} ...")
    while True:
        res = client.get_order(symbol=symbol, orderId=order_id)
        status = res.get("status")
        log(f"   current status: {status}")
        if status in ("FILLED", "CANCELED", "REJECTED", "EXPIRED"):
            return res
        time.sleep(poll_interval)


def place_bracket_orders(
    client: Any,
    *,
    symbol: str,
    side_entry: str,
    qty: float,
    entry_price: float,
    stop_loss_price: float,
    tp1_price: float,
    tp2_price: float,
    tp3_price: float,
    price_decimals: int,
    qty_decimals: int,
) -> None:
    """
    Cria 3 TPs (LIMIT reduceOnly) e 1 SL (STOP-MARKET) na direção inversa da entrada.
    side_entry: "BUY" (long) ou "SELL" (short)
    """
    side_exit = "SELL" if side_entry.upper() == "BUY" else "BUY"

    def fmt_price(p: float) -> str:
        return f"{p:.{price_decimals}f}"

    def fmt_qty(q: float) -> str:
        return f"{q:.{qty_decimals}f}"

    qt = fmt_qty(qty)

    log("-> Placing bracket orders (TP1/TP2/TP3 + SL)...")

    # TPs (3 ordens LIMIT reduceOnly)
    for level, price in zip(
        ("TP1", "TP2", "TP3"),
        (tp1_price, tp2_price, tp3_price),
    ):
        res_tp = client.new_order(
            symbol=symbol,
            side=side_exit,
            type="LIMIT",
            timeInForce="GTC",
            quantity=qt,
            price=fmt_price(price),
            reduceOnly="true",
        )
        log(f"  {level} order: {res_tp}")

    # SL como STOP-MARKET (reduceOnly)
    # - para LONG, stop_loss_price < entry => SELL STOP
    # - para SHORT, stop_loss_price > entry => BUY STOP
    res_sl = client.new_order(
        symbol=symbol,
        side=side_exit,
        type="STOP_MARKET",
        stopPrice=fmt_price(stop_loss_price),
        closePosition="true",
        workingType="CONTRACT_PRICE",
    )
    log(f"  SL order : {res_sl}")


def main() -> None:
    log("-> Connecting to Supabase...")
    # Supabase client já criado em módulo

    signal = fetch_latest_open_signal()
    if not signal:
        log("Nenhum sinal 'open' encontrado. A sair.")
        return

    sig_id = signal["id"]
    symbol = signal["symbol"]
    direction = signal["direction"].upper()
    status = signal["status"]

    entry = float(signal["entry"])
    stop_loss = float(signal["stop_loss"])
    tp1 = float(signal["tp1"])
    tp2 = float(signal["tp2"])
    tp3 = float(signal["tp3"])

    log("Found signal:")
    log(f"  id        : {sig_id}")
    log(f"  symbol    : {symbol}")
    log(f"  direction : {direction}")
    log(f"  status    : {status}")
    log(f"  entry     : {entry}")
    log(f"  tp1       : {tp1}")
    log(f"  stop_loss : {stop_loss}")

    futures_symbol = normalize_futures_symbol(symbol)
    log(f"-> Normalized futures symbol: {futures_symbol}")

    client = get_futures_client()

    log("-> Fetching symbol filters from Binance FUTURES TESTNET...")
    min_qty, step_size, price_decimals, qty_decimals = get_symbol_filters(client, futures_symbol)

    log(f"-> Fetching {FUTURES_BALANCE_ASSET} futures balance from Binance FUTURES TESTNET...")
    balance = get_futures_balance(client, FUTURES_BALANCE_ASSET)
    log(f"  {FUTURES_BALANCE_ASSET} balance (futures): {balance}")

    # Distância em USD por unidade entre entry e SL
    dist = abs(stop_loss - entry)
    if dist <= 0:
        raise ValueError("Distância entre entry e stop_loss inválida")

    # Risk em USD
    risk_usd = balance * (RISK_PER_TRADE_PCT / 100.0)

    log(f"-> Calculating quantity with RISK_PER_TRADE_PCT={RISK_PER_TRADE_PCT}%...")
    raw_qty = risk_usd / dist
    qty = round_down(raw_qty, step_size)
    if qty < min_qty:
        qty = min_qty

    log(f"  raw_qty : {raw_qty}")
    log(f"  qty     : {qty}")

    qt_str = f"{qty:.{qty_decimals}f}"

    side_entry = "BUY" if direction == "BUY" else "SELL"

    log("=== FUTURES ORDER TO SEND (TESTNET) ===")
    log(f"Symbol    : {futures_symbol}")
    log(f"Side      : {side_entry}")
    log(f"Entry     : {entry}")
    log(f"Stop Loss : {stop_loss}")
    log(f"Quantity  : {qty}")
    log("=======================================")

    log("-> Sending MARKET order to Binance FUTURES TESTNET...")
    order = client.new_order(
        symbol=futures_symbol,
        side=side_entry,
        type="MARKET",
        quantity=qt_str,
    )
    log("Order response:")
    log(str(order))

    filled = wait_fill_market_order(client, futures_symbol, int(order["orderId"]))
    if filled.get("status") != "FILLED":
        log(f"Entry order não ficou FILLED (status={filled.get('status')}). Não vou criar brackets.")
        return

    log("-> Entry order FILLED.")

    # Agora criamos as bracket orders (TP1/TP2/TP3 + SL)
    place_bracket_orders(
        client,
        symbol=futures_symbol,
        side_entry=side_entry,
        qty=qty,
        entry_price=entry,
        stop_loss_price=stop_loss,
        tp1_price=tp1,
        tp2_price=tp2,
        tp3_price=tp3,
        price_decimals=price_decimals,
        qty_decimals=qty_decimals,
    )


if __name__ == "__main__":
    main()

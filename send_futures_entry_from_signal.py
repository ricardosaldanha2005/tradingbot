#!/usr/bin/env python3
"""
send_futures_entry_from_signal.py

Lê o último sinal aberto na tabela 'signals' do Supabase e envia
uma ordem MARKET para a Binance FUTURES (TESTNET ou real),
calculando a quantidade com base no risco por trade e no stop loss.

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
  DRY_RUN=0                         # 1 = não envia ordem, só mostra
"""

import os
import time
import hmac
import hashlib
from decimal import Decimal, ROUND_DOWN
from typing import Any, Dict, Tuple, Optional

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
    Lança HTTPError se status != 2xx (mantém o comportamento que vês nos logs).
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
        # Mantém exatamente o estilo que aparece nos logs
        print(f"Binance error: {resp.status_code} {resp.text}")
        raise

    return resp


def get_symbol_filters(symbol: str) -> Tuple[Decimal, Decimal, int]:
    """
    Vai buscar os filtros do símbolo (LOT_SIZE) e devolve:
      (min_qty, step_size, qty_decimals)
    onde qty_decimals é o nº máximo de casas decimais permitido pela Binance
    para a QUANTITY, derivado do step_size.
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

    # Inferir nº de casas decimais a partir do step_size (ex: 0.001 -> 3)
    step_str = step_size_str.rstrip("0")  # "0.001" -> "0.001", "1.00000000" -> "1."
    if "." in step_str:
        decimals = len(step_str.split(".")[1])
    else:
        decimals = 0

    print(f"  min_qty   : {min_qty_str}")
    print(f"  step_size : {step_size_str}")

    return min_qty, step_size, decimals


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
        # fallback bruto
        if s.endswith("USDT") or s.endswith("USDC"):
            base = s[:-4]
            quote = s[-4:]
        else:
            raise ValueError(f"Invalid spot symbol format: {spot_symbol}")
    return f"{base}{quote}"


def quantize_to_step(qty: Decimal, step_size: Decimal) -> Decimal:
    """
    Faz floor da quantidade para o múltiplo de step_size mais próximo.
    Garante que não passamos o limite de precision da Binance.
    """
    if step_size <= 0:
        raise ValueError("step_size must be > 0")
    return (qty // step_size) * step_size


def format_quantity(qty: Decimal, qty_decimals: int) -> str:
    """
    Formata a quantity com exatamente qty_decimals casas decimais,
    para não rebentar a precision da Binance.
    """
    if qty_decimals <= 0:
        # Sem decimais
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

    devolve (raw_qty, final_qty, final_qty_str)
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
    else:  # SELL
        risk_per_unit = stop_loss - entry

    # Garantir valor positivo
    if risk_per_unit <= 0:
        raise ValueError(
            f"Invalid SL distance for {direction}: entry={entry}, stop={stop_loss}"
        )

    risk_capital = balance * (risk_pct / Decimal("100"))
    raw_qty = risk_capital / risk_per_unit

    # Ajustar para step_size (floor) e formatar
    adj_qty = quantize_to_step(raw_qty, step_size)

    # Se por algum motivo ficar abaixo do min step, falha
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
# Supabase
# -----------------------------------------------------------------------------
def fetch_latest_open_signal() -> Optional[Dict[str, Any]]:
    """
    Busca o último sinal com status 'open' (ou simplesmente o último, se quiseres mudar).
    """
    print("-> Fetching latest signal from 'signals'...")

    # Se quiseres ignorar o status, tira o .eq("status", "open")
    res = (
        SUPABASE.table(SIGNALS_TABLE)
        .select("*")
        .eq("status", "open")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    data = res.data or []
    if not data:
        print("No open signals found.")
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


# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    print("Starting Container")
    print("-> Connecting to Supabase...")

    signal = fetch_latest_open_signal()
    if not signal:
        return

    symbol_spot = str(signal["symbol"])
    direction = str(signal["direction"]).upper()
    entry = Decimal(str(signal["entry"]))
    stop_loss = Decimal(str(signal["stop_loss"]))

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

    print("=== FUTURES ORDER TO SEND (TESTNET) ===" if BINANCE_TESTNET else "=== FUTURES ORDER TO SEND (LIVE) ===")
    print(f"Symbol    : {futures_symbol}")
    print(f"Side      : {direction}")
    print(f"Entry     : {entry}")
    print(f"Stop Loss : {stop_loss}")
    print(f"Quantity  : {qty_str}")
    print("=======================================")

    if DRY_RUN:
        print("DRY_RUN=1 -> Ordem NÃO enviada, apenas simulação.")
        return

    print(f"-> Sending MARKET order to {ENV_LABEL}...")

    # Ordem MARKET simples, sem SL/TP nesta chamada.
    params: Dict[str, Any] = {
        "symbol": futures_symbol,
        "side": direction,
        "type": "MARKET",
        "quantity": qty_str,  # já formatado com a precision correta
    }

    # Se quiseres mandar como reduceOnly, etc., podes adicionar mais params aqui.

    resp = signed_request("POST", "/fapi/v1/order", params)
    order = resp.json()

    print("Order response:")
    print(order)


if __name__ == "__main__":
    main()

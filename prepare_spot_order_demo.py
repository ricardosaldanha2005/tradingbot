# pip install supabase==2.6.0 gotrue==2.4.2 storage3==0.7.6 httpx==0.27.2 python-dotenv requests

import os
import time
import hmac
import hashlib
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

# ---------- ENV ----------

BINANCE_MODE = os.getenv("BINANCE_MODE", "").upper()
API_KEY = os.getenv("BINANCE_API_KEY_TEST", "")
API_SECRET = os.getenv("BINANCE_API_SECRET_TEST", "").encode()

BASE_URL = "https://testnet.binance.vision"

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE = os.getenv("SUPABASE_SERVICE_ROLE", "")
SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")

RISK_PER_TRADE_PCT = float(os.getenv("RISK_PER_TRADE_PCT", "0.5"))
BASE_ASSET = os.getenv("BASE_ASSET", "USDT").upper()


# ---------- MODELOS ----------

@dataclass
class Signal:
    id: Any
    symbol_raw: str
    direction: str
    entry: float
    tp1: float
    stop_loss: float
    status: Optional[str]


@dataclass
class SymbolFilters:
    min_qty: float
    step_size: float
    min_notional: float


# ---------- SUPABASE ----------

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)


def fetch_latest_signal(sb: Client) -> Optional[Signal]:
    """Vai buscar o sinal mais recente da tabela, sem filtros de status."""
    res = (
        sb.table(SIGNALS_TABLE)
        .select("*")
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )

    data: list[dict[str, Any]] = res.data or []
    if not data:
        return None

    row = data[0]

    def to_float(x: Any) -> float:
        if x is None:
            raise ValueError("Campo numérico em falta no sinal")
        return float(x)

    return Signal(
        id=row.get("id"),
        symbol_raw=row.get("symbol"),
        direction=str(row.get("direction") or "").upper(),
        entry=to_float(row.get("entry")),
        tp1=to_float(row.get("tp1")),
        stop_loss=to_float(row.get("stop_loss")),
        status=row.get("status"),
    )


# ---------- BINANCE CLIENT ----------

def public_request(path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Binance public error {resp.status_code}: {resp.text}")
    return resp.json()


def signed_request(method: str, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
    if params is None:
        params = {}

    params["timestamp"] = int(time.time() * 1000)
    params["recvWindow"] = 5000

    query_string = urlencode(params, doseq=True)
    signature = hmac.new(API_SECRET, query_string.encode(), hashlib.sha256).hexdigest()
    query_string += f"&signature={signature}"

    url = f"{BASE_URL}{path}?{query_string}"
    headers = {"X-MBX-APIKEY": API_KEY}

    resp = requests.request(method, url, headers=headers, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Binance signed error {resp.status_code}: {resp.text}")
    return resp.json()


def get_symbol_filters(symbol: str) -> SymbolFilters:
    """Vai buscar minQty, stepSize e minNotional para um símbolo (ex: BNBUSDT)."""
    info = public_request("/api/v3/exchangeInfo", {"symbol": symbol})
    symbols = info.get("symbols", [])
    if not symbols:
        raise RuntimeError(f"Symbol {symbol} not found in exchangeInfo")

    s = symbols[0]
    min_qty = 0.0
    step_size = 0.0
    min_notional = 0.0

    for f in s.get("filters", []):
        ftype = f.get("filterType")
        if ftype == "LOT_SIZE":
            min_qty = float(f.get("minQty", "0"))
            step_size = float(f.get("stepSize", "0"))
        elif ftype == "MIN_NOTIONAL":
            min_notional = float(f.get("minNotional", "0"))

    if step_size <= 0 or min_qty <= 0:
        raise RuntimeError(f"Invalid LOT_SIZE filters for {symbol}: min_qty={min_qty}, step_size={step_size}")

    return SymbolFilters(
        min_qty=min_qty,
        step_size=step_size,
        min_notional=min_notional,
    )


def get_base_asset_balance(asset: str) -> float:
    """Lê o saldo free de um asset (ex: USDT) na conta testnet."""
    account = signed_request("GET", "/api/v3/account")
    balances = account.get("balances", [])
    for b in balances:
        if b.get("asset") == asset:
            return float(b.get("free", "0"))
    return 0.0


# ---------- RISCO / QUANTIDADE ----------

def floor_to_step(value: float, step: float) -> float:
    if step <= 0:
        return value
    return (value // step) * step


def calculate_order_quantity(
    balance: float,
    risk_pct: float,
    entry_price: float,
    filters: SymbolFilters,
) -> float:
    """
    Calcula a quantidade a comprar com base em:
      - balance em BASE_ASSET
      - risk_pct (% da banca)
      - entry_price
      - filtros da Binance (min_qty, step_size, min_notional)
    """
    if balance <= 0 or entry_price <= 0 or risk_pct <= 0:
        return 0.0

    risk_amount = balance * (risk_pct / 100.0)  # em USDT, por ex.
    raw_qty = risk_amount / entry_price

    # aplica stepSize
    qty = floor_to_step(raw_qty, filters.step_size)

    # respeita min_qty
    if qty < filters.min_qty:
        return 0.0

    notional = qty * entry_price
    if notional < filters.min_notional:
        return 0.0

    return qty


# ---------- MAIN ----------

def main() -> None:
    # segurança: só testnet por enquanto
    if BINANCE_MODE != "TESTNET":
        raise SystemExit(f"BINANCE_MODE must be TESTNET, got: {BINANCE_MODE!r}")

    if not API_KEY or not API_SECRET:
        raise SystemExit("Missing BINANCE_API_KEY_TEST or BINANCE_API_SECRET_TEST in env")

    print("-> Connecting to Supabase...")
    sb = get_supabase_client()

    print(f"-> Fetching latest signal from '{SIGNALS_TABLE}'...")
    signal = fetch_latest_signal(sb)

    if signal is None:
        print("No signals found in table.")
        return

    print("Found signal:")
    print(f"  id        : {signal.id}")
    print(f"  symbol    : {signal.symbol_raw}")
    print(f"  direction : {signal.direction}")
    print(f"  status    : {signal.status}")
    print(f"  entry     : {signal.entry}")
    print(f"  tp1       : {signal.tp1}")
    print(f"  stop_loss : {signal.stop_loss}")

    # Normalizar símbolo para Binance: "BNB/USDT" -> "BNBUSDT"
    if not signal.symbol_raw or "/" not in signal.symbol_raw:
        raise SystemExit(f"Unexpected symbol format: {signal.symbol_raw!r}")

    binance_symbol = signal.symbol_raw.replace("/", "")
    print(f"-> Normalized symbol for Binance: {binance_symbol}")

    print("-> Fetching symbol filters from Binance TESTNET...")
    filters = get_symbol_filters(binance_symbol)
    print(f"  min_qty     : {filters.min_qty}")
    print(f"  step_size   : {filters.step_size}")
    print(f"  min_notional: {filters.min_notional}")

    print(f"-> Fetching {BASE_ASSET} balance from Binance TESTNET...")
    balance = get_base_asset_balance(BASE_ASSET)
    print(f"  {BASE_ASSET} balance: {balance}")

    if balance <= 0:
        print("No balance available, cannot calculate quantity.")
        return

    print(f"-> Calculating quantity with RISK_PER_TRADE_PCT={RISK_PER_TRADE_PCT}%...")
    qty = calculate_order_quantity(
        balance=balance,
        risk_pct=RISK_PER_TRADE_PCT,
        entry_price=signal.entry,
        filters=filters,
    )

    if qty <= 0:
        print("Calculated quantity is 0 (likely below minQty/minNotional). Cannot place order.")
        return

    notional = qty * signal.entry

    print("\n=== ORDER PREVIEW (NO ORDER SENT) ===")
    print(f"Symbol     : {binance_symbol}")
    print(f"Direction  : {signal.direction}  (script AINDA não valida BUY/SELL, é só preview)")
    print(f"Entry      : {signal.entry}")
    print(f"TP1        : {signal.tp1}")
    print(f"Stop Loss  : {signal.stop_loss}")
    print(f"Quantity   : {qty}")
    print(f"Notional   : {notional}")
    print("=====================================")
    print("Neste passo NÃO é enviada ordem nenhuma, é só para validar sizing e parâmetros.")


if __name__ == "__main__":
    main()

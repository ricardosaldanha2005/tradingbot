# sync_entry_orders_from_binance.py
#
# Lê sinais em que já foi colocada uma ordem de entrada na Binance
# (exchange_sync_status = 'entry_placed') e pergunta à Binance TESTNET
# o estado real dessa ordem. Atualiza exchange_sync_status na tabela 'signals'
# com base no status da Binance.
#
# Por agora:
#   - NEW / PARTIALLY_FILLED -> entry_placed (mantém)
#   - FILLED                 -> entry_filled
#   - CANCELED/EXPIRED/...   -> entry_cancelled
#
# Isto tira-te a dependência do avaliador por candles para saber se a entrada abriu.

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


# ---------- MODELOS ----------

@dataclass
class SignalEntry:
    id: Any
    binance_symbol: str
    binance_entry_order_id: int
    exchange_sync_status: Optional[str]


# ---------- SUPABASE ----------

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)


def fetch_signals_to_sync(sb: Client) -> list[SignalEntry]:
    """
    Vai buscar sinais que já têm ordem de entrada colocada na Binance
    e ainda estão com exchange_sync_status = 'entry_placed'.
    """
    res = (
        sb.table(SIGNALS_TABLE)
        .select("id, binance_symbol, binance_entry_order_id, exchange_sync_status")
        .eq("exchange_sync_status", "entry_placed")
        .not_.is_("binance_entry_order_id", "null")  # garante que temos um order id
        .limit(100)
        .execute()
    )

    data: list[dict[str, Any]] = res.data or []
    out: list[SignalEntry] = []

    for row in data:
        try:
            order_id_raw = row.get("binance_entry_order_id")
            if order_id_raw is None:
                continue
            out.append(
                SignalEntry(
                    id=row.get("id"),
                    binance_symbol=str(row.get("binance_symbol") or ""),
                    binance_entry_order_id=int(order_id_raw),
                    exchange_sync_status=row.get("exchange_sync_status"),
                )
            )
        except Exception as exc:
            print(f"Skipping row {row.get('id')} due to error: {exc}")
            continue

    return out


def update_exchange_sync_status(
    sb: Client,
    signal_id: Any,
    new_status: str,
) -> None:
    (
        sb.table(SIGNALS_TABLE)
        .update({"exchange_sync_status": new_status})
        .eq("id", signal_id)
        .execute()
    )


# ---------- BINANCE CLIENT ----------

def signed_request(
    method: str,
    path: str,
    params: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
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


def get_order_status(symbol: str, order_id: int) -> dict[str, Any]:
    """
    Lê o estado de uma ordem específica na Binance TESTNET (Spot).
    """
    params = {
        "symbol": symbol,
        "orderId": order_id,
    }
    return signed_request("GET", "/api/v3/order", params)


# ---------- MAIN ----------

def main() -> None:
    if BINANCE_MODE != "TESTNET":
        raise SystemExit(f"BINANCE_MODE must be TESTNET, got: {BINANCE_MODE!r}")
    if not API_KEY or not API_SECRET:
        raise SystemExit("Missing BINANCE_API_KEY_TEST or BINANCE_API_SECRET_TEST in env")

    print("-> Connecting to Supabase...")
    sb = get_supabase_client()

    print("-> Fetching signals with entry_placed to sync from Binance...")
    signals = fetch_signals_to_sync(sb)

    if not signals:
        print("No signals to sync.")
        return

    print(f"Found {len(signals)} signal(s) to sync.")

    for s in signals:
        if not s.binance_symbol:
            print(f"- Signal {s.id}: missing binance_symbol, skipping.")
            continue

        print(f"\n=== Checking entry order for signal {s.id} ===")
        print(f"Symbol    : {s.binance_symbol}")
        print(f"Order ID  : {s.binance_entry_order_id}")

        try:
            order = get_order_status(
                symbol=s.binance_symbol,
                order_id=s.binance_entry_order_id,
            )
        except Exception as exc:
            print(f"  Error fetching order from Binance: {exc}")
            continue

        status = order.get("status")
        executed_qty = float(order.get("executedQty", "0"))
        orig_qty = float(order.get("origQty", "0"))

        print(f"  Binance status : {status}")
        print(f"  executedQty    : {executed_qty}")
        print(f"  origQty        : {orig_qty}")

        new_sync_status: Optional[str] = None

        if status in ("NEW", "PARTIALLY_FILLED"):
            # mantemos entry_placed por enquanto
            new_sync_status = "entry_placed"
        elif status == "FILLED":
            new_sync_status = "entry_filled"
        elif status in ("CANCELED", "EXPIRED", "REJECTED"):
            new_sync_status = "entry_cancelled"
        else:
            # outros estados pouco comuns: mantemos visível nos logs
            new_sync_status = f"entry_{status.lower()}"

        if new_sync_status and new_sync_status != s.exchange_sync_status:
            print(f"  Updating exchange_sync_status -> {new_sync_status}")
            update_exchange_sync_status(sb, s.id, new_sync_status)
        else:
            print("  No status change.")

    print("\nSync done.")


if __name__ == "__main__":
    main()

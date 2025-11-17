# create_tp1_sl_oco_for_filled_entries.py
#
# Para cada sinal com:
#   - exchange_sync_status = 'entry_filled'
#   - binance_tp1_order_id IS NULL
#   - binance_sl_order_id IS NULL
# cria um OCO na Binance TESTNET com:
#   - TP1 (LIMIT)
#   - SL (STOP_LOSS_LIMIT)
#
# Depois atualiza a tabela signals com:
#   - binance_tp1_order_id
#   - binance_sl_order_id
#   - exchange_sync_status = 'tp1_sl_placed'

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
class FilledSignal:
    id: Any
    binance_symbol: str
    direction: str
    entry: float
    tp1: float
    stop_loss: float
    binance_entry_order_id: int
    exchange_sync_status: Optional[str]


# ---------- SUPABASE ----------

def get_supabase_client() -> Client:
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE:
        raise SystemExit("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE in env")
    return create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE)


def fetch_filled_signals_without_tp1_sl(sb: Client) -> list[FilledSignal]:
    """
    Sinais que já têm entrada preenchida na Binance,
    mas ainda não foi colocada TP1/SL (binance_tp1_order_id e binance_sl_order_id nulos).
    """
    res = (
        sb.table(SIGNALS_TABLE)
        .select(
            "id, binance_symbol, direction, entry, tp1, stop_loss, "
            "binance_entry_order_id, exchange_sync_status, "
            "binance_tp1_order_id, binance_sl_order_id"
        )
        .eq("exchange_sync_status", "entry_filled")
        .is_("binance_tp1_order_id", "null")
        .is_("binance_sl_order_id", "null")
        .limit(50)
        .execute()
    )

    data: list[dict[str, Any]] = res.data or []
    out: list[FilledSignal] = []

    for row in data:
        try:
            def to_float(x: Any, field: str) -> float:
                if x is None:
                    raise ValueError(f"Campo {field} em falta")
                return float(x)

            order_id_raw = row.get("binance_entry_order_id")
            if order_id_raw is None:
                continue

            out.append(
                FilledSignal(
                    id=row.get("id"),
                    binance_symbol=str(row.get("binance_symbol") or ""),
                    direction=str(row.get("direction") or "").upper(),
                    entry=to_float(row.get("entry"), "entry"),
                    tp1=to_float(row.get("tp1"), "tp1"),
                    stop_loss=to_float(row.get("stop_loss"), "stop_loss"),
                    binance_entry_order_id=int(order_id_raw),
                    exchange_sync_status=row.get("exchange_sync_status"),
                )
            )
        except Exception as exc:
            print(f"Skipping row {row.get('id')} due to error: {exc}")
            continue

    return out


def update_tp1_sl_info(
    sb: Client,
    signal_id: Any,
    tp1_order_id: int,
    sl_order_id: int,
) -> None:
    (
        sb.table(SIGNALS_TABLE)
        .update(
            {
                "binance_tp1_order_id": tp1_order_id,
                "binance_sl_order_id": sl_order_id,
                "exchange_sync_status": "tp1_sl_placed",
            }
        )
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


def get_order(symbol: str, order_id: int) -> dict[str, Any]:
    return signed_request(
        "GET",
        "/api/v3/order",
        {"symbol": symbol, "orderId": order_id},
    )


def place_oco_tp1_sl(
    symbol: str,
    side: str,
    quantity: float,
    tp_price: float,
    sl_price: float,
) -> dict[str, Any]:
    """
    Cria um OCO:
      - LIMIT (TP1) em tp_price
      - STOP_LOSS_LIMIT (SL) em sl_price
    """
    params: dict[str, Any] = {
        "symbol": symbol,
        "side": side,  # SELL se estiveres long, BUY se estiveres short
        "quantity": f"{quantity:.8f}",
        "price": f"{tp_price:.8f}",  # TP1
        "stopPrice": f"{sl_price:.8f}",  # trigger do SL
        "stopLimitPrice": f"{sl_price:.8f}",  # preço da ordem STOP_LIMIT
        "stopLimitTimeInForce": "GTC",
        "newOrderRespType": "FULL",
    }

    print("-> Sending OCO (TP1 + SL) to Binance TESTNET...")
    print(
        f"   {symbol} {side} qty={params['quantity']} "
        f"TP1={params['price']} SL={params['stopPrice']}"
    )

    # Endpoint oficial OCO
    resp = signed_request("POST", "/api/v3/order/oco", params)
    return resp


# ---------- HELPERS ----------

def parse_oco_leg_ids(oco_resp: dict[str, Any]) -> tuple[int, int]:
    """
    A resposta do OCO tem algo como:
      - "orders": [ { orderId, type, ... }, { orderId, type, ... } ]
    Queremos identificar qual é o TP (LIMIT/LIMIT_MAKER) e qual é o SL (STOP_LOSS_LIMIT).
    """
    orders = oco_resp.get("orders", [])
    tp_id = None
    sl_id = None

    for o in orders:
        otype = o.get("type")
        oid = int(o.get("orderId"))
        if otype in ("LIMIT", "LIMIT_MAKER"):
            tp_id = oid
        elif otype in ("STOP_LOSS_LIMIT", "STOP_LOSS"):
            sl_id = oid

    if tp_id is None or sl_id is None:
        raise RuntimeError(f"Could not parse TP/SL orderIds from OCO response: {oco_resp}")

    return tp_id, sl_id


# ---------- MAIN ----------

def main() -> None:
    if BINANCE_MODE != "TESTNET":
        raise SystemExit(f"BINANCE_MODE must be TESTNET, got: {BINANCE_MODE!r}")
    if not API_KEY or not API_SECRET:
        raise SystemExit("Missing BINANCE_API_KEY_TEST or BINANCE_API_SECRET_TEST in env")

    print("-> Connecting to Supabase...")
    sb = get_supabase_client()

    print("-> Fetching filled signals without TP1/SL OCO...")
    signals = fetch_filled_signals_without_tp1_sl(sb)

    if not signals:
        print("No filled signals without TP1/SL to process.")
        return

    print(f"Found {len(signals)} signal(s).")

    for s in signals:
        if not s.binance_symbol:
            print(f"- Signal {s.id}: missing binance_symbol, skipping.")
            continue

        print(f"\n=== Processing signal {s.id} ===")
        print(f"Symbol         : {s.binance_symbol}")
        print(f"Direction      : {s.direction}")
        print(f"Entry          : {s.entry}")
        print(f"TP1            : {s.tp1}")
        print(f"Stop Loss      : {s.stop_loss}")
        print(f"Entry order ID : {s.binance_entry_order_id}")

        # 1) Ler qty efetivamente executada na ordem de entrada
        try:
            entry_order = get_order(s.binance_symbol, s.binance_entry_order_id)
        except Exception as exc:
            print(f"  Error fetching entry order from Binance: {exc}")
            continue

        orig_qty = float(entry_order.get("origQty", "0"))
        executed_qty = float(entry_order.get("executedQty", "0"))
        status = entry_order.get("status")

        print(f"  Entry status   : {status}")
        print(f"  origQty        : {orig_qty}")
        print(f"  executedQty    : {executed_qty}")

        # Segurança mínima: só avançamos se estiver FILLED
        if status != "FILLED" or executed_qty <= 0:
            print("  Entry not fully filled, skipping.")
            continue

        qty = executed_qty

        # 2) Definir side para fechar posição
        if s.direction == "BUY":
            close_side = "SELL"
        elif s.direction == "SELL":
            close_side = "BUY"
        else:
            print(f"  Unknown direction {s.direction}, skipping.")
            continue

        # 3) Criar OCO TP1 + SL
        try:
            oco_resp = place_oco_tp1_sl(
                symbol=s.binance_symbol,
                side=close_side,
                quantity=qty,
                tp_price=s.tp1,
                sl_price=s.stop_loss,
            )
        except Exception as exc:
            print(f"  Error placing OCO order: {exc}")
            continue

        print("  OCO response:")
        print(oco_resp)

        try:
            tp_id, sl_id = parse_oco_leg_ids(oco_resp)
        except Exception as exc:
            print(f"  Error parsing OCO leg ids: {exc}")
            continue

        print(f"  TP1 order id : {tp_id}")
        print(f"  SL  order id : {sl_id}")

        # 4) Atualizar sinal no Supabase
        print("  Updating signal with TP1/SL order ids...")
        update_tp1_sl_info(
            sb=sb,
            signal_id=s.id,
            tp1_order_id=tp_id,
            sl_order_id=sl_id,
        )
        print("  Done.")

    print("\nAll done.")


if __name__ == "__main__":
    main()

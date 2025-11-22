#!/usr/bin/env python3
"""
send_futures_entry_from_signal.py

Lê sinais na tabela 'signals' do Supabase e envia
ordens MARKET para a Binance FUTURES (TESTNET ou real),
calculando a quantidade com base no risco por trade e no stop loss.

Depois de a ordem de entrada ser preenchida, cria automaticamente:
- 1 STOP_MARKET de Stop Loss (quantity fixa + reduceOnly=True)
- até 3 TAKE_PROFIT_MARKET (TP1, TP2, TP3) opcionais, se o sinal tiver tp1/tp2/tp3

Este script corre em loop:
- procura sinais com status='open' e futures_entry_sent=false
- processa 1 sinal de cada vez
- no fim marca futures_entry_sent=true para não voltar a repetir o mesmo sinal
- periodicamente limpa SL/TPs reduceOnly de símbolos sem posição ativa

ENV necessários:

  SUPABASE_URL=...
  SUPABASE_SERVICE_ROLE=...

  SIGNALS_TABLE=signals             # opcional, default 'signals'

  BINANCE_FUTURES_API_KEY=...
  BINANCE_FUTURES_SECRET_KEY=...
  BINANCE_FUTURES_TESTNET=1         # 1 = demo-fapi.binance.com ; 0 = fapi.binance.com

  RISK_PER_TRADE_PCT=0.5            # percentagem da banca arriscada por trade (ex: 0.5)

  # Gestão de saldo:
  FUTURES_BALANCE_ASSET=USDT        # ativo base da conta futures, default 'USDT'
  FUTURES_BALANCE_USE_AVAILABLE=1   # 1 = usar availableBalance, 0 = usar balance/walletBalance
  MIN_FUTURES_BALANCE_USDT=0        # se >0 e saldo < min, não abre posição (mantém sinal pendente)

  DRY_RUN=0                         # 1 = não envia ordem, só mostra (mas marca o sinal como tratado)
  MAX_FUTURES_NOTIONAL_USDT=1500    # notional máximo por trade (USDT). Opcional.

  TP1_FRACTION=0.33                 # fração da quantidade total para o TP1
  TP2_FRACTION=0.33                 # fração da quantidade total para o TP2
  TP3_FRACTION=0.34                 # fração da quantidade total para o TP3

  # Limpeza de SL/TP órfãos:
  CLEANUP_SLTP_ENABLED=1            # 1 = ativa limpeza de SL/TP sem posição, 0 = desativa
  CLEANUP_INTERVAL_S=60             # intervalo mínimo entre limpezas, em segundos
"""

import os
import time
import hmac
import hashlib
from decimal import Decimal
from typing import Any, Dict, Tuple, Optional, List
import datetime as dt

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
FUTURES_BALANCE_USE_AVAILABLE = os.getenv("FUTURES_BALANCE_USE_AVAILABLE", "1") == "1"
MIN_FUTURES_BALANCE_USDT = Decimal(os.getenv("MIN_FUTURES_BALANCE_USDT", "0"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"
MAX_FUTURES_NOTIONAL_USDT = os.getenv("MAX_FUTURES_NOTIONAL_USDT")

TP1_FRACTION = Decimal(os.getenv("TP1_FRACTION", "0.33"))
TP2_FRACTION = Decimal(os.getenv("TP2_FRACTION", "0.33"))
TP3_FRACTION = Decimal(os.getenv("TP3_FRACTION", "0.34"))

CLEANUP_SLTP_ENABLED = os.getenv("CLEANUP_SLTP_ENABLED", "1") == "1"
CLEANUP_INTERVAL_S = int(os.getenv("CLEANUP_INTERVAL_S", "60"))

# Timeout de requests para a Binance
REQUEST_TIMEOUT = 10


# -----------------------------------------------------------------------------
# Helpers Binance (assinatura, requests, filtros, balance, posições, limpeza)
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

    print(f"  min_qty            : {min_qty_str}")
    print(f"  step_size          : {step_size_str}")
    print(f"  quantityPrecision  : {info.get('quantityPrecision')}")
    print(f"  baseAssetPrecision : {info.get('baseAssetPrecision')}")
    print(f"  qty_decimals used  : {qty_decimals}")

    return min_qty, step_size, qty_decimals


def get_futures_balance(asset: str = "USDT", use_available: bool = False) -> Decimal:
    """
    Vai buscar o balance no futures para o asset dado.
    Usa /fapi/v2/balance.

    Se use_available=True, tenta usar availableBalance (saldo realmente disponível
    para abrir novas posições). Caso contrário, usa balance/walletBalance.
    """
    path = "/fapi/v2/balance"
    params: Dict[str, Any] = {}

    resp = signed_request("GET", path, params)
    data = resp.json()

    for entry in data:
        if entry.get("asset") == asset:
            wallet_str = entry.get("balance") or entry.get("walletBalance") or "0"
            avail_str = entry.get("availableBalance") or "0"

            wallet = Decimal(wallet_str)
            available = Decimal(avail_str)

            print(f"  {asset} wallet balance (futures)   : {wallet}")
            print(f"  {asset} availableBalance (futures): {available}")

            if use_available:
                # Se available for 0 (conta nova, sem posições), cai para wallet.
                chosen = available if available > 0 else wallet
                print(f"  -> Using AVAILABLE balance for sizing: {chosen}")
            else:
                chosen = wallet
                print(f"  -> Using WALLET balance for sizing: {chosen}")

            return chosen

    print(f"  {asset} balance (futures): 0 (not found)")
    return Decimal("0")


def ensure_isolated_1x(symbol: str) -> None:
    """
    Garante que o símbolo está em margem ISOLATED e leverage 1x.
    Corre antes de abrir a posição.
    """
    print(f"-> Ensuring {symbol} is on ISOLATED 1x ...")

    # 1) Mudar marginType para ISOLATED
    mt_params: Dict[str, Any] = {
        "symbol": symbol,
        "marginType": "ISOLATED",
    }
    try:
        resp = signed_request("POST", "/fapi/v1/marginType", mt_params)
        print("   marginType set to ISOLATED:")
        print("   ", resp.json())
    except requests.exceptions.HTTPError as e:
        r = getattr(e, "response", None)
        txt = r.text if r is not None else ""
        if "No need to change margin type" in txt or "-4046" in txt:
            print("   marginType already ISOLATED, OK.")
        else:
            print("   Error setting marginType to ISOLATED:")
            print("   ", txt)
            raise

    # 2) Mudar leverage para 1x
    lev_params: Dict[str, Any] = {
        "symbol": symbol,
        "leverage": 1,
    }
    try:
        resp = signed_request("POST", "/fapi/v1/leverage", lev_params)
        print("   leverage set to 1x:")
        print("   ", resp.json())
    except requests.exceptions.HTTPError as e:
        r = getattr(e, "response", None)
        txt = r.text if r is not None else ""
        print("   Error setting leverage to 1x:")
        print("   ", txt)
        raise


def get_active_futures_symbols() -> List[str]:
    """
    Devolve lista de símbolos futures com posição ativa (positionAmt != 0).
    Usa /fapi/v2/positionRisk.
    """
    path = "/fapi/v2/positionRisk"
    params: Dict[str, Any] = {}

    resp = signed_request("GET", path, params)
    data = resp.json()

    active_symbols: List[str] = []
    for pos in data:
        try:
            amt = Decimal(str(pos.get("positionAmt", "0")))
        except Exception:
            amt = Decimal("0")

        if amt != 0:
            sym = pos.get("symbol")
            if sym:
                active_symbols.append(sym)

    print(f"-> Active futures symbols (positionAmt != 0): {active_symbols}")
    return active_symbols


def cleanup_stale_sl_tp_orders() -> None:
    """
    Limpa ordens SL/TP reduceOnly em símbolos onde NÃO há posição ativa.

    Lógica:
      - Vai buscar símbolos com posição ativa (positionAmt != 0).
      - Vai buscar todas as openOrders.
      - Para cada ordem:
           se symbol NÃO está na lista de ativos
           e reduceOnly == true
           e type em {STOP_MARKET, TAKE_PROFIT_MARKET, STOP, TAKE_PROFIT}
           -> cancela a ordem.
    """
    if not CLEANUP_SLTP_ENABLED:
        return

    print("-> Cleanup SL/TP: checking for stale reduceOnly orders sem posição ativa...")

    active_symbols = set(get_active_futures_symbols())

    # Buscar todas as openOrders
    try:
        resp = signed_request("GET", "/fapi/v1/openOrders", params={})
    except Exception as e:
        print(f"   Error fetching openOrders for cleanup: {e}")
        return

    try:
        open_orders = resp.json()
    except Exception:
        print("   Error parsing openOrders JSON.")
        return

    if not isinstance(open_orders, list):
        print("   openOrders response is not a list; nothing to do.")
        return

    cancelled = 0
    candidate_types = {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT"}

    for o in open_orders:
        sym = o.get("symbol")
        if not sym:
            continue

        # Se tiver posição ativa, não mexemos
        if sym in active_symbols:
            continue

        # Só limpamos reduceOnly TP/SL
        reduce_only = o.get("reduceOnly", False)
        o_type = o.get("type")

        if not reduce_only:
            continue
        if o_type not in candidate_types:
            continue

        order_id = o.get("orderId")
        print(
            f"   Cancelling stale {o_type} reduceOnly order {order_id} on {sym} "
            f"(no active position)."
        )

        try:
            signed_request(
                "DELETE",
                "/fapi/v1/order",
                {"symbol": sym, "orderId": order_id},
            )
            cancelled += 1
        except Exception as e:
            print(f"   Error cancelling order {order_id} on {sym}: {e}")

    print(f"-> Cleanup SL/TP done. Cancelled {cancelled} stale orders.")


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
    print(f"  balance  : {balance}")
    print(f"  risk_cap : {risk_capital}")
    print(f"  raw_qty  : {raw_qty}")
    print(f"  qty      : {qty_str}")

    return raw_qty, adj_qty, qty_str


# -----------------------------------------------------------------------------
# Helpers de ordens: esperar fill
# -----------------------------------------------------------------------------
def wait_for_fill(
    symbol: str,
    order_id: int,
    timeout_s: float = 10.0,
    poll_interval_s: float = 0.5,
) -> Optional[Dict[str, Any]]:
    """
    Faz polling à Binance Futures até a ordem ficar FILLED ou o timeout expirar.
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
            print(f"-> Entry order ended with status={status}, aborting SL/TPs.")
            return None

        time.sleep(poll_interval_s)

    print("-> Timeout waiting for fill; SL/TP orders NOT sent.")
    return None


# -----------------------------------------------------------------------------
# Função genérica de ordem com retries de precisão
# -----------------------------------------------------------------------------
def send_order_with_precision_retries(
    symbol: str,
    side: str,
    base_params: Dict[str, Any],
    qty: Decimal,
    qty_decimals: int,
) -> Dict[str, Any]:
    """
    Envia uma ordem (MARKET, STOP_MARKET, TAKE_PROFIT_MARKET, etc.), tentando
    reduzir o nº de casas decimais na quantity se a Binance devolver o erro:
        "Precision is over the maximum defined for this asset."
    """
    last_error: Optional[Exception] = None

    for d in range(qty_decimals, -1, -1):
        qty_str = format_quantity(qty, d)
        params = {
            **base_params,
            "symbol": symbol,
            "side": side,
            "quantity": qty_str,
        }

        print(
            f"-> Trying {base_params.get('type')} order with quantity={qty_str} (decimals={d}) ..."
        )

        try:
            resp = signed_request("POST", "/fapi/v1/order", params)
            data = resp.json()
            print("Order response:")
            print(data)
            return data

        except requests.exceptions.HTTPError as e:
            resp = getattr(e, "response", None)
            text = resp.text if resp is not None else ""
            if (
                "Precision is over the maximum defined for this asset" in text
                and d > 0
            ):
                print("   -> Precision error, lowering decimals and retrying...")
                last_error = e
                continue

            # Outro erro, ou já estamos em 0 decimais: re-lança
            raise

    if last_error:
        raise last_error
    raise RuntimeError("Failed to send order due to unknown precision issue.")


def send_market_order_with_precision_retries(
    symbol: str,
    side: str,
    qty: Decimal,
    qty_decimals: int,
) -> Dict[str, Any]:
    """
    Wrapper específico para MARKET.
    """
    base_params: Dict[str, Any] = {
        "type": "MARKET",
    }
    return send_order_with_precision_retries(
        symbol=symbol,
        side=side,
        base_params=base_params,
        qty=qty,
        qty_decimals=qty_decimals,
    )


# -----------------------------------------------------------------------------
# SL + TP1/TP2/TP3
# -----------------------------------------------------------------------------
def place_sl_and_tp_orders(
    symbol: str,
    side_entry: str,
    qty: Decimal,
    stop_loss: Decimal,
    tp1: Optional[Decimal],
    tp2: Optional[Decimal],
    tp3: Optional[Decimal],
    tp1_fraction: Decimal,
    tp2_fraction: Decimal,
    tp3_fraction: Decimal,
    step_size: Decimal,
    min_qty: Decimal,
    qty_decimals: int,
) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
    """
    Cria:
      - 1 STOP_MARKET de SL (quantity = qty total, reduceOnly=True)
      - até 3 TAKE_PROFIT_MARKET (TP1, TP2, TP3) opcionais

    Ambos em reduceOnly, positionSide=BOTH.

    Devolve: (sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id)
    """
    side_entry = side_entry.upper()
    if side_entry not in ("BUY", "SELL"):
        raise ValueError(f"Invalid side_entry: {side_entry}")

    side_close = "SELL" if side_entry == "BUY" else "BUY"

    print("-> Placing SL + TP orders...")

    # Ajustar qty total à step_size
    total_qty = quantize_to_step(qty, step_size)
    if total_qty < min_qty:
        raise ValueError(
            f"Total qty {total_qty} < min_qty {min_qty}, cannot place SL/TPs."
        )

    # STOP LOSS (sempre full size)
    sl_base_params: Dict[str, Any] = {
        "type": "STOP_MARKET",
        "stopPrice": str(stop_loss),
        "reduceOnly": True,
        "workingType": "CONTRACT_PRICE",
        "positionSide": "BOTH",
    }

    print(
        f"   Sending STOP_MARKET (SL) qty≈{format_quantity(total_qty, qty_decimals)}, "
        f"price={stop_loss} ..."
    )
    sl_resp = send_order_with_precision_retries(
        symbol=symbol,
        side=side_close,
        base_params=sl_base_params,
        qty=total_qty,
        qty_decimals=qty_decimals,
    )
    print("   SL response:")
    print(sl_resp)

    sl_order_id: Optional[int] = None
    try:
        sl_order_id = int(sl_resp.get("orderId"))
    except Exception:
        sl_order_id = None

    # ---- Frações de TP (normalização básica) ----
    f1 = max(tp1_fraction, Decimal("0"))
    f2 = max(tp2_fraction, Decimal("0"))
    f3 = max(tp3_fraction, Decimal("0"))
    sum_f = f1 + f2 + f3

    if sum_f > Decimal("1"):
        print(f"   TP fractions sum to {sum_f} > 1; normalizing para somar 1.")
        factor = Decimal("1") / sum_f
        f1 *= factor
        f2 *= factor
        f3 *= factor

    # Vamos alocar qty sequencialmente para TP1 -> TP2 -> TP3
    remaining_qty = total_qty

    def make_tp(
        label: str,
        tp_price: Optional[Decimal],
        frac: Decimal,
        remaining: Decimal,
    ) -> Tuple[Optional[int], Decimal]:
        if tp_price is None:
            print(f"   No {label} in signal; skipping {label} order.")
            return None, remaining
        if tp_price <= 0:
            print(f"   Invalid {label}={tp_price}; skipping {label} order.")
            return None, remaining
        if frac <= 0:
            print(f"   {label}_FRACTION={frac} <= 0; skipping {label} order.")
            return None, remaining
        if remaining <= 0:
            print(f"   No remaining qty for {label}; skipping.")
            return None, remaining

        raw_qty = total_qty * frac
        tp_qty = quantize_to_step(raw_qty, step_size)

        # Não pode exceder o remaining_qty
        if tp_qty > remaining:
            tp_qty = quantize_to_step(remaining, step_size)

        if tp_qty < min_qty:
            print(
                f"   Computed {label} qty {tp_qty} < min_qty {min_qty}; skipping {label} order."
            )
            return None, remaining

        base_params: Dict[str, Any] = {
            "type": "TAKE_PROFIT_MARKET",
            "stopPrice": str(tp_price),
            "reduceOnly": True,
            "workingType": "CONTRACT_PRICE",
            "positionSide": "BOTH",
        }

        print(
            f"   Sending TAKE_PROFIT_MARKET ({label}) qty≈{format_quantity(tp_qty, qty_decimals)}, "
            f"price={tp_price} ..."
        )
        resp = send_order_with_precision_retries(
            symbol=symbol,
            side=side_close,
            base_params=base_params,
            qty=tp_qty,
            qty_decimals=qty_decimals,
        )
        print(f"   {label} response:")
        print(resp)

        order_id: Optional[int] = None
        try:
            order_id = int(resp.get("orderId"))
        except Exception:
            order_id = None

        new_remaining = remaining - tp_qty
        return order_id, new_remaining

    tp1_order_id: Optional[int]
    tp2_order_id: Optional[int]
    tp3_order_id: Optional[int]

    tp1_order_id, remaining_qty = make_tp("TP1", tp1, f1, remaining_qty)
    tp2_order_id, remaining_qty = make_tp("TP2", tp2, f2, remaining_qty)
    tp3_order_id, remaining_qty = make_tp("TP3", tp3, f3, remaining_qty)

    return sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id


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
    print(f"  stop_loss : {signal.get('stop_loss')}")
    print(f"  tp1       : {signal.get('tp1')}")
    print(f"  tp2       : {signal.get('tp2')}")
    print(f"  tp3       : {signal.get('tp3')}")

    return signal


# -----------------------------------------------------------------------------
# MAIN (loop infinito a processar sinais + limpeza periódica)
# -----------------------------------------------------------------------------
def main() -> None:
    print("Starting Container")
    print("-> Connecting to Supabase...")

    last_cleanup_ts = 0.0

    while True:
        try:
            # Limpeza periódica de SL/TP órfãos
            now = time.time()
            if CLEANUP_SLTP_ENABLED and (now - last_cleanup_ts) >= CLEANUP_INTERVAL_S:
                cleanup_stale_sl_tp_orders()
                last_cleanup_ts = now

            # Processar próximo sinal
            signal = fetch_latest_open_signal()
            if not signal:
                time.sleep(5)
                continue

            symbol_spot = str(signal["symbol"])
            direction = str(signal["direction"]).upper()
            entry = Decimal(str(signal["entry"]))
            stop_loss = Decimal(str(signal["stop_loss"]))

            tp1_val = signal.get("tp1")
            tp2_val = signal.get("tp2")
            tp3_val = signal.get("tp3")

            tp1: Optional[Decimal] = Decimal(str(tp1_val)) if tp1_val is not None else None
            tp2: Optional[Decimal] = Decimal(str(tp2_val)) if tp2_val is not None else None
            tp3: Optional[Decimal] = Decimal(str(tp3_val)) if tp3_val is not None else None

            futures_symbol = normalize_futures_symbol(symbol_spot)
            print(f"-> Normalized futures symbol: {futures_symbol}")

            print(f"-> Fetching symbol filters from {ENV_LABEL}...")
            min_qty, step_size, qty_decimals = get_symbol_filters(futures_symbol)

            # Garantir ISOLATED 1x antes de abrir posição
            ensure_isolated_1x(futures_symbol)

            print(
                f"-> Fetching {FUTURES_BALANCE_ASSET} futures balance from {ENV_LABEL} "
                f"(use_available={FUTURES_BALANCE_USE_AVAILABLE})..."
            )
            balance = get_futures_balance(
                asset=FUTURES_BALANCE_ASSET,
                use_available=FUTURES_BALANCE_USE_AVAILABLE,
            )

            # Se tiveres um mínimo de saldo exigido, aborta o trade se estiver abaixo
            if MIN_FUTURES_BALANCE_USDT > 0 and balance < MIN_FUTURES_BALANCE_USDT:
                print(
                    f"-> Balance {balance} {FUTURES_BALANCE_ASSET} < MIN_FUTURES_BALANCE_USDT={MIN_FUTURES_BALANCE_USDT}. "
                    f"Não vou abrir posição. Sinal mantém-se pendente."
                )
                time.sleep(10)
                continue

            raw_qty, adj_qty, qty_str = calculate_position_size(
                balance=balance,
                entry=entry,
                stop_loss=stop_loss,
                direction=direction,
                risk_pct=RISK_PER_TRADE_PCT,
                step_size=step_size,
                qty_decimals=qty_decimals,
            )

            # Opcional: limitar o notional máximo por trade
            if MAX_FUTURES_NOTIONAL_USDT:
                try:
                    max_notional = Decimal(MAX_FUTURES_NOTIONAL_USDT)
                except Exception:
                    max_notional = None

                if max_notional and max_notional > 0:
                    current_notional = entry * adj_qty
                    print(f"-> Current notional: {current_notional} {FUTURES_BALANCE_ASSET}")
                    print(f"-> Max notional allowed by config: {max_notional} {FUTURES_BALANCE_ASSET}")

                    if current_notional > max_notional:
                        print("-> Notional acima do limite configurado, vou reduzir a quantidade...")
                        # nova qty = max_notional / entry, ajustada ao step_size
                        new_qty = quantize_to_step(max_notional / entry, step_size)
                        if new_qty <= 0:
                            raise ValueError(
                                f"Clamped quantity <= 0 depois de aplicar MAX_FUTURES_NOTIONAL_USDT={max_notional}"
                            )
                        adj_qty = new_qty
                        qty_str = format_quantity(adj_qty, qty_decimals)
                        print(f"  qty (clamped) : {qty_str}")

            print(
                "=== FUTURES ORDER TO SEND (TESTNET) ==="
                if BINANCE_TESTNET
                else "=== FUTURES ORDER TO SEND (LIVE) ==="
            )
            print(f"Symbol    : {futures_symbol}")
            print(f"Side      : {direction}")
            print(f"Entry     : {entry}")
            print(f"Stop Loss : {stop_loss}")
            print(f"TP1       : {tp1 if tp1 is not None else 'N/A'}")
            print(f"TP2       : {tp2 if tp2 is not None else 'N/A'}")
            print(f"TP3       : {tp3 if tp3 is not None else 'N/A'}")
            print(f"Quantity  : {qty_str}")
            print("=======================================")

            if DRY_RUN:
                print("DRY_RUN=1 -> Ordem NÃO enviada, apenas simulação.")
                SUPABASE.table(SIGNALS_TABLE).update(
                    {
                        "futures_entry_sent": True,
                        "futures_symbol": futures_symbol,
                        "futures_entry_status": "DRY_RUN",
                        "entered_at": dt.datetime.utcnow().isoformat() + "Z",
                    }
                ).eq("id", signal["id"]).execute()
                print(f"-> Marked signal {signal['id']} as futures_entry_sent=true (DRY_RUN).")
                time.sleep(1)
                continue

            print(f"-> Sending MARKET order to {ENV_LABEL}...")

            order = send_market_order_with_precision_retries(
                symbol=futures_symbol,
                side=direction,
                qty=adj_qty,
                qty_decimals=qty_decimals,
            )

            order_id = order.get("orderId")
            if not isinstance(order_id, int):
                try:
                    order_id = int(order_id)
                except Exception:
                    print("-> Não consegui obter orderId válido; não envio SL/TPs.")
                    time.sleep(5)
                    continue

            filled_data = wait_for_fill(
                symbol=futures_symbol,
                order_id=order_id,
                timeout_s=10.0,
                poll_interval_s=0.5,
            )

            if not filled_data:
                time.sleep(5)
                continue

            # Quantidade realmente executada
            try:
                executed_qty = Decimal(str(filled_data.get("executedQty", "0")))
            except Exception:
                executed_qty = Decimal("0")

            if executed_qty <= 0:
                print("-> executedQty <= 0, vou usar adj_qty como fallback.")
                effective_qty = adj_qty
            else:
                effective_qty = quantize_to_step(executed_qty, step_size)
                if effective_qty <= 0:
                    raise ValueError(
                        f"executedQty {executed_qty} ajustada à step_size ficou <= 0."
                    )

            print(f"-> Effective filled qty for SL/TPs: {effective_qty}")

            # Enviar ordens de SL + TP1/TP2/TP3 com a qty efetivamente executada
            sl_order_id, tp1_order_id, tp2_order_id, tp3_order_id = place_sl_and_tp_orders(
                symbol=futures_symbol,
                side_entry=direction,
                qty=effective_qty,
                stop_loss=stop_loss,
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                tp1_fraction=TP1_FRACTION,
                tp2_fraction=TP2_FRACTION,
                tp3_fraction=TP3_FRACTION,
                step_size=step_size,
                min_qty=min_qty,
                qty_decimals=qty_decimals,
            )

            # Atualizar o sinal no Supabase
            update_payload: Dict[str, Any] = {
                "futures_entry_sent": True,
                "futures_symbol": futures_symbol,
                "futures_entry_order_id": order_id,
                "futures_entry_status": filled_data.get("status", "FILLED"),
                # marcar momento da entrada para disparar o webhook de "trade-entered"
                "entered_at": dt.datetime.utcnow().isoformat() + "Z",
            }

            # usar as colunas que EXISTEM na tua tabela (binance_*):
            if sl_order_id is not None:
                update_payload["binance_sl_order_id"] = sl_order_id
            if tp1_order_id is not None:
                update_payload["binance_tp1_order_id"] = tp1_order_id
            if tp2_order_id is not None:
                update_payload["binance_tp2_order_id"] = tp2_order_id
            if tp3_order_id is not None:
                update_payload["binance_tp3_order_id"] = tp3_order_id

            SUPABASE.table(SIGNALS_TABLE).update(update_payload)\
                .eq("id", signal["id"]).execute()

            print(f"-> Marked signal {signal['id']} as futures_entry_sent=true.")

        except Exception as e:
            print(f"Error processing signal: {e}")
            time.sleep(5)

        time.sleep(1)


if __name__ == "__main__":
    main()

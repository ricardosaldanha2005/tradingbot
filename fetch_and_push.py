#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import json
import math
import signal
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional

import requests
from dotenv import load_dotenv

# CCXT para recolher velas diretamente nas corretoras
try:
    import ccxt
except Exception:
    ccxt = None

# -----------------------
# Config & Logging
# -----------------------
load_dotenv()

SUPABASE_URL   = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY   = os.getenv("SUPABASE_SERVICE_ROLE") or os.getenv("SUPABASE_ANON_KEY", "")
FEAT_TABLE     = os.getenv("FEAT_TABLE", "features_snapshot")

UNIVERSE_TABLE       = os.getenv("UNIVERSE_TABLE", "universe")
UNIVERSE_ONLY_ACTIVE = os.getenv("UNIVERSE_ONLY_ACTIVE", "1") == "1"
UNIVERSE_ACTIVE_COL  = (os.getenv("UNIVERSE_ACTIVE_COL", "enabled") or "enabled").strip()

FETCH_SOURCE   = (os.getenv("FETCH_SOURCE", "ccxt") or "ccxt").lower()  # ccxt | supabase (não usamos supabase para velas aqui)
CANDLES_LIMIT  = int(os.getenv("CANDLES_LIMIT", "200"))

# scheduling
SCHED_MODE     = os.getenv("SCHED_MODE", "ALIGN_1H").upper()  # ALIGN_1H | FIXED_INTERVAL
FIXED_SECONDS  = int(os.getenv("FIXED_SECONDS", "300"))
ALIGN_DELAY_S  = int(os.getenv("ALIGN_DELAY_S", "60"))
RUN_IMMEDIATE  = os.getenv("RUN_IMMEDIATE", "0") == "1"

MAX_BACKOFF_S  = int(os.getenv("MAX_BACKOFF_S", "300"))

BASE_HEADERS   = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("fetchbot")

_running = True
def _handle_stop(signum, frame):
    global _running
    log.info(f"Signal {signum} recebido. A terminar no próximo ciclo…")
    _running = False

signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)

# -----------------------
# Utils simples
# -----------------------
def now_utc() -> datetime:
    return datetime.now(timezone.utc)

def sleep_until(ts: datetime):
    """Dorme até um datetime UTC, mas acorda rapidamente se receber SIGINT/SIGTERM."""
    while _running:
        secs = (ts - now_utc()).total_seconds()
        if secs <= 0:
            break
        time.sleep(min(1.0, secs))

def next_1h_slot(delay_s: int = 60) -> datetime:
    """Próximo HH:00 + delay_s (UTC)."""
    n = now_utc()
    top = (n.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    return top + timedelta(seconds=delay_s)

def _debug_sample(candles: List[Dict[str, Any]]) -> str:
    if not candles:
        return "0 velas"
    return f"{len(candles)} velas ({candles[0]['ts']} … {candles[-1]['ts']})"

# -----------------------
# Indicadores
# -----------------------
def ema(series: List[float], span: int) -> List[float]:
    k = 2/(span+1)
    out = []
    e = None
    for v in series:
        e = v if e is None else v*k + e*(1-k)
        out.append(e)
    return out

def rsi(series: List[float], p: int=14) -> List[Optional[float]]:
    if len(series) < p+1: return [None]*len(series)
    out = [None]*len(series)
    gains = sum(max(series[i]-series[i-1],0) for i in range(1, p+1))
    losses= sum(max(series[i-1]-series[i],0) for i in range(1, p+1))
    rs = (gains/p)/((losses/p) or 1e-9)
    out[p] = 100 - (100/(1+rs))
    g, l = gains/p, losses/p
    for i in range(p+1, len(series)):
        d = series[i]-series[i-1]
        g = (g*(p-1) + max(d,0))/p
        l = (l*(p-1) + max(-d,0))/p
        rs = g/(l or 1e-9)
        out[i] = 100 - (100/(1+rs))
    return out

def atr(high: List[float], low: List[float], close: List[float], p: int=14) -> List[Optional[float]]:
    if not close: return []
    tr = []
    for i in range(len(close)):
        prev = close[i-1] if i>0 else close[0]
        a = high[i]-low[i]
        b = abs(high[i]-prev)
        c = abs(low[i]-prev)
        tr.append(max(a,b,c))
    out = [None]*len(close)
    s = 0.0
    for i, v in enumerate(tr):
        s += v
        if i==p: out[i] = s/p
        elif i>p: out[i] = (out[i-1]*(p-1)+v)/p
    return out

def macd(series: List[float], f=12, s=26, sg=9):
    ef = ema(series, f); es = ema(series, s)
    m  = [ef[i]-es[i] for i in range(len(series))]
    sig= ema(m, sg)
    hist=[m[i]-sig[i] for i in range(len(m))]
    return m, sig, hist

# -----------------------
# Supabase helpers (universe + insert features)
# -----------------------
def sb_post(table: str, rows: List[Dict[str, Any]], on_conflict: Optional[str]=None):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = BASE_HEADERS.copy()
    if on_conflict:
        url += f"?on_conflict={on_conflict}"
        headers["Prefer"] = "resolution=merge-duplicates"
    resp = requests.post(url, headers=headers, data=json.dumps(rows), timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Supabase POST {table} {resp.status_code}: {resp.text}")
    return resp

def sb_get_universe() -> List[Dict[str, Any]]:
    """Lê a tabela universe e devolve [{exchange, symbol, timeframe}] só com enabled=true (se configurado)."""
    url = f"{SUPABASE_URL}/rest/v1/{UNIVERSE_TABLE}"
    params = {
        "select": "exchange,symbol,timeframe,enabled",
        "order": "symbol.asc",
    }
    if UNIVERSE_ONLY_ACTIVE:
        col = UNIVERSE_ACTIVE_COL.lower().strip()
        if col == "enable":  # deslize comum
            col = "enabled"
        if col not in ("enabled", "active"):
            log.warning(f"UNIVERSE_ACTIVE_COL='{UNIVERSE_ACTIVE_COL}' inesperado; a usar 'enabled'")
            col = "enabled"
        params[col] = "is.true"

    resp = requests.get(url, headers=BASE_HEADERS, params=params, timeout=30)
    if not resp.ok:
        raise RuntimeError(f"Supabase GET {UNIVERSE_TABLE} {resp.status_code}: {resp.text}")
    data = resp.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Resposta inesperada de {UNIVERSE_TABLE}")

    out = []
    for r in data:
        ex = str(r["exchange"]).lower().strip()
        sy = str(r["symbol"]).strip()
        tf = str(r["timeframe"]).lower().strip()
        out.append({"exchange": ex, "symbol": sy, "timeframe": tf})
    return out

# -----------------------
# CCXT adapters
# -----------------------
def get_ccxt_client(exchange: str):
    """Mapeia o nome da exchange para o cliente CCXT."""
    if ccxt is None:
        raise RuntimeError("ccxt não instalado. Instala com: pip install ccxt")

    ex = exchange.lower()
    if ex in ("binance", "binance_spot"):
        return ccxt.binance({"enableRateLimit": True})
    elif ex in ("bybit",):
        return ccxt.bybit({"enableRateLimit": True})
    elif ex in ("okx", "okex"):
        return ccxt.okx({"enableRateLimit": True})
    # adiciona mais se precisares
    else:
        raise RuntimeError(f"Exchange '{exchange}' não suportada neste coletor.")

def fetch_ohlcv_ccxt(exchange: str, symbol: str, timeframe: str, limit: int=200) -> List[Dict[str, Any]]:
    client = get_ccxt_client(exchange)
    data = client.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
    out = []
    for ts, o, h, l, c, v in data:
        out.append({
            "ts": datetime.fromtimestamp(ts/1000, tz=timezone.utc).isoformat(),
            "open": float(o), "high": float(h), "low": float(l),
            "close": float(c), "volume": float(v),
        })
    # CCXT devolve ASC
    return out

# -----------------------
# Core: calcular e gravar snapshot para (exchange,symbol,timeframe)
# -----------------------
def compute_and_upsert_snapshot(exchange: str, symbol: str, timeframe: str):
    if FETCH_SOURCE != "ccxt":
        log.warning(f"FETCH_SOURCE='{FETCH_SOURCE}' → a usar CCXT na mesma (o pipeline está desenhado para CCXT).")

    candles = fetch_ohlcv_ccxt(exchange, symbol, timeframe, limit=CANDLES_LIMIT)
    if not candles or len(candles) < 50:
        raise RuntimeError(f"Poucas velas devolvidas via CCXT: {_debug_sample(candles)}")

    candles = sorted(candles, key=lambda x: x["ts"])  # garante ASC

    close = [x["close"] for x in candles]
    high  = [x["high"]  for x in candles]
    low   = [x["low"]   for x in candles]

    ema21 = ema(close, 21)
    ema55 = ema(close, 55)
    atr14 = atr(high, low, close, 14)
    rsi14 = rsi(close, 14)
    macdLine, macdSig, macdHist = macd(close)

    # Bollinger width (20, 2σ)
    bb_width = None
    if len(close) >= 20:
        sl = close[-20:]
        mu = sum(sl)/len(sl)
        sd = math.sqrt(sum((x-mu)**2 for x in sl)/len(sl))
        upper = mu + 2*sd; lower = mu - 2*sd
        bb_width = (upper - lower) / (mu or 1e-9)

    ema_slope_pct = None
    if len(ema21) > 5 and ema21[-6] not in (None, 0):
        ema_slope_pct = (ema21[-1] - ema21[-6]) / ema21[-6] * 100

    ret_1h = None
    if len(close) >= 2 and close[-2] != 0:
        ret_1h = close[-1] / close[-2] - 1

    last_candle = candles[-1]
    last_ts = last_candle["ts"]

    row = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": timeframe,
        "ts": last_ts,
        # features
        "ema_fast": ema21[-1],
        "ema_slow": ema55[-1],
        "ema_slope_pct": ema_slope_pct,
        "atr": atr14[-1],
        "rsi": rsi14[-1],
        "macd": macdLine[-1],
        "macd_signal": macdSig[-1],
        "macd_hist": macdHist[-1],
        "bb_width": bb_width,
        "ret_1h": ret_1h,
        # OHLCV última vela
        "open":   float(last_candle["open"]),
        "high":   float(last_candle["high"]),
        "low":    float(last_candle["low"]),
        "close":  float(last_candle["close"]),
        "volume": float(last_candle["volume"]),
    }

    sb_post(FEAT_TABLE, [row], on_conflict="exchange,symbol,timeframe,ts")
    log.info(f"[{exchange}] {symbol} [{timeframe}] snapshot @ {last_ts} gravado (features + OHLCV).")

# -----------------------
# Main loop
# -----------------------
def main():
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise SystemExit("Configura SUPABASE_URL e SUPABASE_* no .env")
    if ccxt is None:
        raise SystemExit("ccxt não instalado. Instala com: pip install ccxt")

    # leitura inicial do universo
    try:
        universe = sb_get_universe()
        log.info(f"RUN_IMMEDIATE: {len(universe)} entradas no universo")
    except Exception as e:
        log.error(f"RUN_IMMEDIATE falhou ao ler universo: {e}", exc_info=True)
        universe = []

    if RUN_IMMEDIATE and universe:
        for u in universe:
            ex, sy, tf = u["exchange"], u["symbol"], u["timeframe"]
            try:
                compute_and_upsert_snapshot(ex, sy, tf)
            except Exception as e:
                log.error(f"RUN_IMMEDIATE erro {ex} {sy} {tf}: {e}", exc_info=True)

    backoff = 5
    while _running:
        try:
            universe = sb_get_universe()
            if not universe:
                log.warning("Universe vazio (enabled=true?). Vou reintentar após backoff.")
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF_S)
                continue

            if SCHED_MODE == "ALIGN_1H":
                target = next_1h_slot(ALIGN_DELAY_S)
                log.info(f"UTC now={now_utc().isoformat()} | next 1h slot={target.isoformat()} (delay={ALIGN_DELAY_S}s)")
                sleep_until(target)
                if not _running: break

                for u in universe:
                    ex, sy, tf = u["exchange"], u["symbol"], u["timeframe"]
                    log.info(f"Fetching {ex} {sy} [{tf}] …")
                    compute_and_upsert_snapshot(ex, sy, tf)

                backoff = 5  # reset
            else:
                start = now_utc()
                for u in universe:
                    ex, sy, tf = u["exchange"], u["symbol"], u["timeframe"]
                    log.info(f"Fetching {ex} {sy} [{tf}] …")
                    compute_and_upsert_snapshot(ex, sy, tf)
                elapsed = (now_utc() - start).total_seconds()
                wait = max(1, FIXED_SECONDS - elapsed)
                log.info(f"Intervalo FIXO: a dormir {int(wait)}s")
                time.sleep(wait)
                backoff = 5

        except Exception as e:
            log.error(f"Erro no ciclo principal: {e}", exc_info=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, MAX_BACKOFF_S)

    log.info("Encerrado com segurança.")

if __name__ == "__main__":
    main()

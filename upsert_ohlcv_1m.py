#!/usr/bin/env python3
# upsert_ohlcv_1m.py
# Recolhe OHLCV de 1 minuto via CCXT e faz upsert em Supabase (tabela raw_candles com coluna 'ts')
#
# Depêndencias:
#   pip install ccxt supabase
#
# Requer .env (ou variáveis de ambiente) com:
#   SUPABASE_URL=...
#   SUPABASE_SERVICE_ROLE=...
#   UNIVERSE_TABLE=universe
#   UNIVERSE_ONLY_ACTIVE=1
#   UNIVERSE_ACTIVE_COL=enabled
#   RAW_TABLE=raw_candles
#   TIMEFRAME=1m
#   CANDLES_LIMIT=500
#   BACKFILL_MINUTES=240
#   CHECK_SECONDS=30
#   RUN_IMMEDIATE=1
#   EXCHANGE (opcional — se estiveres a fixar um exchange)
#
# Chave única esperada:
#   create unique index raw_candles_unique_key on raw_candles (exchange, symbol, timeframe, ts);

import os
import time
import math
import traceback
import datetime as dt
from typing import List, Dict, Any

from dotenv import load_dotenv
load_dotenv()


import ccxt
from supabase import create_client, Client


# --------- ENV ---------
SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
supabase: Client = create_client(SB_URL, SB_KEY)

UNIVERSE_TABLE = os.getenv("UNIVERSE_TABLE", "universe")
UNIVERSE_ONLY_ACTIVE = os.getenv("UNIVERSE_ONLY_ACTIVE", "0") == "1"
UNIVERSE_ACTIVE_COL = os.getenv("UNIVERSE_ACTIVE_COL", "enabled")

RAW_TABLE = os.getenv("RAW_TABLE", "raw_candles")
TIMEFRAME = os.getenv("TIMEFRAME", "1m")
LIMIT = int(os.getenv("CANDLES_LIMIT", "500"))
BACKFILL_MIN = int(os.getenv("BACKFILL_MINUTES", "240"))
CHECK_SECONDS = int(os.getenv("CHECK_SECONDS", "30"))
RUN_IMMEDIATE = int(os.getenv("RUN_IMMEDIATE", "1"))

# Se quiseres forçar um único exchange via env (senão lê da universe.linha.exchange)
DEFAULT_EXCHANGE = os.getenv("EXCHANGE", "").strip() or None

LOG_PREFIX = "[upsert_ohlcv_1m]"


# --------- UTILS ---------
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def parse_iso_to_epoch_ms(s: str) -> int:
    """Converte ISO (com ou sem 'Z') para epoch ms."""
    if not s:
        return 0
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return int(dt.datetime.fromisoformat(s).timestamp() * 1000)


def epoch_ms_to_iso(ts_ms: int) -> str:
    return dt.datetime.utcfromtimestamp(ts_ms / 1000.0).replace(tzinfo=dt.timezone.utc).isoformat()


def chunked(seq, n):
    """Divide uma lista em blocos de tamanho n."""
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


# --------- DB ---------
def get_universe() -> List[Dict[str, Any]]:
    q = supabase.table(UNIVERSE_TABLE).select("*")
    if UNIVERSE_ONLY_ACTIVE:
        q = q.eq(UNIVERSE_ACTIVE_COL, True)
    res = q.execute()
    return res.data or []


def last_ts(exchange: str, symbol: str) -> str | None:
    """Obtém o último ts (ISO) existente na raw_candles para (exchange, symbol, TIMEFRAME)."""
    r = (
        supabase.table(RAW_TABLE)
        .select("ts")
        .eq("exchange", exchange)
        .eq("symbol", symbol)
        .eq("timeframe", TIMEFRAME)
        .order("ts", desc=True)
        .limit(1)
        .execute()
        .data
    )
    return r[0]["ts"] if r else None


def upsert_batch(rows: List[Dict[str, Any]]):
    if not rows:
        return
    supabase.table(RAW_TABLE).upsert(
        rows, on_conflict="exchange,symbol,timeframe,ts"
    ).execute()


# --------- CCXT ---------
def get_ccxt_instance(exchange_id: str):
    if not hasattr(ccxt, exchange_id):
        raise ValueError(f"Exchange '{exchange_id}' não existe em ccxt.")
    ex = getattr(ccxt, exchange_id)()
    ex.enableRateLimit = True
    # ajuda a corrigir defasagens do servidor
    try:
        ex.load_time_difference()
    except Exception:
        pass
    return ex


def fetch_ohlcv_all(exchange_id: str, symbol: str, since_ms: int, timeframe: str, limit: int) -> List[list]:
    """
    Faz paginação até alcançar 'agora' (ou ficar sem dados).
    Retorna lista concatenada de arrays OHLCV: [ts, open, high, low, close, volume]
    """
    ex = get_ccxt_instance(exchange_id)
    out: List[list] = []
    cursor = since_ms
    while True:
        try:
            batch = ex.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=limit)
        except Exception as e:
            # Alguns exchanges falham com since muito antigo; dá um pequeno salto
            print(f"{LOG_PREFIX} ERRO fetch_ohlcv {exchange_id} {symbol}: {e}")
            time.sleep(1)
            break

        if not batch:
            break

        out.extend(batch)

        # Avança 1 candle
        cursor_next = batch[-1][0] + 60_000  # 1m
        # Se o avanço não mexe (protecção para loops)
        if cursor_next <= cursor:
            break
        cursor = cursor_next

        # Se o lote veio pequeno, estamos perto do fim
        if len(batch) < limit:
            break

        # Pequena espera para respeitar rate-limit
        time.sleep(ex.rateLimit / 1000.0 if getattr(ex, "rateLimit", 0) else 0.2)

    return out


def map_ohlcv(exchange: str, symbol: str, timeframe: str, arr: List[list]) -> List[Dict[str, Any]]:
    """
    Converte OHLCV ccxt para linhas a inserir na raw_candles (com coluna 'ts' ISO).
    """
    rows = []
    for ts_ms, o, h, l, c, v in arr:
        rows.append({
            "exchange": exchange,
            "symbol": symbol,
            "timeframe": timeframe,
            "ts": epoch_ms_to_iso(ts_ms),
            "open": o,
            "high": h,
            "low": l,
            "close": c,
            "volume": v,
            "source": "rest",
        })
    return rows


# --------- WORKFLOW ---------
def fetch_and_upsert_pair(exchange_id: str, symbol: str):
    # decide 'since' com base no último ts já inserido
    last = last_ts(exchange_id, symbol)
    if last:
        since_ms = parse_iso_to_epoch_ms(last) + 60_000  # próximo minuto
    else:
        since_ms = int((now_utc() - dt.timedelta(minutes=BACKFILL_MIN)).timestamp() * 1000)

    ohlcv = fetch_ohlcv_all(exchange_id, symbol, since_ms, TIMEFRAME, LIMIT)
    if not ohlcv:
        return

    rows = map_ohlcv(exchange_id, symbol, TIMEFRAME, ohlcv)

    # Upsert em blocos para não enviar payload gigante
    for block in chunked(rows, 2000):
        upsert_batch(block)


def main_loop():
    print(f"{LOG_PREFIX} iniciado. TF={TIMEFRAME} backfill={BACKFILL_MIN}m interval={CHECK_SECONDS}s")

    while True:
        try:
            universe = get_universe()
            if not universe:
                print(f"{LOG_PREFIX} universo vazio (tabela {UNIVERSE_TABLE}).")
            else:
                # Agrupar por exchange (usa DEFAULT_EXCHANGE se definido; senão lê de cada linha)
                by_ex: dict[str, List[str]] = {}
                for u in universe:
                    ex = (DEFAULT_EXCHANGE or (u.get("exchange") or "binance")).lower()
                    sym = u["symbol"]
                    by_ex.setdefault(ex, []).append(sym)

                for ex_id, symbols in by_ex.items():
                    # Evita duplicados
                    symbols = sorted(set(symbols))
                    for sym in symbols:
                        try:
                            print(f"{LOG_PREFIX} {ex_id} {sym} → fetch+upsert 1m")
                            fetch_and_upsert_pair(ex_id, sym)
                        except Exception as e_sym:
                            print(f"{LOG_PREFIX} ERRO par {ex_id} {sym}: {e_sym}")
                            traceback.print_exc()
                            time.sleep(0.5)

        except Exception as e:
            print(f"{LOG_PREFIX} ERRO loop principal: {e}")
            traceback.print_exc()

        # Intervalo
        time.sleep(CHECK_SECONDS if CHECK_SECONDS > 0 else 30)


if __name__ == "__main__":
    main_loop()

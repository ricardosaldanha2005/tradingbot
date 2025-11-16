#!/usr/bin/env python3
# evaluate_signals.py
#
# Avalia sinais usando candles 1m (raw_candles.ts) e atualiza a tabela signals + signal_events.
#
# Política ajustada por Ricardo:
# - Após TP1 → SL = entry (break-even do remanescente)
# - Após TP2 → SL = TP1 (trailing stop)
# - Se voltar e bater SL após TP2 → fecha remanescente com lucro (sl_trailing)
#
# Tudo implementado nesta versão.

import os
import time
import datetime as dt
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
load_dotenv()

from supabase import create_client, Client

SB_URL = os.environ["SUPABASE_URL"]
SB_KEY = os.environ["SUPABASE_SERVICE_ROLE"]
supabase: Client = create_client(SB_URL, SB_KEY)

RAW_TABLE     = os.getenv("RAW_TABLE", "raw_candles")
SIGNALS_TABLE = os.getenv("SIGNALS_TABLE", "signals")
EVENTS_TABLE  = os.getenv("EVENTS_TABLE", "signal_events")
POLICY_VERSION = os.getenv("POLICY_VERSION", "v1_conservative_SL_priority")
LOOP_SECONDS   = int(os.getenv("LOOP_SECONDS", "30"))
BATCH_SIZE     = int(os.getenv("BATCH_SIZE", "200"))

LOG = "[evaluator]"

TP1_FRAC = 0.33
TP2_FRAC = 0.33
TP3_FRAC = 0.34


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def dt_to_iso(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).isoformat()

def fetch_open_signals(limit: int = 500) -> List[Dict[str, Any]]:
    q1 = (supabase.table(SIGNALS_TABLE)
          .select("*")
          .in_("status", ["open", "tp1", "tp2"])
          .order("created_at", desc=True)
          .limit(limit)
          .execute().data or [])
    ids = {x["id"] for x in q1}

    q2 = (supabase.table(SIGNALS_TABLE)
          .select("*")
          .eq("status", "open")
          .is_("entered_at", None)
          .order("created_at", desc=True)
          .limit(limit)
          .execute().data or [])

    out = q1[:]
    for x in q2:
        if x["id"] not in ids:
            out.append(x)
    return out[:limit]

def fetch_candles_1m(exchange: str, symbol: str, start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    return (supabase.table(RAW_TABLE)
            .select("ts,open,high,low,close")
            .eq("exchange", exchange)
            .eq("symbol", symbol)
            .eq("timeframe", "1m")
            .gte("ts", start_iso)
            .lte("ts", end_iso)
            .order("ts")
            .execute().data or [])

def log_event(signal_id: str, event: str, price: Optional[float], candle_ts: Optional[str],
              src: str = "1m", details: Optional[Dict[str, Any]] = None):

    UNIQUE_EVENTS = {"entered","hit_tp1","hit_tp2","hit_tp3","hit_sl","closed"}

    try:
        if event in UNIQUE_EVENTS:
            exists = (supabase.table(EVENTS_TABLE)
                      .select("id").eq("signal_id", signal_id)
                      .eq("event", event).limit(1)
                      .execute().data)
            if exists:
                return

        supabase.table(EVENTS_TABLE).insert({
            "signal_id": signal_id,
            "event": event,
            "price": price,
            "candle_start": candle_ts,
            "candle_timeframe": "1m",
            "src_timeframe": src,
            "details": details or {}
        }).execute()

    except:
        pass


def compute_mfe_mae(dire: str, entry: float, hi: float, lo: float) -> Tuple[float, float]:
    if dire == "BUY":
        return (hi - entry) / entry * 100.0, (lo - entry) / entry * 100.0
    else:
        return (entry - lo) / entry * 100.0, (entry - hi) / entry * 100.0

def r_at(price: float, entry: float, stop_initial: float, dire: str) -> float:
    risk = abs(entry - stop_initial)
    if risk == 0:
        return 0
    return (price - entry)/risk if dire=="BUY" else (entry - price)/risk


# -------------------------------------------------------------------
#     APPLY STATE MACHINE (VERSÃO CORRIGIDA E COMPLETA)
# -------------------------------------------------------------------
def apply_state_machine(sig: Dict[str, Any]) -> None:

    required = ["exchange", "symbol", "timeframe", "direction",
                "entry", "stop_loss", "tp1", "tp2", "tp3"]
    missing = [k for k in required if not sig.get(k)]
    if missing:
        log_event(sig.get("id"), "error", None, None,
                  details={"msg": f"missing_fields: {missing}"})
        return

    sid = sig["id"]
    exchange = (sig.get("exchange") or "binance").lower()
    symbol   = sig["symbol"]
    dire     = sig["direction"]
    status   = sig["status"]
    created  = sig["created_at"]

    entry = float(sig["entry"])
    sl_db = float(sig["stop_loss"])
    tp1   = float(sig["tp1"])
    tp2   = float(sig["tp2"])
    tp3   = float(sig["tp3"])

    stop_initial   = float(sig.get("stop_loss_initial") or sl_db)
    entered_at     = sig.get("entered_at")
    partial_real   = float(sig.get("partial_realized") or 0)
    partial_profit = float(sig.get("partial_profit")  or 0)

    exit_at: Optional[str] = None
    exit_level: Optional[str] = None

    mfe_best = sig.get("mfe")
    mae_worst = sig.get("mae")

    start_iso = created
    end_iso   = dt_to_iso(now_utc())

    candles = fetch_candles_1m(exchange, symbol, start_iso, end_iso)
    if not candles:
        return

    sl_working = sl_db

    for c in candles:
        hi = float(c["high"])
        lo = float(c["low"])
        ts = c["ts"]

        # ENTRADA
        if not entered_at:
            if lo <= entry <= hi:
                entered_at = ts
                log_event(sid, "entered", entry, ts, details={"policy": POLICY_VERSION})
            else:
                continue

        # SL depois de TP1/TP2
        if status == "tp1":
            sl_working = entry
        elif status == "tp2":
            sl_working = tp1
        else:
            sl_working = sl_db

        # -------------------------------------
        #               BUY
        # -------------------------------------
        if dire == "BUY":
            hit_sl = (lo <= sl_working)
            hit_tp1 = (hi >= tp1)
            hit_tp2 = (hi >= tp2)
            hit_tp3 = (hi >= tp3)

            if hit_sl:
                prev = status
                frac_rem = max(0, 1 - partial_real)

                if prev == "tp1":
                    exit_level = "be"
                    exit_at = ts
                    status = "close"
                    log_event(sid, "closed", entry, ts,
                              details={"reason":"breakeven_after_partial","exit_level":"be","policy":POLICY_VERSION})

                elif prev == "tp2":
                    exit_level = "sl_trailing"
                    exit_at = ts
                    r_extra = r_at(sl_working, entry, stop_initial, "BUY") * frac_rem
                    partial_profit += r_extra
                    partial_real = 1
                    status = "close"
                    log_event(sid, "closed", sl_working, ts,
                              details={"reason":"trailing_stop_after_tp2","exit_level":"sl_trailing",
                                       "policy":POLICY_VERSION,"fraction":frac_rem,"r_event":r_extra})

                else:
                    exit_level = "sl"
                    exit_at = ts
                    status = "close"
                    log_event(sid,"hit_sl", sl_working, ts, details={"policy":POLICY_VERSION})

                break

            # TPs
            if status=="open" and hit_tp1:
                status="tp1"
                partial_real += TP1_FRAC
                pr = r_at(tp1, entry, stop_initial, "BUY") * TP1_FRAC
                partial_profit += pr
                log_event(sid,"hit_tp1",tp1,ts,details={"policy":POLICY_VERSION,"fraction":TP1_FRAC,"r_event":pr})

            if status in ("tp1","open") and hit_tp2:
                status="tp2"
                partial_real += TP2_FRAC
                pr = r_at(tp2, entry, stop_initial, "BUY") * TP2_FRAC
                partial_profit += pr
                log_event(sid,"hit_tp2",tp2,ts,details={"policy":POLICY_VERSION,"fraction":TP2_FRAC,"r_event":pr})

            if status in ("tp2","tp1","open") and hit_tp3:
                status="tp3"
                exit_level="tp3"
                exit_at=ts
                frac_rem=max(0,1-partial_real)
                pr = r_at(tp3, entry, stop_initial, "BUY") * frac_rem
                partial_profit += pr
                partial_real=1
                log_event(sid,"hit_tp3",tp3,ts,details={"policy":POLICY_VERSION,"fraction":frac_rem,"r_event":pr})
                break

            mfe,mae = compute_mfe_mae("BUY",entry,hi,lo)


        # -------------------------------------
        #               SELL
        # -------------------------------------
        else:
            hit_sl = (hi >= sl_working)
            hit_tp1 = (lo <= tp1)
            hit_tp2 = (lo <= tp2)
            hit_tp3 = (lo <= tp3)

            if hit_sl:
                prev=status
                frac_rem=max(0,1-partial_real)

                if prev=="tp1":
                    exit_level="be"
                    exit_at=ts
                    status="close"
                    log_event(sid,"closed",entry,ts,
                              details={"reason":"breakeven_after_partial","exit_level":"be","policy":POLICY_VERSION})

                elif prev=="tp2":
                    exit_level="sl_trailing"
                    exit_at=ts
                    r_extra = r_at(sl_working, entry, stop_initial, "SELL") * frac_rem
                    partial_profit += r_extra
                    partial_real = 1
                    status="close"
                    log_event(sid,"closed",sl_working,ts,
                              details={"reason":"trailing_stop_after_tp2","exit_level":"sl_trailing",
                                       "policy":POLICY_VERSION,"fraction":frac_rem,"r_event":r_extra})

                else:
                    exit_level="sl"
                    exit_at=ts
                    status="close"
                    log_event(sid,"hit_sl",sl_working,ts,details={"policy":POLICY_VERSION})

                break

            if status=="open" and hit_tp1:
                status="tp1"
                partial_real+=TP1_FRAC
                pr = r_at(tp1,entry,stop_initial,"SELL")*TP1_FRAC
                partial_profit+=pr
                log_event(sid,"hit_tp1",tp1,ts,details={"policy":POLICY_VERSION,"fraction":TP1_FRAC,"r_event":pr})

            if status in ("tp1","open") and hit_tp2:
                status="tp2"
                partial_real+=TP2_FRAC
                pr = r_at(tp2,entry,stop_initial,"SELL")*TP2_FRAC
                partial_profit+=pr
                log_event(sid,"hit_tp2",tp2,ts,details={"policy":POLICY_VERSION,"fraction":TP2_FRAC,"r_event":pr})

            if status in ("tp2","tp1","open") and hit_tp3:
                status="tp3"
                exit_level="tp3"
                exit_at=ts
                frac_rem=max(0,1-partial_real)
                pr = r_at(tp3,entry,stop_initial,"SELL")*frac_rem
                partial_profit+=pr
                partial_real=1
                log_event(sid,"hit_tp3",tp3,ts,details={"policy":POLICY_VERSION,"fraction":frac_rem,"r_event":pr})
                break

            mfe,mae = compute_mfe_mae("SELL",entry,hi,lo)


        if mfe_best is None or mfe > mfe_best:  mfe_best = mfe
        if mae_worst is None or mae < mae_worst: mae_worst = mae

    # -------------------------------------------------------------------
    # FECHO FINAL
    # -------------------------------------------------------------------
    r_mult=None
    profit_pct=None

    if exit_at:
        r_mult = partial_profit
        risk_pct = abs(stop_initial - entry)/entry*100
        profit_pct = r_mult * risk_pct

    update = {
        "id": sid,
        "policy_version": POLICY_VERSION,
        "mfe": mfe_best,
        "mae": mae_worst,
        "status": status,
        "partial_realized": partial_real,
        "partial_profit": partial_profit
    }

    if entered_at:
        update["entered_at"] = entered_at

    # SL final armazenado
    new_stop=None
    if status=="tp1":
        new_stop=entry
    elif status=="tp2":
        new_stop=tp1

    if exit_at:
        if exit_level=="be":
            new_stop=entry
        elif exit_level=="sl_trailing":
            new_stop=tp1

    if new_stop is not None and new_stop != sl_db:
        update["stop_loss"] = new_stop

    if exit_at:
        update["exit_at"] = exit_at
        update["exit_level"] = exit_level
        update["r_multiple"] = r_mult
        update["finalized"] = True
        update["profit_pct"] = profit_pct

    supabase.table(SIGNALS_TABLE).update(update).eq("id", sid).execute()


# -------------------------------------------------------------------
# LOOP PRINCIPAL
# -------------------------------------------------------------------
def tick_once():
    signals = fetch_open_signals(limit=BATCH_SIZE)
    if not signals:
        print(f"{LOG} sem sinais para avaliar.")
        return

    ids_to_fix = [s["id"] for s in signals if not s.get("stop_loss_initial")]
    if ids_to_fix:
        for sid in ids_to_fix:
            s = next(x for x in signals if x["id"] == sid)
            supabase.table(SIGNALS_TABLE).update(
                {"stop_loss_initial": s["stop_loss"]}
            ).eq("id", sid).execute()

    for s in signals:
        try:
            apply_state_machine(s)
        except Exception as e:
            supabase.table(EVENTS_TABLE).insert({
                "signal_id": s["id"],
                "event": "error",
                "details": {"msg": str(e)}
            }).execute()

def main():
    if LOOP_SECONDS <= 0:
        tick_once()
        return
    print(f"{LOG} iniciado. loop={LOOP_SECONDS}s policy={POLICY_VERSION}")
    while True:
        try:
            tick_once()
        except Exception as e:
            print(f"{LOG} erro ciclo: {e}")
        time.sleep(LOOP_SECONDS)

if __name__ == "__main__":
    main()

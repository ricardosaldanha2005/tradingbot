#!/usr/bin/env python3
# evaluate_signals.py
#
# Avalia sinais usando candles 1m (raw_candles.ts) e atualiza a tabela signals + signal_events.
#
# ENV necessário (.env no mesmo diretório ou variáveis exportadas):
#   SUPABASE_URL=...
#   SUPABASE_SERVICE_ROLE=...
#   RAW_TABLE=raw_candles
#   SIGNALS_TABLE=signals
#   EVENTS_TABLE=signal_events
#   POLICY_VERSION=v1_conservative_SL_priority
#   LOOP_SECONDS=30           # opcional: se >0 corre em loop; se =0 corre uma vez e sai
#   BATCH_SIZE=200            # opcional: nº máx. de sinais por ciclo

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

# Frações de posição em cada TP (somam 1.0)
TP1_FRAC = 0.33
TP2_FRAC = 0.33
TP3_FRAC = 0.34


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)

def iso_to_dt(s: str) -> dt.datetime:
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return dt.datetime.fromisoformat(s)

def dt_to_iso(d: dt.datetime) -> str:
    if d.tzinfo is None:
        d = d.replace(tzinfo=dt.timezone.utc)
    return d.astimezone(dt.timezone.utc).isoformat()

def fetch_open_signals(limit: int = 500) -> List[Dict[str, Any]]:
    """
    Sinais candidatos:
    - status em ('open','tp1','tp2')  (ainda em curso)
    - ou entered_at IS NULL & status='open' (ainda sem entrada)
    """
    # 1) estados em curso
    q1 = (supabase.table(SIGNALS_TABLE)
          .select("*")
          .in_("status", ["open", "tp1", "tp2"])
          .order("created_at", desc=True)
          .limit(limit)
          .execute().data or [])
    ids = {x["id"] for x in q1}
    # 2) ainda sem entrada (garante que não duplicamos)
    q2 = (supabase.table(SIGNALS_TABLE)
          .select("*")
          .eq("status", "open")
          .is_("entered_at", "null")
          .order("created_at", desc=True)
          .limit(limit)
          .execute().data or [])
    out = q1[:]
    for x in q2:
        if x["id"] not in ids:
            out.append(x)
    return out[:limit]

def fetch_candles_1m(exchange: str, symbol: str, start_iso: str, end_iso: str) -> List[Dict[str, Any]]:
    """
    Vai buscar candles 1m de raw_candles (coluna ts).
    """
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
    supabase.table(EVENTS_TABLE).insert({
        "signal_id": signal_id,
        "event": event,
        "price": price,
        "candle_start": candle_ts,
        "candle_timeframe": "1m",
        "src_timeframe": src,
        "details": details or {}
    }).execute()

def compute_mfe_mae(dire: str, entry: float, hi: float, lo: float) -> Tuple[float, float]:
    if dire == "BUY":
        mfe = (hi - entry) / entry * 100.0
        mae = (lo - entry) / entry * 100.0
    else:  # SELL
        mfe = (entry - lo) / entry * 100.0
        mae = (entry - hi) / entry * 100.0
    return mfe, mae

def r_at(price: float, entry: float, stop_initial: float, dire: str) -> float:
    """Retorna o R (reward/risk) no 'price' vs entry & stop_initial."""
    risk = abs(entry - stop_initial)
    if risk <= 0:
        return 0.0
    if dire == "BUY":
        return (price - entry) / risk
    else:
        return (entry - price) / risk

def apply_state_machine(sig: Dict[str, Any]) -> None:
    """
    Avalia 1 sinal: avança estados, fecha quando SL/TP3, atualiza métricas e escreve eventos.
    Política v1: prioridade SL/BE antes de TPs no mesmo minuto (conservador).
    """

    # ---- Sanity check: campos obrigatórios ----
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
    dire     = sig["direction"]   # 'BUY' | 'SELL'
    status   = sig["status"]      # 'open' | 'tp1' | 'tp2' | 'tp3' | 'sl' | 'close' | 'skip'
    created  = sig["created_at"]

    entry = float(sig["entry"])
    sl_db = float(sig["stop_loss"])
    tp1   = float(sig["tp1"])
    tp2   = float(sig["tp2"])
    tp3   = float(sig["tp3"])

    stop_initial   = float(sig.get("stop_loss_initial") or sl_db)
    entered_at     = sig.get("entered_at")
    partial_real   = float(sig.get("partial_realized") or 0.0)
    partial_profit = float(sig.get("partial_profit")  or 0.0)

    exit_at: Optional[str] = None
    exit_level: Optional[str] = None

    mfe_best: Optional[float] = sig.get("mfe")
    mae_worst: Optional[float] = sig.get("mae")

    # janela: de created_at até agora
    start_iso = created
    end_iso   = dt_to_iso(now_utc())

    candles = fetch_candles_1m(exchange, symbol, start_iso, end_iso)
    if not candles:
        return

    # trabalhamos com uma cópia de SL (que pode mover para BE após TP1)
    sl_working = sl_db

    for c in candles:
        hi = float(c["high"])
        lo = float(c["low"])
        ts = c["ts"]

        # 1) Entrada (ainda não entrou)
        if not entered_at:
            touched = (lo <= entry <= hi)
            if touched:
                entered_at = ts
                log_event(sid, "entered", entry, ts, details={"policy": POLICY_VERSION})
            else:
                # ainda não entrou, segue p/ próximo minuto
                continue

        # 2) Posição ativa: prioridade SL/BE antes de TPs no mesmo minuto (conservador)
        #    Regras de BE:
        #    - Após TP1: move SL para entry (break-even do remanescente)
        #    - Após TP2: mantém SL no entry (remanescente protegido)
        if status in ("tp1", "tp2"):
            sl_working = entry  # break-even do remanescente

        if dire == "BUY":
            hit_sl_or_be = (lo <= sl_working)
            hit_tp3 = (hi >= tp3)
            hit_tp2 = (hi >= tp2)
            hit_tp1 = (hi >= tp1)

            # SL/BE tem prioridade no mesmo minuto
            if hit_sl_or_be:
                if status in ("tp1", "tp2"):
                    # Fecho do remanescente a break-even (após parciais): lucro total = só parciais
                    exit_level = "be"
                    exit_at = ts
                    status = "close"
                    log_event(
                        sid, "closed", entry, ts,
                        details={
                            "reason": "breakeven_after_partial",
                            "exit_level": "be",
                            "policy": POLICY_VERSION
                        }
                    )
                else:
                    status = "sl"
                    exit_level = "sl"
                    exit_at = ts
                    log_event(sid, "hit_sl", sl_working, ts, details={"policy": POLICY_VERSION})
                break

            # TPs (podem realizar parciais)
            if status in ("open",) and hit_tp1:
                status = "tp1"
                partial_real += TP1_FRAC
                pr = r_at(tp1, entry, stop_initial, dire) * TP1_FRAC
                partial_profit += pr
                log_event(sid, "hit_tp1", tp1, ts,
                          details={"policy": POLICY_VERSION, "fraction": TP1_FRAC, "r_event": pr})

            if status in ("tp1", "open") and hit_tp2:
                status = "tp2"
                partial_real += TP2_FRAC
                pr = r_at(tp2, entry, stop_initial, dire) * TP2_FRAC
                partial_profit += pr
                log_event(sid, "hit_tp2", tp2, ts,
                          details={"policy": POLICY_VERSION, "fraction": TP2_FRAC, "r_event": pr})

            if status in ("tp2", "tp1", "open") and hit_tp3:
                status = "tp3"
                exit_level = "tp3"
                exit_at = ts
                frac_rem = max(0.0, 1.0 - partial_real)
                pr = r_at(tp3, entry, stop_initial, dire) * frac_rem
                partial_profit += pr
                partial_real = 1.0
                log_event(sid, "hit_tp3", tp3, ts,
                          details={"policy": POLICY_VERSION, "fraction": frac_rem, "r_event": pr})
                break

            mfe, mae = compute_mfe_mae(dire, entry, hi, lo)

        else:  # SELL
            hit_sl_or_be = (hi >= sl_working)
            hit_tp3 = (lo <= tp3)
            hit_tp2 = (lo <= tp2)
            hit_tp1 = (lo <= tp1)

            if hit_sl_or_be:
                if status in ("tp1", "tp2"):
                    exit_level = "be"
                    exit_at = ts
                    status = "close"
                    log_event(
                        sid, "closed", entry, ts,
                        details={
                            "reason": "breakeven_after_partial",
                            "exit_level": "be",
                            "policy": POLICY_VERSION
                        }
                    )
                else:
                    status = "sl"
                    exit_level = "sl"
                    exit_at = ts
                    log_event(sid, "hit_sl", sl_working, ts, details={"policy": POLICY_VERSION})
                break

            if status in ("open",) and hit_tp1:
                status = "tp1"
                partial_real += TP1_FRAC
                pr = r_at(tp1, entry, stop_initial, dire) * TP1_FRAC
                partial_profit += pr
                log_event(sid, "hit_tp1", tp1, ts,
                          details={"policy": POLICY_VERSION, "fraction": TP1_FRAC, "r_event": pr})

            if status in ("tp1", "open") and hit_tp2:
                status = "tp2"
                partial_real += TP2_FRAC
                pr = r_at(tp2, entry, stop_initial, dire) * TP2_FRAC
                partial_profit += pr
                log_event(sid, "hit_tp2", tp2, ts,
                          details={"policy": POLICY_VERSION, "fraction": TP2_FRAC, "r_event": pr})

            if status in ("tp2", "tp1", "open") and hit_tp3:
                status = "tp3"
                exit_level = "tp3"
                exit_at = ts
                frac_rem = max(0.0, 1.0 - partial_real)
                pr = r_at(tp3, entry, stop_initial, dire) * frac_rem
                partial_profit += pr
                partial_real = 1.0
                log_event(sid, "hit_tp3", tp3, ts,
                          details={"policy": POLICY_VERSION, "fraction": frac_rem, "r_event": pr})
                break

            mfe, mae = compute_mfe_mae(dire, entry, hi, lo)

        # atualiza MFE/MAE acumulados
        if mfe_best is None or mfe > mfe_best:
            mfe_best = mfe
        if mae_worst is None or mae < mae_worst:
            mae_worst = mae

    # 3) Se fechou, calcula R final (já temos partial_profit acumulado)
    r_mult: Optional[float] = None
    if exit_at:
        if exit_level == "sl":
            r_mult = r_at(sl_working, entry, stop_initial, dire)
        elif exit_level == "tp3":
            r_mult = partial_profit
        elif exit_level == "be":
            r_mult = partial_profit
        else:
            r_mult = partial_profit

    # 4) Persistir alterações ao sinal
    update: Dict[str, Any] = {
        "id": sid,
        "policy_version": POLICY_VERSION,
        "mfe": mfe_best,
        "mae": mae_worst,
        "status": status,
        "partial_realized": partial_real,
        "partial_profit": partial_profit,
    }
    if entered_at:
        update["entered_at"] = entered_at
    # persistir SL movido para BE após TP1/TP2
    if status in ("tp1", "tp2") and sl_db != entry:
        update["stop_loss"] = entry
    if exit_at:
        update["exit_at"] = exit_at
        update["exit_level"] = exit_level
        update["r_multiple"] = r_mult

    supabase.table(SIGNALS_TABLE).update(update).eq("id", sid).execute()


def tick_once():
    signals = fetch_open_signals(limit=BATCH_SIZE)
    if not signals:
        print(f"{LOG} sem sinais para avaliar.")
        return
    # garante stop_loss_initial
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

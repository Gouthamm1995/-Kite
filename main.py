import asyncio
import csv
import datetime
import json
import math
import os
import multiprocessing as mp
import logging
import threading
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from zoneinfo import ZoneInfo

import redis
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from kiteconnect import KiteConnect, KiteTicker
import uvicorn


# =========================
# TIMEZONE (India / Kolkata)
# =========================
IST = ZoneInfo("Asia/Kolkata")


# =========================
# PATHS + LOGGING
# =========================
BASE_DIR = Path(__file__).resolve().parent

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper().strip()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(processName)s %(levelname)s %(message)s",
)
log = logging.getLogger("kitealgo")


# =========================
# CONFIG
# =========================
API_KEY = os.environ.get("KITE_API_KEY", "eeo1b4qfvxqt7spz")
API_SECRET = os.environ.get("KITE_API_SECRET", "cq7z4ycp4ccezf4k9os2h0i24ba1hh0j")
REDIRECT_URL = os.environ.get("KITE_REDIRECT_URL", "http://127.0.0.1:8000/zerodha/callback")

WORKERS = int(os.environ.get("WORKERS", "6"))
MP_QUEUE_MAX = int(os.environ.get("MP_QUEUE_MAX", "20000"))

# ---- helpers (env parsing) ----
def _parse_hhmm(value: Optional[str], default: datetime.time) -> datetime.time:
    if not value:
        return default
    s = str(value).strip()
    try:
        hh, mm = s.split(":", 1)
        return datetime.time(int(hh), int(mm))
    except Exception:
        return default

# Strategy
NO_NEW_TRADES_AFTER = _parse_hhmm(os.environ.get("NO_NEW_TRADES_AFTER"), datetime.time(9, 45))  # 09:30 AM IST default
RISK_PER_TRADE = float(os.environ.get("RISK_PER_TRADE", "50"))
BREAKOUT_VALUE_MIN = float(os.environ.get("BREAKOUT_VALUE_MIN", "10000000"))  # 1.0 cr
PRODUCT = os.environ.get("PRODUCT", "MIS").upper().strip()
EXCHANGE = os.environ.get("EXCHANGE", "NSE").upper().strip()
BREAKOUT_MODE = os.environ.get("BREAKOUT_MODE", "FIRST_CANDLE").upper().strip()  # FIRST_CANDLE | DAY_HIGH
OPENING_PATTERN_MODE = os.environ.get("OPENING_PATTERN_MODE", "LEGACY").upper().strip()  # NONE | LEGACY
PENDING_TRIGGER_TIMEOUT_S = int(os.environ.get("PENDING_TRIGGER_TIMEOUT_S", "1800"))  # 30 min

MIN_ENTRY_PRICE = float(os.environ.get("MIN_ENTRY_PRICE", "100"))
MAX_ENTRY_PRICE = float(os.environ.get("MAX_ENTRY_PRICE", "5000"))
MAX_TRADES = int(os.environ.get("MAX_TRADES", "6"))  # ✅ max trades = 6

# ✅ Daily max profit (squareoff all)
DAILY_MAX_PROFIT = float(os.environ.get("DAILY_MAX_PROFIT", "750"))

# ✅ NEW: SL rule
SL_PCT_BELOW_ENTRY = float(os.environ.get("SL_PCT_BELOW_ENTRY", "0.008"))  # 0.8%
TARGET_PCT_ABOVE_ENTRY = float(os.environ.get("TARGET_PCT_ABOVE_ENTRY", "0.032"))  # 3.2%
TRAIL_STEP_PCT = float(os.environ.get("TRAIL_STEP_PCT", "0.008"))  # 0.8% steps

# Redis
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", "6379"))
ACCESS_TOKEN_KEY = "access_token"

# Persisted for the day (until next access_token is generated)
DAY_KEY = "day_key"
POSITIONS_SNAPSHOT_KEY = "positions_snapshot_json"
POSITIONS_SNAPSHOT_TS_KEY = "positions_snapshot_ts"

# LTP storage (hash; O(1) update per tick)
LTP_HASH_KEY = "ltp_map"
LTP_MAP_TS_KEY = "ltp_map_ts"
TRADES_DONE_KEY = "trades_done"

# ✅ Daily profit squareoff keys
SQUAREOFF_DONE_KEY = "squareoff_done"
TRADING_DISABLED_KEY = "trading_disabled"

# Per-symbol keys
def k_in_trade(sym): return f"in_trade:{sym}"
def k_entry(sym): return f"entry_price:{sym}"
def k_sl(sym): return f"sl:{sym}"
def k_target(sym): return f"target:{sym}"
def k_qty(sym): return f"qty:{sym}"
def k_sl_oid(sym): return f"sl_order_id:{sym}"
def k_tgt_oid(sym): return f"tgt_order_id:{sym}"
# --- NEW: Pyramiding + Trailing management ---
def k_base_entry(sym): return f"base_entry:{sym}"    # original entry reference (never changes)
def k_base_qty(sym): return f"base_qty:{sym}"        # initial qty (used for add-buys)
def k_step(sym): return f"trail_step:{sym}"          # current step number (0 → 4)


# =========================
# GLOBAL STATE (for heartbeat)
# =========================
_tick_lock = threading.Lock()
_ticks_total = 0
_last_tick_ts = 0
_last_tick_token = 0
_last_tick_price = 0.0

_ws_connected = False
_ws_connected_ts = 0
_ws_last_event_ts = 0
_ws_last_error = ""
_last_ltp_ts_sec = 0
_ltp_cache_lock = threading.Lock()
_ltp_cache: Dict[str, float] = {}


# =========================
# REDIS
# =========================
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

def redis_ok() -> bool:
    try:
        r.ping()
        return True
    except Exception:
        return False


def ltp_flush_loop():
    """
    Ultra-low-latency tick path: collect LTP updates in-memory and flush to Redis in batches.
    """
    interval_ms = int(os.environ.get("LTP_FLUSH_INTERVAL_MS", "200"))
    interval_s = max(0.05, float(interval_ms) / 1000.0)

    global _last_ltp_ts_sec
    while True:
        time.sleep(interval_s)

        with _ltp_cache_lock:
            if not _ltp_cache:
                continue
            batch = dict(_ltp_cache)
            _ltp_cache.clear()

        try:
            r.hset(LTP_HASH_KEY, mapping=batch)
            now = int(time.time())
            if now != _last_ltp_ts_sec:
                _last_ltp_ts_sec = now
                r.set(LTP_MAP_TS_KEY, str(now))
        except Exception:
            pass


# =========================
# FASTAPI + KITE
# =========================
app = FastAPI()
kite = KiteConnect(api_key=API_KEY)
kite.redirect_url = REDIRECT_URL


def ensure_kite_token_global() -> bool:
    if not redis_ok():
        return False
    at = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not at:
        return False
    kite.set_access_token(at)
    return True


# =========================
# LOAD UNIVERSE (allowed_stocks.json [+ optional derivative filter])
# =========================
ALLOWED_STOCKS_PATH = Path(os.environ.get("ALLOWED_STOCKS_PATH", str(BASE_DIR / "allowed_stocks.json")))
DERIVATIVE_STOCKS_PATH = Path(os.environ.get("DERIVATIVE_STOCKS_PATH", str(BASE_DIR / "derivative_stocks.txt")))
UNIVERSE_MODE = os.environ.get("UNIVERSE_MODE", "ALL").upper().strip()  # ALL | DERIVATIVES

with open(ALLOWED_STOCKS_PATH, "r", encoding="utf-8") as f:
    allowed_data = json.load(f)

if isinstance(allowed_data, list):
    allowed_stocks: Dict[str, int] = {
        item["symbol"].upper(): int(item["token"])
        for item in allowed_data
        if isinstance(item, dict) and "symbol" in item and "token" in item
    }
elif isinstance(allowed_data, dict):
    allowed_stocks = {k.upper(): int(v) for k, v in allowed_data.items()}
else:
    raise ValueError("allowed_stocks.json format not supported")

if UNIVERSE_MODE in ("DERIVATIVES", "FNO"):
    try:
        deriv = set()
        with open(DERIVATIVE_STOCKS_PATH, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip().upper()
                if s:
                    deriv.add(s)
        before = len(allowed_stocks)
        allowed_stocks = {sym: tok for sym, tok in allowed_stocks.items() if sym in deriv}
        log.info("Universe filtered: mode=%s before=%s after=%s", UNIVERSE_MODE, before, len(allowed_stocks))
    except FileNotFoundError:
        log.warning("Derivative list not found at %s; using full universe", DERIVATIVE_STOCKS_PATH)
    except Exception as e:
        log.warning("Derivative filter failed (%s); using full universe", e)

token_to_symbol = {v: k for k, v in allowed_stocks.items()}


# =========================
# INSTRUMENT META (tick size) for order price rounding
# =========================
INSTRUMENTS_CSV_PATH = Path(os.environ.get("INSTRUMENTS_CSV_PATH", str(BASE_DIR / "kite_instruments.csv")))
TICK_SIZE_DEFAULT = float(os.environ.get("TICK_SIZE_DEFAULT", "0.05"))
tick_size_by_symbol: Dict[str, float] = {}

try:
    if INSTRUMENTS_CSV_PATH.exists():
        allowed_tokens = set(int(t) for t in allowed_stocks.values())
        with open(INSTRUMENTS_CSV_PATH, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    tok = int(row.get("instrument_token") or 0)
                except Exception:
                    continue
                if tok not in allowed_tokens:
                    continue
                sym = token_to_symbol.get(tok) or str(row.get("tradingsymbol", "")).upper()
                try:
                    ts = float(row.get("tick_size") or 0.0)
                except Exception:
                    ts = 0.0
                if ts and ts > 0:
                    tick_size_by_symbol[sym] = ts
        log.info("Loaded tick sizes: %s symbols", len(tick_size_by_symbol))
except Exception as e:
    log.warning("Tick size load failed (%s); using default %s", e, TICK_SIZE_DEFAULT)


def tick_size(sym: str) -> float:
    try:
        return float(tick_size_by_symbol.get(str(sym).upper(), TICK_SIZE_DEFAULT) or TICK_SIZE_DEFAULT)
    except Exception:
        return TICK_SIZE_DEFAULT


def floor_to_tick(price: float, tick: float) -> float:
    try:
        t = float(tick) if float(tick) > 0 else TICK_SIZE_DEFAULT
        v = math.floor(float(price) / t) * t
        return round(float(v), 2)
    except Exception:
        return float(price)


def ceil_to_tick(price: float, tick: float) -> float:
    try:
        t = float(tick) if float(tick) > 0 else TICK_SIZE_DEFAULT
        v = math.ceil(float(price) / t) * t
        return round(float(v), 2)
    except Exception:
        return float(price)


# =========================
# MULTIPROCESSING ROUTING
# =========================
TOKENS_SORTED = sorted([int(t) for t in allowed_stocks.values()])
TOKEN_TO_WORKER = {tok: (i % WORKERS) for i, tok in enumerate(TOKENS_SORTED)}
_worker_token_counts = [0] * WORKERS
for tok, wid in TOKEN_TO_WORKER.items():
    _worker_token_counts[int(wid)] += 1

_worker_procs: List[mp.Process] = []
_worker_queues: List[Any] = []
_workers_started = False


def _start_workers_if_needed():
    global _workers_started, _worker_procs, _worker_queues
    if _workers_started:
        return

    ctx = mp.get_context("spawn")
    _worker_queues = []
    _worker_procs = []

    for i in range(WORKERS):
        q = ctx.Queue(maxsize=MP_QUEUE_MAX)
        p = ctx.Process(target=worker_main, args=(i, q), daemon=True)
        p.start()
        _worker_queues.append(q)
        _worker_procs.append(p)

    _workers_started = True
    log.info("Started %s worker processes", WORKERS)
    log.info("Token distribution: %s", _worker_token_counts)


def _route_tick_to_worker(tick: dict):
    global _ticks_total, _last_tick_ts, _last_tick_token, _last_tick_price, _ws_last_event_ts

    token = tick.get("instrument_token")
    if token is None:
        return

    wid = TOKEN_TO_WORKER.get(int(token), 0)

    lp = tick.get("last_price")
    now = int(time.time())

    with _tick_lock:
        _ticks_total += 1
        _last_tick_ts = now
        _last_tick_token = int(token)
        _last_tick_price = float(lp) if lp is not None else 0.0
        _ws_last_event_ts = now

    # store LTP for UI (in-memory; flushed to Redis in batches by ltp_flush_loop)
    if lp is not None:
        try:
            with _ltp_cache_lock:
                _ltp_cache[str(int(token))] = float(lp)
        except Exception:
            pass

    try:
        _worker_queues[int(wid)].put_nowait(tick)
    except Exception:
        pass


# =========================
# STRATEGY HELPERS
# =========================
def breakout_value_ok(close_px: float, vol_1m: float) -> Tuple[bool, float]:
    try:
        val = float(close_px) * float(vol_1m)
        return (val >= float(BREAKOUT_VALUE_MIN)), val
    except Exception:
        return False, 0.0


def risk_qty(entry: float, sl: float, risk: float) -> int:
    diff = float(entry) - float(sl)
    if diff <= 0:
        return 0
    qty = int(float(risk) / diff)
    return max(qty, 0)


def to_ist(dt: datetime.datetime) -> datetime.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=IST)
    return dt.astimezone(IST)


def within_new_trade_window(ts: datetime.datetime) -> bool:
    # ✅ stop all new trades if daily profit squareoff has happened
    try:
        if redis_ok() and (r.get(TRADING_DISABLED_KEY) or "").strip() == "1":
            return False
    except Exception:
        pass
    return to_ist(ts).time() <= NO_NEW_TRADES_AFTER


# =========================
# ✅ DAILY MAX PROFIT WATCHER (SQUAREOFF ALL)
# =========================
def squareoff_all_positions_if_profit_hit():
    """
    If total P&L >= DAILY_MAX_PROFIT:
      - cancel SL/Target orders (best effort)
      - squareoff all open positions at market
      - disable new trades for the day
      - run only once (Redis lock)
    """
    while True:
        try:
            if not redis_ok():
                time.sleep(1)
                continue

            # already executed?
            if (r.get(SQUAREOFF_DONE_KEY) or "").strip() == "1":
                time.sleep(2)
                continue

            if not ensure_kite_token_global():
                time.sleep(1)
                continue

            pos = kite.positions()
            net = pos.get("net", []) or []

            total_pnl = 0.0
            open_rows = []
            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper().strip()
                if not sym:
                    continue
                avg = float(p.get("average_price") or 0.0)
                ltp = float(p.get("last_price") or 0.0)
                unreal = (ltp - avg) * qty
                realised = float(p.get("realised") or 0.0)
                total_pnl += float(unreal) + float(realised)
                open_rows.append((sym, qty))

            if total_pnl >= float(DAILY_MAX_PROFIT) and open_rows:
                # lock once
                r.set(SQUAREOFF_DONE_KEY, "1")
                r.set(TRADING_DISABLED_KEY, "1")

                log.info("✅ DAILY PROFIT HIT %.2f >= %.2f. SQUAREOFF START.", total_pnl, float(DAILY_MAX_PROFIT))

                # cancel exit orders + squareoff all
                for sym, qty in open_rows:
                    try:
                        # cancel SL/TGT from redis if present (best effort)
                        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
                        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()
                        if sl_oid:
                            try:
                                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
                            except Exception:
                                pass
                        if tgt_oid:
                            try:
                                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
                            except Exception:
                                pass

                        txn = kite.TRANSACTION_TYPE_SELL if qty > 0 else kite.TRANSACTION_TYPE_BUY
                        kite.place_order(
                            variety=kite.VARIETY_REGULAR,
                            exchange=EXCHANGE,
                            tradingsymbol=sym,
                            transaction_type=txn,
                            quantity=abs(int(qty)),
                            product=PRODUCT,
                            order_type=kite.ORDER_TYPE_MARKET,
                        )

                        # clear local state keys (best effort)
                        r.delete(k_in_trade(sym))
                        r.delete(k_entry(sym))
                        r.delete(k_sl(sym))
                        r.delete(k_target(sym))
                        r.delete(k_qty(sym))
                        r.delete(k_sl_oid(sym))
                        r.delete(k_tgt_oid(sym))
                        r.delete(k_base_entry(sym))
                        r.delete(k_base_qty(sym))
                        r.delete(k_step(sym))

                    except Exception:
                        pass

                log.info("✅ SQUAREOFF DONE. Trading disabled for the day.")

        except Exception:
            pass

        time.sleep(1)


# =========================
# WORKER PROCESS
# =========================
def worker_main(worker_id: int, q: mp.Queue):
    r_local = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)

    kite_local = KiteConnect(api_key=API_KEY)
    kite_local.redirect_url = REDIRECT_URL

    TOKEN_REFRESH_INTERVAL_S = float(os.environ.get("TOKEN_REFRESH_INTERVAL_S", "5"))
    _last_token_refresh = 0.0
    _last_access_token = ""

    def refresh_token(force: bool = False):
        nonlocal _last_token_refresh, _last_access_token
        now = time.time()
        if (not force) and (now - _last_token_refresh) < TOKEN_REFRESH_INTERVAL_S:
            return
        _last_token_refresh = now
        try:
            at = (r_local.get(ACCESS_TOKEN_KEY) or "").strip()
        except Exception:
            return
        if at and at != _last_access_token:
            try:
                kite_local.set_access_token(at)
                _last_access_token = at
            except Exception:
                pass

    def _diag_key(sym: str) -> str:
        return f"diag:{sym}"

    def diag_set(sym: str, **fields: Any):
        try:
            mapping = {k: (json.dumps(v) if isinstance(v, (dict, list)) else str(v)) for k, v in fields.items()}
            r_local.hset(_diag_key(sym), mapping=mapping)
            r_local.expire(_diag_key(sym), 60 * 60 * 16)  # trading-day TTL
        except Exception:
            pass

    def wait_for_complete_and_avg(order_id: str, timeout_s: int = 10) -> Tuple[bool, float]:
        t0 = time.time()
        last_avg = 0.0
        while time.time() - t0 < timeout_s:
            try:
                hist = kite_local.order_history(order_id)
                if hist:
                    last = hist[-1]
                    status = str(last.get("status", "")).upper()
                    avg = float(last.get("average_price") or 0.0)
                    if avg > 0:
                        last_avg = avg
                    if status == "COMPLETE":
                        return True, float(avg or last_avg or 0.0)
                    if status in ("REJECTED", "CANCELLED"):
                        return False, 0.0
            except Exception:
                pass
            time.sleep(0.4)
        return False, 0.0

    # ==========================================================
    # ✅ TRAILING + PYRAMIDING MANAGER (TICK-LEVEL)
    # ==========================================================
    def manage_trailing_and_pyramiding(sym: str, ltp: float):
        """
        Steps:
          step 0: entered. SL = entry*(1-0.8%), target = entry*(1+3.2%)
          when ltp >= entry*(1+0.8%): step->1, BUY add, SL->entry
          when ltp >= entry*(1+1.6%): step->2, BUY add, SL->entry*(1+0.8%)
          when ltp >= entry*(1+2.4%): step->3, BUY add, SL->entry*(1+1.6%)
          step 4 corresponds to 3.2% target level (we do NOT add buy at step 4)
        """
        try:
            if not r_local.get(k_in_trade(sym)):
                return

            # ✅ if trading disabled (daily squareoff), don't do anything
            if (r_local.get(TRADING_DISABLED_KEY) or "").strip() == "1":
                return

            base_entry_s = (r_local.get(k_base_entry(sym)) or "").strip()
            base_qty_s = (r_local.get(k_base_qty(sym)) or "").strip()
            step_s = (r_local.get(k_step(sym)) or "0").strip()
            sl_oid = (r_local.get(k_sl_oid(sym)) or "").strip()
            tgt_oid = (r_local.get(k_tgt_oid(sym)) or "").strip()

            if not base_entry_s or not base_qty_s or not sl_oid or not tgt_oid:
                return

            base_entry = float(base_entry_s)
            base_qty = int(float(base_qty_s))
            step = int(step_s)

            if base_entry <= 0 or base_qty <= 0:
                return

            max_step = int(round(float(TARGET_PCT_ABOVE_ENTRY) / float(TRAIL_STEP_PCT)))
            if max_step < 1:
                max_step = 1

            next_step = step + 1
            if next_step > max_step:
                return

            next_trigger = base_entry * (1.0 + float(TRAIL_STEP_PCT) * float(next_step))
            if float(ltp) < float(next_trigger):
                return

            new_step = next_step

            new_sl_level = base_entry * (1.0 + float(TRAIL_STEP_PCT) * max(0, new_step - 1))
            t = tick_size(sym)
            new_sl_level = floor_to_tick(new_sl_level, t)

            new_target = base_entry * (1.0 + float(TARGET_PCT_ABOVE_ENTRY))
            new_target = ceil_to_tick(new_target, t)

            add_buy = (new_step < max_step)

            refresh_token(force=True)
            if not _last_access_token:
                diag_set(sym, trail_error="missing_access_token", trail_error_ts=int(time.time()))
                return

            total_qty = int(float(r_local.get(k_qty(sym)) or "0") or 0)
            if total_qty <= 0:
                total_qty = base_qty

            # 1) optional add buy
            if add_buy:
                try:
                    buy_oid2 = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(base_qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )
                    filled2, _avg2 = wait_for_complete_and_avg(buy_oid2, timeout_s=10)
                    if filled2:
                        total_qty += int(base_qty)
                        r_local.set(k_qty(sym), str(int(total_qty)))
                        diag_set(sym, pyramid_buy_step=new_step, pyramid_buy_oid=buy_oid2, total_qty=total_qty)
                    else:
                        diag_set(sym, pyramid_buy_failed_step=new_step, pyramid_buy_oid=buy_oid2)
                except Exception as e:
                    diag_set(sym, pyramid_buy_error_step=new_step, pyramid_buy_error=str(e))

            # 2) cancel old exits (both), then place new exits for UPDATED total qty
            try:
                try:
                    kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=tgt_oid)
                except Exception:
                    pass
                try:
                    kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=sl_oid)
                except Exception:
                    pass

                new_sl_oid = kite_local.place_order(
                    variety=kite_local.VARIETY_REGULAR,
                    exchange=EXCHANGE,
                    tradingsymbol=sym,
                    transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                    quantity=int(total_qty),
                    product=PRODUCT,
                    order_type=kite_local.ORDER_TYPE_SLM,
                    trigger_price=float(new_sl_level),
                )

                new_tgt_oid = kite_local.place_order(
                    variety=kite_local.VARIETY_REGULAR,
                    exchange=EXCHANGE,
                    tradingsymbol=sym,
                    transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                    quantity=int(total_qty),
                    product=PRODUCT,
                    order_type=kite_local.ORDER_TYPE_LIMIT,
                    price=float(new_target),
                )

                r_local.set(k_sl(sym), str(float(new_sl_level)))
                r_local.set(k_target(sym), str(float(new_target)))
                r_local.set(k_sl_oid(sym), str(new_sl_oid))
                r_local.set(k_tgt_oid(sym), str(new_tgt_oid))
                r_local.set(k_step(sym), str(int(new_step)))

                diag_set(
                    sym,
                    trail_step=new_step,
                    trail_sl=float(new_sl_level),
                    trail_target=float(new_target),
                    total_qty=int(total_qty),
                    trail_update_ts=int(time.time()),
                )

            except Exception as e:
                diag_set(sym, trail_exit_replace_error=str(e), trail_exit_replace_error_ts=int(time.time()))

        except Exception:
            pass

    # ==========================================================
    # ✅ REST OPENING CANDLES (ONE CALL, IST-NORMALIZED)
    # ==========================================================
    _rest_opening: Dict[str, dict] = {}  # sym -> {"c915":..., "c916":...}

    def fetch_opening_candles(sym: str) -> Optional[dict]:
        """
        Fetch 09:15 candle (and 09:16 if available) in ONE REST call.
        Range: 09:15 -> 09:18 (small buffer), then extract exact minutes by IST.
        """
        try:
            tok = allowed_stocks.get(sym)
            if not tok:
                return None

            today = datetime.datetime.now(IST).date()
            start_dt = datetime.datetime.combine(today, datetime.time(9, 15), tzinfo=IST)
            end_dt = datetime.datetime.combine(today, datetime.time(9, 18), tzinfo=IST)

            data = kite_local.historical_data(
                instrument_token=int(tok),
                from_date=start_dt,
                to_date=end_dt,
                interval="minute"
            )

            c915 = None
            c916 = None

            for c in data or []:
                dt = c.get("date")
                if dt is None:
                    continue
                if isinstance(dt, str):
                    try:
                        dt = datetime.datetime.fromisoformat(dt)
                    except Exception:
                        continue

                dt = to_ist(dt)
                if dt.date() != today:
                    continue

                if dt.hour == 9 and dt.minute == 15:
                    c915 = c
                elif dt.hour == 9 and dt.minute == 16:
                    c916 = c

            if not c915:
                return None

            out = {
                "c915": {
                    "open": float(c915["open"]),
                    "high": float(c915["high"]),
                    "low": float(c915["low"]),
                    "close": float(c915["close"]),
                },
            }
            if c916:
                out["c916"] = {
                    "open": float(c916["open"]),
                    "high": float(c916["high"]),
                    "low": float(c916["low"]),
                    "close": float(c916["close"]),
                }
            return out
        except Exception:
            return None

    def try_lock_opening_from_rest(sym: str, ts: datetime.datetime, m: dict):
        """
        Lock opening reference once time >= 09:16 IST (so 09:15 candle is complete).
        Single attempt only.
        """
        if m.get("open_locked") or m.get("ignored"):
            return

        ts = to_ist(ts)

        if ts.time() < datetime.time(9, 16):
            return

        now_s = time.time()
        if now_s < float(m.get("_rest_retry_after", 0.0) or 0.0):
            return

        refresh_token(force=True)
        if not _last_access_token:
            if not m.get("_diag_no_token"):
                m["_diag_no_token"] = True
                diag_set(sym, opening_fetch_waiting_token=1, last_skip_ts=int(now_s))
            return

        attempts = int(m.get("_rest_attempts", 0) or 0)
        max_attempts = int(os.environ.get("OPENING_FETCH_MAX_ATTEMPTS", "5"))
        if attempts >= max_attempts:
            m["open_locked"] = True
            m["ignored"] = True
            m["pattern_ok"] = False
            m["ignore_reason"] = "opening_fetch_failed"
            diag_set(sym, open_locked=1, ignored=1, pattern_ok=0, ignore_reason=m["ignore_reason"])
            return

        got = _rest_opening.get(sym)
        if not got:
            got = fetch_opening_candles(sym)
            if got:
                _rest_opening[sym] = got

        if not got:
            attempts += 1
            m["_rest_attempts"] = attempts
            retry_s = float(os.environ.get("OPENING_FETCH_RETRY_S", "10"))
            m["_rest_retry_after"] = now_s + retry_s
            diag_set(sym, opening_fetch_attempts=attempts, opening_fetch_retry_after=int(m["_rest_retry_after"]))
            return

        c915 = got["c915"]
        c916 = got.get("c916")

        o1, h1, l1, cl1 = c915["open"], c915["high"], c915["low"], c915["close"]
        m["first_high"] = float(h1)

        ignored = False
        if OPENING_PATTERN_MODE == "LEGACY":
            red1 = (cl1 < o1)
            ignored = (not red1)
            if ignored:
                m["ignore_reason"] = "first_candle_not_red"

        m["open_locked"] = True
        m["ignored"] = bool(ignored)
        m["pattern_ok"] = bool(not ignored)

        if c916:
            m["day_high"] = float(max(h1, float(c916["high"])))
        else:
            m["day_high"] = float(h1)
        diag_set(
            sym,
            open_locked=1,
            ignored=int(bool(m["ignored"])),
            pattern_ok=int(bool(m["pattern_ok"])),
            first_high=m["first_high"],
            day_high=m["day_high"],
            locked_at=ts.isoformat(),
            ignore_reason=m.get("ignore_reason", ""),
        )

    # ==========================================================
    # ✅ OCO MONITOR (CANCEL OTHER EXIT ORDER)
    # ==========================================================
    def monitor_exit_orders():
        while True:
            try:
                refresh_token()

                # ✅ if trading disabled, we still let exits complete, but don't do extra logic
                for sym in list(mem.keys()):
                    if not r_local.get(k_in_trade(sym)):
                        continue

                    sl_oid = (r_local.get(k_sl_oid(sym)) or "").strip()
                    tgt_oid = (r_local.get(k_tgt_oid(sym)) or "").strip()
                    if not sl_oid or not tgt_oid:
                        continue

                    def status_of(oid: str) -> str:
                        try:
                            h = kite_local.order_history(oid)
                            if not h:
                                return ""
                            return str(h[-1].get("status", "")).upper()
                        except Exception:
                            return ""

                    sl_st = status_of(sl_oid)
                    tg_st = status_of(tgt_oid)

                    if sl_st == "COMPLETE" and tg_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=tgt_oid)
                        except Exception:
                            pass

                    if tg_st == "COMPLETE" and sl_st not in ("CANCELLED", "REJECTED", "COMPLETE"):
                        try:
                            kite_local.cancel_order(variety=kite_local.VARIETY_REGULAR, order_id=sl_oid)
                        except Exception:
                            pass

                    if sl_st == "COMPLETE" or tg_st == "COMPLETE":
                        r_local.delete(k_in_trade(sym))
                        r_local.delete(k_entry(sym))
                        r_local.delete(k_sl(sym))
                        r_local.delete(k_target(sym))
                        r_local.delete(k_qty(sym))
                        r_local.delete(k_sl_oid(sym))
                        r_local.delete(k_tgt_oid(sym))
                        r_local.delete(k_base_entry(sym))
                        r_local.delete(k_base_qty(sym))
                        r_local.delete(k_step(sym))

            except Exception:
                pass

            time.sleep(1)

    refresh_token()
    log.info("[WORKER %s] started", worker_id)

    candle_1m: Dict[str, dict] = {}
    mem: Dict[str, dict] = {}
    pending_next_open: Dict[str, dict] = {}
    pending_breakout: Dict[str, dict] = {}  # FIRST_CANDLE mode: wait for breakout-candle high to break

    threading.Thread(target=monitor_exit_orders, daemon=True).start()

    while True:
        tick = q.get()
        if tick is None:
            break

        try:
            token = tick.get("instrument_token")
            if token is None:
                continue
            sym = token_to_symbol.get(int(token))
            if not sym:
                continue

            refresh_token()

            # ✅ if daily profit squareoff hit, do nothing in worker
            if (r_local.get(TRADING_DISABLED_KEY) or "").strip() == "1":
                continue

            if sym not in mem:
                mem[sym] = {
                    "ignored": False,
                    "pattern_ok": False,
                    "day_high": None,
                    "first_high": None,
                    "open_locked": False,
                    "_rest_attempts": 0,
                    "_rest_retry_after": 0.0,
                }
            m = mem[sym]
            if m["ignored"]:
                continue

            price = float(tick.get("last_price", 0.0))
            vol_today = float(tick.get("volume_traded", 0.0))

            ts = tick.get("exchange_timestamp")
            if ts is None:
                ts = datetime.datetime.now(IST)
            elif isinstance(ts, str):
                ts = datetime.datetime.fromisoformat(ts)
            ts = to_ist(ts)

            try_lock_opening_from_rest(sym, ts, m)
            if m["ignored"]:
                continue

            manage_trailing_and_pyramiding(sym, price)

            def maybe_entry_on_breakout_trigger(now_dt: datetime.datetime, ltp: float):
                if BREAKOUT_MODE != "FIRST_CANDLE":
                    return
                if not m.get("open_locked") or not m.get("pattern_ok"):
                    return

                # ✅ hard stop if daily profit squareoff hit
                if (r_local.get(TRADING_DISABLED_KEY) or "").strip() == "1":
                    pending_breakout.pop(sym, None)
                    return

                pe = pending_breakout.get(sym)
                if not pe:
                    return

                now_i = int(time.time())
                set_ts = int(pe.get("set_ts", 0) or 0)
                if set_ts and (now_i - set_ts) > int(PENDING_TRIGGER_TIMEOUT_S):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="trigger_timeout", last_skip_ts=now_i)
                    return

                trigger = float(pe.get("trigger") or 0.0)
                if trigger <= 0:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="bad_trigger", last_skip_ts=now_i)
                    return

                if float(ltp) <= trigger:
                    return

                try:
                    trades_done = int(r_local.get(TRADES_DONE_KEY) or "0")
                except Exception:
                    trades_done = 0
                if trades_done >= MAX_TRADES:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="max_trades_reached", trades_done=trades_done, last_skip_ts=now_i)
                    return

                if not within_new_trade_window(now_dt):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="outside_trade_window", last_skip_ts=now_i)
                    return

                if r_local.get(k_in_trade(sym)):
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="already_in_trade", last_skip_ts=now_i)
                    return

                entry_ref = float(ltp)
                if entry_ref < MIN_ENTRY_PRICE or entry_ref > MAX_ENTRY_PRICE:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="price_filter", entry=entry_ref, last_skip_ts=now_i)
                    return

                t = tick_size(sym)
                sl = float(entry_ref) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                sl = floor_to_tick(sl, t)

                if entry_ref <= 0 or sl <= 0 or entry_ref <= sl:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="invalid_sl", entry=entry_ref, sl=sl, last_skip_ts=now_i)
                    return

                qty = risk_qty(entry_ref, sl, RISK_PER_TRADE)
                if qty < 1:
                    pending_breakout.pop(sym, None)
                    diag_set(sym, last_skip_reason="qty_lt_1", entry=entry_ref, sl=sl, last_skip_ts=now_i)
                    return

                pe = pending_breakout.pop(sym, None) or pe

                try:
                    refresh_token(force=True)
                    if not _last_access_token:
                        diag_set(sym, last_order_error="missing_access_token", last_order_error_ts=now_i)
                        return

                    diag_set(
                        sym,
                        last_order_attempt_ts=now_i,
                        entry=entry_ref,
                        trigger=trigger,
                        sl=sl,
                        qty=int(qty),
                    )

                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )

                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        diag_set(sym, last_order_error="buy_not_filled", buy_order_id=buy_oid, last_order_error_ts=int(time.time()))
                        return

                    sl_final = float(avg_fill) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                    sl_final = floor_to_tick(sl_final, t)

                    if float(avg_fill) <= float(sl_final):
                        diag_set(sym, last_order_error="invalid_sl_after_fill", avg_fill=avg_fill, sl=sl_final, last_order_error_ts=int(time.time()))
                        return

                    target = float(avg_fill) * (1.0 + float(TARGET_PCT_ABOVE_ENTRY))
                    target = ceil_to_tick(target, t)

                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl_final),
                    )

                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_LIMIT,
                        price=float(target),
                    )

                    r_local.set(k_in_trade(sym), "BUY")
                    r_local.set(k_entry(sym), str(avg_fill))
                    r_local.set(k_sl(sym), str(sl_final))
                    r_local.set(k_target(sym), str(target))
                    r_local.set(k_qty(sym), str(int(qty)))
                    r_local.set(k_sl_oid(sym), str(sl_oid))
                    r_local.set(k_tgt_oid(sym), str(tgt_oid))
                    r_local.set(k_base_entry(sym), str(float(avg_fill)))
                    r_local.set(k_base_qty(sym), str(int(qty)))
                    r_local.set(k_step(sym), "0")

                    try:
                        r_local.incr(TRADES_DONE_KEY)
                    except Exception:
                        pass

                    diag_set(
                        sym,
                        last_order_ok_ts=int(time.time()),
                        buy_order_id=buy_oid,
                        sl_order_id=sl_oid,
                        tgt_order_id=tgt_oid,
                        avg_fill=avg_fill,
                        sl=sl_final,
                        target=target,
                    )

                except Exception as e:
                    diag_set(sym, last_order_error=str(e), last_order_error_ts=int(time.time()))

            maybe_entry_on_breakout_trigger(ts, price)

            minute_bucket = ts.replace(second=0, microsecond=0)
            cur = candle_1m.get(sym)

            def maybe_entry_on_open(minute_dt: datetime.datetime, open_price: float):
                pe = pending_next_open.get(sym)
                if not pe or pe["next_minute"] != minute_dt:
                    return

                # ✅ hard stop if daily profit squareoff hit
                if (r_local.get(TRADING_DISABLED_KEY) or "").strip() == "1":
                    pending_next_open.pop(sym, None)
                    return

                try:
                    trades_done = int(r_local.get(TRADES_DONE_KEY) or "0")
                except Exception:
                    trades_done = 0
                if trades_done >= MAX_TRADES:
                    diag_set(sym, last_skip_reason="max_trades_reached", trades_done=trades_done, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                if not within_new_trade_window(minute_dt):
                    diag_set(sym, last_skip_reason="outside_trade_window", last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                if r_local.get(k_in_trade(sym)):
                    diag_set(sym, last_skip_reason="already_in_trade", last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                entry = float(open_price)

                if entry < MIN_ENTRY_PRICE or entry > MAX_ENTRY_PRICE:
                    diag_set(sym, last_skip_reason="price_filter", entry=entry, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                t = tick_size(sym)
                sl = float(entry) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                sl = floor_to_tick(sl, t)

                if entry <= 0 or sl <= 0 or entry <= sl:
                    diag_set(sym, last_skip_reason="invalid_sl", entry=entry, sl=sl, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                qty = risk_qty(entry, sl, RISK_PER_TRADE)
                if qty < 1:
                    diag_set(sym, last_skip_reason="qty_lt_1", entry=entry, sl=sl, last_skip_ts=int(time.time()))
                    pending_next_open.pop(sym, None)
                    return

                target = float(entry) * (1.0 + float(TARGET_PCT_ABOVE_ENTRY))
                target = ceil_to_tick(target, t)

                try:
                    refresh_token(force=True)
                    if not _last_access_token:
                        diag_set(sym, last_order_error="missing_access_token", last_order_error_ts=int(time.time()))
                        pending_next_open.pop(sym, None)
                        return

                    diag_set(sym, last_order_attempt_ts=int(time.time()), entry=entry, sl=sl, target=target, qty=int(qty))

                    buy_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_BUY,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_MARKET,
                    )

                    filled, avg_fill = wait_for_complete_and_avg(buy_oid, timeout_s=10)
                    if not filled or avg_fill <= 0:
                        diag_set(
                            sym,
                            last_order_error="buy_not_filled",
                            buy_order_id=buy_oid,
                            last_order_error_ts=int(time.time())
                        )
                        pending_next_open.pop(sym, None)
                        return

                    r_local.set(k_base_entry(sym), str(float(avg_fill)))
                    r_local.set(k_base_qty(sym), str(int(qty)))
                    r_local.set(k_step(sym), "0")

                    sl = float(avg_fill) * (1.0 - float(SL_PCT_BELOW_ENTRY))
                    sl = floor_to_tick(sl, t)

                    target = float(avg_fill) * (1.0 + float(TARGET_PCT_ABOVE_ENTRY))
                    target = ceil_to_tick(target, t)

                    sl_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_SLM,
                        trigger_price=float(sl),
                    )

                    tgt_oid = kite_local.place_order(
                        variety=kite_local.VARIETY_REGULAR,
                        exchange=EXCHANGE,
                        tradingsymbol=sym,
                        transaction_type=kite_local.TRANSACTION_TYPE_SELL,
                        quantity=int(qty),
                        product=PRODUCT,
                        order_type=kite_local.ORDER_TYPE_LIMIT,
                        price=float(target),
                    )

                    r_local.set(k_in_trade(sym), "BUY")
                    r_local.set(k_entry(sym), str(avg_fill))
                    r_local.set(k_sl(sym), str(sl))
                    r_local.set(k_target(sym), str(target))
                    r_local.set(k_qty(sym), str(int(qty)))
                    r_local.set(k_sl_oid(sym), str(sl_oid))
                    r_local.set(k_tgt_oid(sym), str(tgt_oid))

                    try:
                        r_local.incr(TRADES_DONE_KEY)
                    except Exception:
                        pass

                    diag_set(
                        sym,
                        last_order_ok_ts=int(time.time()),
                        buy_order_id=buy_oid,
                        sl_order_id=sl_oid,
                        tgt_order_id=tgt_oid,
                        avg_fill=avg_fill,
                    )
                    pending_next_open.pop(sym, None)

                except Exception as e:
                    diag_set(sym, last_order_error=str(e), last_order_error_ts=int(time.time()))
                    pending_next_open.pop(sym, None)

            if cur is None:
                candle_1m[sym] = {
                    "minute": minute_bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "vol_today_start": vol_today,
                    "vol_today_end": vol_today,
                }
                maybe_entry_on_open(minute_bucket, price)
                continue

            if cur["minute"] == minute_bucket:
                cur["high"] = max(cur["high"], price)
                cur["low"] = min(cur["low"], price)
                cur["close"] = price
                cur["vol_today_end"] = vol_today
                continue

            closed = cur
            vol_1m = max(0.0, float(closed["vol_today_end"]) - float(closed["vol_today_start"]))

            candle_ts: datetime.datetime = closed["minute"]
            c_high = float(closed["high"])
            c_low = float(closed["low"])
            c_close = float(closed["close"])

            if not m.get("open_locked"):
                candle_1m[sym] = {
                    "minute": minute_bucket,
                    "open": price,
                    "high": price,
                    "low": price,
                    "close": price,
                    "vol_today_start": vol_today,
                    "vol_today_end": vol_today,
                }
                maybe_entry_on_open(minute_bucket, price)
                continue

            prev_day_high = float(m["day_high"] or c_high)
            m["day_high"] = max(prev_day_high, c_high)

            if within_new_trade_window(candle_ts) and m["pattern_ok"] and (not r_local.get(k_in_trade(sym))):
                if BREAKOUT_MODE == "DAY_HIGH":
                    if sym not in pending_next_open:
                        if c_high > prev_day_high:
                            ok, _val = breakout_value_ok(c_close, float(vol_1m))
                            if ok:
                                pending_next_open[sym] = {
                                    "next_minute": minute_bucket,
                                }
                                diag_set(
                                    sym,
                                    pending_next_open=minute_bucket.isoformat(),
                                    pending_sl=float(c_low),
                                    pending_set_ts=int(time.time()),
                                    breakout_close=c_close,
                                    breakout_vol_1m=float(vol_1m),
                                )

                elif BREAKOUT_MODE == "FIRST_CANDLE":
                    if sym not in pending_breakout:
                        first_high = float(m.get("first_high") or 0.0)
                        if first_high > 0 and candle_ts.time() >= datetime.time(9, 16):
                            c_open = float(closed.get("open") or 0.0)
                            if c_open < first_high and c_close > first_high:
                                ok, _val = breakout_value_ok(c_close, float(vol_1m))
                                if ok:
                                    pending_breakout[sym] = {
                                        "trigger": float(c_high),
                                        "sl": float(c_low),
                                        "set_ts": int(time.time()),
                                        "breakout_minute": candle_ts.isoformat(),
                                        "first_high": first_high,
                                        "open": c_open,
                                        "close": c_close,
                                    }
                                    diag_set(
                                        sym,
                                        pending_break_trigger=float(c_high),
                                        pending_sl=float(c_low),
                                        pending_set_ts=int(time.time()),
                                        first_high=first_high,
                                        breakout_open=c_open,
                                        breakout_close=c_close,
                                        breakout_vol_1m=float(vol_1m),
                                    )

            candle_1m[sym] = {
                "minute": minute_bucket,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "vol_today_start": vol_today,
                "vol_today_end": vol_today,
            }
            maybe_entry_on_open(minute_bucket, price)

        except Exception:
            pass

    log.info("[WORKER %s] stopped", worker_id)


# =========================
# KITE TICKER (FULL MODE)
# =========================
ticker_started = False
ticker_running = False
_ticker_lock = threading.Lock()
_tkr: Optional[KiteTicker] = None


async def run_ticker():
    global ticker_running, _tkr
    global _ws_connected, _ws_connected_ts, _ws_last_event_ts, _ws_last_error

    if not redis_ok():
        return

    access_token = (r.get(ACCESS_TOKEN_KEY) or "").strip()
    if not access_token:
        return

    with _ticker_lock:
        if ticker_running:
            return
        ticker_running = True

        try:
            if _tkr is not None:
                _tkr.close()
        except Exception:
            pass

        _tkr = KiteTicker(
            API_KEY,
            access_token,
            reconnect=True,
            reconnect_max_tries=300,
            reconnect_max_delay=60,
            connect_timeout=30,
        )

    def on_ticks(ws, ticks):
        for t in ticks:
            _route_tick_to_worker(t)

    def on_connect(ws, response):
        global _ws_connected, _ws_connected_ts, _ws_last_event_ts, _ws_last_error
        now = int(time.time())
        with _tick_lock:
            _ws_connected = True
            _ws_connected_ts = now
            _ws_last_event_ts = now
            _ws_last_error = ""
        tokens = list(allowed_stocks.values())
        ws.subscribe(tokens)
        ws.set_mode(ws.MODE_FULL, tokens)

    def on_close(ws, code, reason):
        global _ws_connected, _ws_last_error
        with _tick_lock:
            _ws_connected = False
            _ws_last_error = f"close {code}: {reason}"

    def on_reconnect(ws, attempts_count):
        global _ws_last_event_ts
        with _tick_lock:
            _ws_last_event_ts = int(time.time())

    def on_noreconnect(ws):
        global ticker_running, _ws_connected
        with _tick_lock:
            _ws_connected = False
        ticker_running = False

    def on_error(ws, code, reason):
        global ticker_running, _ws_connected, _ws_last_error
        msg = f"{code} - {reason}"
        with _tick_lock:
            _ws_last_error = msg
        if "403" in msg or str(code) == "403":
            ticker_running = False
            with _tick_lock:
                _ws_connected = False
            try:
                ws.close()
            except Exception:
                pass

    _tkr.on_ticks = on_ticks
    _tkr.on_connect = on_connect
    _tkr.on_close = on_close
    _tkr.on_error = on_error
    _tkr.on_reconnect = on_reconnect
    _tkr.on_noreconnect = on_noreconnect

    _tkr.connect(threaded=True)


def start_ticker_background():
    def runner():
        try:
            asyncio.run(run_ticker())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(run_ticker())
            loop.close()

    threading.Thread(target=runner, daemon=True).start()


# =========================
# POSITIONS SNAPSHOT UPDATER (for UI, low latency)
# =========================
def positions_snapshot_loop():
    while True:
        try:
            if not redis_ok():
                time.sleep(1)
                continue
            if not ensure_kite_token_global():
                time.sleep(1)
                continue

            pos = kite.positions()
            net = pos.get("net", [])
            rows = []
            total_pnl = 0.0

            tok_list: List[str] = []
            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper()
                tok = allowed_stocks.get(sym)
                if tok is None:
                    continue
                tok_list.append(str(int(tok)))

            ltp_by_tok: Dict[str, float] = {}
            if tok_list:
                try:
                    vals = r.hmget(LTP_HASH_KEY, tok_list)
                    for i, t in enumerate(tok_list):
                        v = vals[i]
                        if v is None or v == "":
                            continue
                        try:
                            ltp_by_tok[t] = float(v)
                        except Exception:
                            continue
                except Exception:
                    ltp_by_tok = {}

            for p in net:
                qty = int(p.get("quantity", 0))
                if qty == 0:
                    continue
                sym = str(p.get("tradingsymbol", "")).upper()
                avg = float(p.get("average_price") or 0.0)

                tok = allowed_stocks.get(sym)
                ltp = float(p.get("last_price") or 0.0)
                if tok is not None:
                    ltp = float(ltp_by_tok.get(str(int(tok)), ltp) or ltp)

                unreal = (ltp - avg) * qty
                realised = float(p.get("realised") or 0.0)
                pnl = float(unreal) + float(realised)
                total_pnl += pnl

                rows.append({
                    "symbol": sym,
                    "qty": qty,
                    "avg": avg,
                    "ltp": ltp,
                    "pnl": pnl,
                    "sl": (r.get(k_sl(sym)) or ""),
                    "target": (r.get(k_target(sym)) or ""),
                    "in_trade": bool(r.get(k_in_trade(sym)) or ""),
                })

            snap = {
                "rows": rows,
                "total_pnl": total_pnl,
                "ts": int(time.time()),
                "trades_done": int(r.get(TRADES_DONE_KEY) or "0"),
                "max_trades": MAX_TRADES,
                "daily_max_profit": DAILY_MAX_PROFIT,
                "trading_disabled": bool((r.get(TRADING_DISABLED_KEY) or "").strip() == "1"),
                "squareoff_done": bool((r.get(SQUAREOFF_DONE_KEY) or "").strip() == "1"),
            }
            r.set(POSITIONS_SNAPSHOT_KEY, json.dumps(snap))
            r.set(POSITIONS_SNAPSHOT_TS_KEY, str(snap["ts"]))

        except Exception:
            pass

        time.sleep(1)


# =========================
# ROUTES
# =========================
@app.get("/health")
def health():
    now = int(time.time())
    redis_up = redis_ok()
    with _tick_lock:
        last_ts = int(_last_tick_ts or 0)
        ticks_total = int(_ticks_total)
        last_token = int(_last_tick_token or 0)
        last_price = float(_last_tick_price or 0.0)

        ws_connected = bool(_ws_connected)
        ws_connected_ts = int(_ws_connected_ts or 0)
        ws_last_event_ts = int(_ws_last_event_ts or 0)
        ws_last_error = str(_ws_last_error or "")

    tick_age = (now - last_ts) if last_ts else None
    ws_age = (now - ws_last_event_ts) if ws_last_event_ts else None
    ws_conn_age = (now - ws_connected_ts) if ws_connected_ts else None

    has_token = False
    if redis_up:
        try:
            has_token = bool((r.get(ACCESS_TOKEN_KEY) or "").strip())
        except Exception:
            has_token = False

    trading_disabled = False
    squareoff_done = False
    if redis_up:
        try:
            trading_disabled = bool((r.get(TRADING_DISABLED_KEY) or "").strip() == "1")
            squareoff_done = bool((r.get(SQUAREOFF_DONE_KEY) or "").strip() == "1")
        except Exception:
            pass

    return {
        "status": "ok",
        "redis": redis_up,
        "workers": WORKERS,
        "tokens": len(allowed_stocks),
        "universe_mode": UNIVERSE_MODE,
        "breakout_mode": BREAKOUT_MODE,
        "opening_pattern_mode": OPENING_PATTERN_MODE,
        "token_distribution": _worker_token_counts,
        "has_access_token": has_token,
        "exchange": EXCHANGE,
        "product": PRODUCT,
        "no_new_trades_after": NO_NEW_TRADES_AFTER.isoformat(),
        "ltp_backend": "redis_hash",
        "ticker_running": ticker_running,
        "ticks_total": ticks_total,
        "last_tick_ts": last_ts,
        "last_tick_age_s": tick_age,
        "last_tick_token": last_token,
        "last_tick_price": last_price,
        "ws_connected": ws_connected,
        "ws_connected_age_s": ws_conn_age,
        "ws_last_event_age_s": ws_age,
        "ws_last_error": ws_last_error,
        "max_trades": MAX_TRADES,
        "daily_max_profit": DAILY_MAX_PROFIT,
        "trading_disabled": trading_disabled,
        "squareoff_done": squareoff_done,
    }


@app.get("/state")
def state():
    if not redis_ok():
        return {"ok": False, "error": "Redis not running"}

    snap = None
    ts = None
    raw = r.get(POSITIONS_SNAPSHOT_KEY)
    ts = r.get(POSITIONS_SNAPSHOT_TS_KEY)
    if raw:
        try:
            snap = json.loads(raw)
        except Exception:
            snap = None

    return {
        "ok": True,
        "positions": snap or {
            "rows": [],
            "total_pnl": 0.0,
            "ts": None,
            "trades_done": 0,
            "max_trades": MAX_TRADES,
            "daily_max_profit": DAILY_MAX_PROFIT,
            "trading_disabled": bool((r.get(TRADING_DISABLED_KEY) or "").strip() == "1") if redis_ok() else False,
            "squareoff_done": bool((r.get(SQUAREOFF_DONE_KEY) or "").strip() == "1") if redis_ok() else False,
        },
        "positions_ts": ts,
    }


@app.get("/universe")
def universe():
    return {
        "ok": True,
        "mode": UNIVERSE_MODE,
        "count": len(allowed_stocks),
        "allowed_stocks_path": str(ALLOWED_STOCKS_PATH),
        "derivative_stocks_path": str(DERIVATIVE_STOCKS_PATH),
        "exchange": EXCHANGE,
        "product": PRODUCT,
        "breakout_mode": BREAKOUT_MODE,
        "opening_pattern_mode": OPENING_PATTERN_MODE,
        "pending_trigger_timeout_s": PENDING_TRIGGER_TIMEOUT_S,
        "no_new_trades_after": NO_NEW_TRADES_AFTER.isoformat(),
        "min_entry_price": MIN_ENTRY_PRICE,
        "max_entry_price": MAX_ENTRY_PRICE,
        "max_trades": MAX_TRADES,
        "daily_max_profit": DAILY_MAX_PROFIT,
    }


@app.get("/diag/{symbol}")
def diag(symbol: str):
    if not redis_ok():
        return {"ok": False, "error": "Redis not running"}
    sym = symbol.strip().upper()
    try:
        d = r.hgetall(f"diag:{sym}") or {}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True, "symbol": sym, "diag": d}


@app.get("/login")
def login():
    if redis_ok() and (r.get(ACCESS_TOKEN_KEY) or "").strip():
        return RedirectResponse(url="/", status_code=303)
    return RedirectResponse(kite.login_url())


@app.get("/zerodha/callback")
async def callback(request: Request):
    request_token = request.query_params.get("request_token")
    data = kite.generate_session(request_token, api_secret=API_SECRET)
    access_token = data["access_token"]

    if redis_ok():
        r.set(ACCESS_TOKEN_KEY, access_token)
        r.set(DAY_KEY, str(int(time.time())))
        r.set(TRADES_DONE_KEY, "0")
        # ✅ reset daily squareoff flags on new login/session
        r.set(TRADING_DISABLED_KEY, "0")
        r.set(SQUAREOFF_DONE_KEY, "0")

    kite.set_access_token(access_token)

    global ticker_started
    if not ticker_started:
        ticker_started = True
        start_ticker_background()

    return RedirectResponse(url="/", status_code=303)


@app.post("/override")
def set_override(symbol: str = Form(...), sl: str = Form(...), target: str = Form(...)):
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)
    if not ensure_kite_token_global():
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    sym = symbol.strip().upper()
    try:
        sl_v = float(sl)
        t_v = float(target)
    except Exception:
        return JSONResponse({"ok": False, "error": "Invalid SL/Target"}, status_code=400)

    t = tick_size(sym)
    sl_v = floor_to_tick(sl_v, t)
    t_v = ceil_to_tick(t_v, t)

    try:
        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()

        if sl_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
            except Exception:
                pass
        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
            except Exception:
                pass

        qty = int(r.get(k_qty(sym)) or "0")
        if qty <= 0:
            pos = kite.positions().get("net", [])
            for p in pos:
                if str(p.get("tradingsymbol", "")).upper() == sym:
                    qty = abs(int(p.get("quantity", 0)))
                    break

        if qty <= 0:
            return JSONResponse({"ok": False, "error": "No quantity found for symbol"}, status_code=400)

        new_sl_oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_SLM,
            trigger_price=float(sl_v),
        )
        new_tgt_oid = kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=kite.TRANSACTION_TYPE_SELL,
            quantity=int(qty),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_LIMIT,
            price=float(t_v),
        )

        r.set(k_sl(sym), str(sl_v))
        r.set(k_target(sym), str(t_v))
        r.set(k_qty(sym), str(int(qty)))
        r.set(k_sl_oid(sym), str(new_sl_oid))
        r.set(k_tgt_oid(sym), str(new_tgt_oid))

        return {"ok": True, "symbol": sym, "sl": sl_v, "target": t_v, "qty": qty}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


@app.post("/exit/{symbol}")
def exit_symbol(symbol: str):
    if not redis_ok():
        return JSONResponse({"ok": False, "error": "Redis not running"}, status_code=500)
    if not ensure_kite_token_global():
        return JSONResponse({"ok": False, "error": "Login required"}, status_code=401)

    sym = symbol.strip().upper()

    try:
        sl_oid = (r.get(k_sl_oid(sym)) or "").strip()
        tgt_oid = (r.get(k_tgt_oid(sym)) or "").strip()
        if sl_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=sl_oid)
            except Exception:
                pass
        if tgt_oid:
            try:
                kite.cancel_order(variety=kite.VARIETY_REGULAR, order_id=tgt_oid)
            except Exception:
                pass

        qty = 0
        pos = kite.positions().get("net", [])
        for p in pos:
            if str(p.get("tradingsymbol", "")).upper() == sym:
                qty = int(p.get("quantity", 0))
                break
        if qty == 0:
            return {"ok": True, "message": "No position"}

        txn = kite.TRANSACTION_TYPE_SELL if qty > 0 else kite.TRANSACTION_TYPE_BUY
        kite.place_order(
            variety=kite.VARIETY_REGULAR,
            exchange=EXCHANGE,
            tradingsymbol=sym,
            transaction_type=txn,
            quantity=abs(int(qty)),
            product=PRODUCT,
            order_type=kite.ORDER_TYPE_MARKET,
        )

        r.delete(k_in_trade(sym))
        r.delete(k_entry(sym))
        r.delete(k_sl(sym))
        r.delete(k_target(sym))
        r.delete(k_qty(sym))
        r.delete(k_sl_oid(sym))
        r.delete(k_tgt_oid(sym))
        r.delete(k_base_entry(sym))
        r.delete(k_base_qty(sym))
        r.delete(k_step(sym))

        return {"ok": True, "message": "Exit placed"}
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


# ----------- Dashboard UI -----------
@app.get("/", response_class=HTMLResponse)
def dashboard():
    token_present = bool((r.get(ACCESS_TOKEN_KEY) or "").strip()) if redis_ok() else False
    login_btn = (
        '<button disabled style="opacity:0.6; cursor:not-allowed;">Logged in ✅</button>'
        if token_present
        else '<button onclick="window.location.href=\'/login\'">Login to Zerodha</button>'
    )

    html = f"""
    <html>
    <head>
      <title>FASTAPI Kite Algotrading</title>
      <style>
        body {{ font-family: Arial, sans-serif; padding: 18px; }}
        .row {{ display:flex; gap:12px; flex-wrap:wrap; margin: 10px 0; }}
        .card {{ border:1px solid #ddd; border-radius:10px; padding:12px; min-width:280px; }}
        .pnl-pos {{ color: green; font-weight: bold; }}
        .pnl-neg {{ color: red; font-weight: bold; }}
        table {{ border-collapse: collapse; width: 100%; margin-top: 10px; }}
        th, td {{ border: 1px solid #999; padding: 8px; text-align: left; }}
        th {{ background: #f3f3f3; }}
        input {{ width: 110px; padding:4px; }}
        button {{ padding: 6px 12px; cursor: pointer; }}
        .tiny {{ font-size: 12px; opacity: 0.9; margin-top: 8px; }}
        .badge {{
          display: inline-block;
          padding: 2px 8px;
          border-radius: 999px;
          font-size: 11px;
          font-weight: bold;
          margin: 0 6px;
          border: 1px solid #ddd;
        }}
        .badge-live {{ background: #e8f7ee; color: #157a3d; border-color: #bfe9cf; }}
        .badge-idle {{ background: #fff7dd; color: #7a5a00; border-color: #ffe29a; }}
        .badge-off {{ background: #f2f2f2; color: #555; border-color: #ddd; }}
        .badge-bad {{ background: #fdecec; color: #a32121; border-color: #f4bcbc; }}
      </style>
    </head>
    <body>
      <h1>Goutham's Custard Apple</h1>

      <div class="row">
        <div class="card">
          __LOGIN_BTN__
          <div class="tiny" style="margin-top:10px;">
            <b>Tick heartbeat:</b>
            <span id="tickBadge" class="badge badge-off">OFF</span>
            <span id="tickHb">-</span>
          </div>
          <div class="tiny" style="margin-top:10px;">
            <b>Daily Max Profit:</b> {DAILY_MAX_PROFIT} (auto squareoff)<br>
            <b>Trading Disabled:</b> <span id="tradeDisabled">-</span>
          </div>
        </div>

        <div class="card">
          <div><b>Active P&amp;L (updates every 1s):</b></div>
          <div style="font-size:22px; margin-top:8px;">
            <span id="totalPnl" class="pnl-pos">0.00</span>
          </div>
          <div class="tiny" style="margin-top:8px;">
            <b>Risk per trade:</b> 50 (Qty = 50 / (Entry - SL))<br>
            <b>SL rule:</b> Entry - 0.8% (Fixed)<br>
            <b>Entry price range:</b> 100 to 5000<br>
            <b>Max trades:</b> {MAX_TRADES}<br>
            <b>No new trades after:</b> 09:30 AM (Asia/Kolkata)<br>
            <b>Breakout value:</b> LTP * 1mVolume >= 1.0cr
          </div>
        </div>
      </div>

      <h2>Positions (MIS)</h2>
      <div>Change SL / Target from UI. Changes apply immediately (replaces exit orders).</div>

      <table>
        <thead>
          <tr>
            <th>Symbol</th><th>Qty</th><th>Avg</th><th>LTP</th><th>P&amp;L</th>
            <th>SL</th><th>Target</th><th>Update</th><th>Exit</th>
          </tr>
        </thead>
        <tbody id="posBody">
          <tr><td colspan="9">Loading...</td></tr>
        </tbody>
      </table>

      <script>
        function pnlClass(v){{ return (Number(v||0) >= 0) ? 'pnl-pos' : 'pnl-neg'; }}

        function statusBadgeGlobal(h){{
          if (!h.ticker_running) return {{cls:"badge badge-off", txt:"OFF"}};
          if (!h.ws_connected) return {{cls:"badge badge-bad", txt:"DISCONNECTED"}};
          const tickAge = (h.last_tick_age_s === null || h.last_tick_age_s === undefined) ? null : Number(h.last_tick_age_s);
          if (tickAge !== null && tickAge <= 2) return {{cls:"badge badge-live", txt:"LIVE"}};
          return {{cls:"badge badge-idle", txt:"CONNECTED (NO TICKS)"}};
        }}

        async function refreshHeartbeat(){{
          const res = await fetch("/health?ts=" + Date.now(), {{ cache: "no-store" }});
          const h = await res.json();

          const hb = document.getElementById("tickHb");
          const badgeEl = document.getElementById("tickBadge");
          if (!hb || !badgeEl) return;

          const b = statusBadgeGlobal(h);
          badgeEl.className = b.cls;
          badgeEl.textContent = b.txt;

          hb.textContent =
            "ticks=" + (h.ticks_total ?? 0) +
            ", tick_age_s=" + (h.last_tick_age_s ?? "-") +
            ", ws_age_s=" + (h.ws_last_event_age_s ?? "-") +
            (h.ws_last_error ? (", ws_err=" + h.ws_last_error) : "");

          const td = document.getElementById("tradeDisabled");
          if (td) td.textContent = (h.trading_disabled ? "YES ✅" : "NO");
        }}

        async function updateSlTarget(sym){{
          const sl = document.getElementById("sl_" + sym).value;
          const target = document.getElementById("t_" + sym).value;
          await fetch("/override", {{
            method: "POST",
            headers: {{ "Content-Type": "application/x-www-form-urlencoded" }},
            body: "symbol=" + encodeURIComponent(sym) + "&sl=" + encodeURIComponent(sl) + "&target=" + encodeURIComponent(target)
          }});
        }}

        async function exitSymbol(sym){{
          await fetch("/exit/" + encodeURIComponent(sym), {{ method: "POST" }});
        }}

        async function refreshPositions(){{
          const res = await fetch("/state?ts=" + Date.now(), {{ cache: "no-store" }});
          const data = await res.json();

          const total = Number((data.positions && data.positions.total_pnl) || 0);
          const pnlEl = document.getElementById("totalPnl");
          pnlEl.textContent = total.toFixed(2);
          pnlEl.className = pnlClass(total);

          const body = document.getElementById("posBody");
          body.innerHTML = "";

          const rows = (data.positions && data.positions.rows) ? data.positions.rows : [];
          if (!rows.length){{
            body.innerHTML = "<tr><td colspan='9'>No positions</td></tr>";
            return;
          }}

          for (const rr of rows){{
            const sym = String(rr.symbol || "").toUpperCase();
            const pnl = Number(rr.pnl || 0);
            const qty = Number(rr.qty || 0);

            const slVal = (rr.sl ?? "");
            const tVal  = (rr.target ?? "");

            const tr = document.createElement("tr");
            tr.innerHTML = `
              <td>${{sym}}</td>
              <td>${{qty}}</td>
              <td>${{Number(rr.avg||0).toFixed(2)}}</td>
              <td>${{Number(rr.ltp||0).toFixed(2)}}</td>
              <td class="${{pnlClass(pnl)}}">${{pnl.toFixed(2)}}</td>
              <td><input id="sl_${{sym}}" value="${{slVal}}"></td>
              <td><input id="t_${{sym}}" value="${{tVal}}"></td>
              <td><button onclick="updateSlTarget('${{sym}}')">Update</button></td>
              <td><button onclick="exitSymbol('${{sym}}')">Exit</button></td>
            `;
            body.appendChild(tr);
          }}
        }}

        refreshHeartbeat();
        setInterval(refreshHeartbeat, 1000);

        refreshPositions();
        setInterval(refreshPositions, 1000);
      </script>
    </body>
    </html>
    """

    html = html.replace("__LOGIN_BTN__", login_btn)
    return HTMLResponse(html)


@app.on_event("startup")
async def startup_event():
    if not redis_ok():
        log.warning("Redis not running. Start Redis on %s:%s", REDIS_HOST, REDIS_PORT)
        return

    if not r.get(TRADES_DONE_KEY):
        r.set(TRADES_DONE_KEY, "0")

    # ✅ ensure flags exist
    if not r.get(TRADING_DISABLED_KEY):
        r.set(TRADING_DISABLED_KEY, "0")
    if not r.get(SQUAREOFF_DONE_KEY):
        r.set(SQUAREOFF_DONE_KEY, "0")

    _start_workers_if_needed()

    global ticker_started
    if (r.get(ACCESS_TOKEN_KEY) or "").strip() and not ticker_started:
        ticker_started = True
        start_ticker_background()

    threading.Thread(target=ltp_flush_loop, daemon=True).start()
    threading.Thread(target=positions_snapshot_loop, daemon=True).start()

    # ✅ start daily max profit watcher
    threading.Thread(target=squareoff_all_positions_if_profit_hit, daemon=True).start()

    log.info("Startup done.")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=False)

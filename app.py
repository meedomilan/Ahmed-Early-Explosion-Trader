import asyncio
import html
import json
import logging
import math
import os
import signal
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse


# =========================================================
# الإعدادات
# =========================================================

BINANCE_BASE = os.getenv("BINANCE_BASE_URL", "https://fapi.binance.com").rstrip("/")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
PORT = int(os.getenv("PORT", "8080"))
TZ = ZoneInfo(os.getenv("TZ", "Asia/Riyadh"))

SCAN_SECONDS = int(os.getenv("SCAN_SECONDS", "15"))
MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "12"))
DEEP_CANDIDATES = int(os.getenv("DEEP_CANDIDATES", "30"))
FAST_POOL = int(os.getenv("FAST_POOL", "120"))
CACHE_SECONDS = int(os.getenv("CACHE_SECONDS", "30"))
MIN_QUOTE_VOLUME = float(os.getenv("MIN_QUOTE_VOLUME_USDT", "5000000"))

EARLY_SCORE = float(os.getenv("EARLY_SCORE", "68"))
CONFIRMED_SCORE = float(os.getenv("CONFIRMED_SCORE", "78"))
EXPLOSION_SCORE = float(os.getenv("EXPLOSION_SCORE", "86"))
COOLDOWN_MINUTES = int(os.getenv("COOLDOWN_MINUTES", "180"))
EARLY_STREAK = int(os.getenv("EARLY_STREAK", "2"))
CONFIRMED_STREAK = int(os.getenv("CONFIRMED_STREAK", "2"))
DIRECTION_GAP = float(os.getenv("DIRECTION_GAP", "7"))
MAX_EXTENSION_ATR = float(os.getenv("MAX_EXTENSION_ATR", "0.75"))
STATE_HISTORY = int(os.getenv("STATE_HISTORY", "8"))
MIN_PERSISTENCE = int(os.getenv("MIN_PERSISTENCE", "3"))
SMART_SCORE = float(os.getenv("SMART_SCORE", "72"))
INVALIDATION_STREAK = int(os.getenv("INVALIDATION_STREAK", "3"))

# v5 Reversal Entry
REVERSAL_ZONE_SCORE = float(os.getenv("REVERSAL_ZONE_SCORE", "62"))
FLOW_FLIP_SCORE = float(os.getenv("FLOW_FLIP_SCORE", "70"))
ENTRY_TRIGGER_SCORE = float(os.getenv("ENTRY_TRIGGER_SCORE", "78"))
MAX_LATE_MOVE_ATR = float(os.getenv("MAX_LATE_MOVE_ATR", "0.65"))
MAX_TRIGGER_AGE_BARS = int(os.getenv("MAX_TRIGGER_AGE_BARS", "2"))
MIN_RR_TP1 = float(os.getenv("MIN_RR_TP1", "1.0"))
MICRO_STREAK = int(os.getenv("MICRO_STREAK", "2"))

# v6 Balanced Voting
WATCH_SCORE = float(os.getenv("WATCH_SCORE", "59"))
READY_SCORE = float(os.getenv("READY_SCORE", "68"))
ENTRY_SCORE = float(os.getenv("ENTRY_SCORE", "76"))
MIN_LOCATION_POINTS = float(os.getenv("MIN_LOCATION_POINTS", "8"))
MIN_FLOW_POINTS = float(os.getenv("MIN_FLOW_POINTS", "9"))
MAX_SIGNAL_EXTENSION_ATR = float(os.getenv("MAX_SIGNAL_EXTENSION_ATR", "0.60"))
ALLOW_COUNTER_TREND = os.getenv("ALLOW_COUNTER_TREND", "true").lower() == "true"
WEIGHT_LOCATION = float(os.getenv("WEIGHT_LOCATION", "25"))
WEIGHT_FLOW = float(os.getenv("WEIGHT_FLOW", "30"))
WEIGHT_CANDLES = float(os.getenv("WEIGHT_CANDLES", "20"))
WEIGHT_MOMENTUM = float(os.getenv("WEIGHT_MOMENTUM", "15"))
WEIGHT_TREND = float(os.getenv("WEIGHT_TREND", "10"))

# v7 Scenario Engine
MIN_RECIPE_SCORE = float(os.getenv("MIN_RECIPE_SCORE", "52"))
RECIPE_READY_SCORE = float(os.getenv("RECIPE_READY_SCORE", "62"))
RECIPE_ENTRY_SCORE = float(os.getenv("RECIPE_ENTRY_SCORE", "72"))
MAX_RECIPE_EXTENSION_ATR = float(os.getenv("MAX_RECIPE_EXTENSION_ATR", "0.72"))
RECIPE_MIN_MATCHES = int(os.getenv("RECIPE_MIN_MATCHES", "3"))
LEARNING_MIN_CASES = int(os.getenv("LEARNING_MIN_CASES", "30"))
LEARNING_WEIGHT_MAX = float(os.getenv("LEARNING_WEIGHT_MAX", "12"))

# v8 Radar & Heat Engine
RADAR_POOL = int(os.getenv("RADAR_POOL", "140"))
RADAR_DEEP_CANDIDATES = int(os.getenv("RADAR_DEEP_CANDIDATES", "45"))
RADAR_HOT_KEEP = int(os.getenv("RADAR_HOT_KEEP", "25"))
RADAR_HEAT_DECAY = float(os.getenv("RADAR_HEAT_DECAY", "0.82"))
RADAR_MIN_HEAT = float(os.getenv("RADAR_MIN_HEAT", "18"))
RADAR_WATCH_SCORE = float(os.getenv("RADAR_WATCH_SCORE", "52"))
RADAR_READY_SCORE = float(os.getenv("RADAR_READY_SCORE", "62"))
RADAR_ENTRY_SCORE = float(os.getenv("RADAR_ENTRY_SCORE", "72"))
RADAR_EARLY_STREAK = int(os.getenv("RADAR_EARLY_STREAK", "1"))
RADAR_READY_STREAK = int(os.getenv("RADAR_READY_STREAK", "1"))

# v9 Adaptive Strategy
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"
ENABLE_MANUAL_TEST_ENDPOINT = os.getenv("ENABLE_MANUAL_TEST_ENDPOINT", "true").lower() == "true"
TREND_ADX_PROXY = float(os.getenv("TREND_ADX_PROXY", "58"))
RANGE_COMPRESSION = float(os.getenv("RANGE_COMPRESSION", "62"))
BREAKOUT_COMPRESSION = float(os.getenv("BREAKOUT_COMPRESSION", "70"))
EXHAUSTION_SCORE = float(os.getenv("EXHAUSTION_SCORE", "68"))
PULLBACK_MIN_SCORE = float(os.getenv("PULLBACK_MIN_SCORE", "60"))
LIQUIDITY_REVERSAL_MIN_SCORE = float(os.getenv("LIQUIDITY_REVERSAL_MIN_SCORE", "58"))
BREAKOUT_MIN_SCORE = float(os.getenv("BREAKOUT_MIN_SCORE", "62"))
ABSORPTION_REVERSAL_MIN_SCORE = float(os.getenv("ABSORPTION_REVERSAL_MIN_SCORE", "60"))

DB_PATH = os.getenv("DB_PATH", "data/reversal_entry.db")
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

TIMEFRAMES = {"15m": 15, "1h": 60, "4h": 240}
KLINE_LIMIT = 80

Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
log = logging.getLogger("early-explosion")


# =========================================================
# أدوات حسابية
# =========================================================

def clamp(x: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, x))


def safe_div(a: float, b: float, default: float = 0.0) -> float:
    return a / b if b else default


def pct_change(a: float, b: float) -> float:
    return safe_div(a - b, abs(b), 0.0) * 100.0


def ema(values: list[float], length: int) -> float:
    if not values:
        return 0.0
    alpha = 2.0 / (length + 1.0)
    out = values[0]
    for v in values[1:]:
        out = alpha * v + (1 - alpha) * out
    return out


def atr(rows: list[list[Any]], length: int = 14) -> float:
    if len(rows) < 2:
        return 0.0
    trs = []
    for i in range(1, len(rows)):
        high = float(rows[i][2])
        low = float(rows[i][3])
        prev_close = float(rows[i - 1][4])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs[-length:]) / max(1, min(length, len(trs)))


def fmt_price(x: float) -> str:
    if x >= 1000:
        return f"{x:,.2f}"
    if x >= 1:
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    if x >= 0.01:
        return f"{x:.6f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def now_local() -> datetime:
    return datetime.now(TZ)


# =========================================================
# نماذج
# =========================================================

@dataclass
class Signal:
    symbol: str
    direction: str
    stage: str
    score: float
    explosion_score: float
    entry_score: float
    safety_score: float
    scores_by_tf: dict[str, float]
    price: float
    entry_low: float
    entry_high: float
    stop: float
    tp1: float
    tp2: float
    tp3: float
    rr1: float
    rr2: float
    rr3: float
    recipe: list[str]
    details: dict[str, Any]


# =========================================================
# قاعدة البيانات
# =========================================================

SCHEMA = """
CREATE TABLE IF NOT EXISTS opportunities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    current_stage TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'OPEN',
    opened_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    early_at TEXT,
    confirmed_at TEXT,
    explosion_at TEXT,
    early_price REAL,
    confirmed_price REAL,
    explosion_price REAL,
    score REAL,
    explosion_score REAL,
    entry_score REAL,
    safety_score REAL,
    score_15m REAL,
    score_1h REAL,
    score_4h REAL,
    entry_low REAL,
    entry_high REAL,
    stop REAL,
    tp1 REAL,
    tp2 REAL,
    tp3 REAL,
    rr1 REAL,
    rr2 REAL,
    rr3 REAL,
    recipe TEXT,
    details_json TEXT,
    entered_at TEXT,
    entered_price REAL,
    tp1_at TEXT,
    tp2_at TEXT,
    tp3_at TEXT,
    stop_at TEXT,
    mfe_pct REAL DEFAULT 0,
    mae_pct REAL DEFAULT 0,
    best_price REAL,
    worst_price REAL,
    closed_at TEXT,
    outcome TEXT
);

CREATE INDEX IF NOT EXISTS idx_opp_open
ON opportunities(status, symbol, direction);

CREATE TABLE IF NOT EXISTS recipe_stats (
    recipe_id TEXT PRIMARY KEY,
    recipe_type TEXT NOT NULL,
    cases INTEGER NOT NULL DEFAULT 0,
    wins INTEGER NOT NULL DEFAULT 0,
    losses INTEGER NOT NULL DEFAULT 0,
    tp1_hits INTEGER NOT NULL DEFAULT 0,
    avg_mfe REAL NOT NULL DEFAULT 0,
    avg_mae REAL NOT NULL DEFAULT 0,
    learned_weight REAL NOT NULL DEFAULT 0,
    updated_at TEXT
);

CREATE TABLE IF NOT EXISTS rejected_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    recipe_id TEXT,
    score REAL,
    reason TEXT,
    details_json TEXT
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    scan_number INTEGER,
    symbols_total INTEGER,
    candidates_total INTEGER,
    analyzed_total INTEGER,
    alerts_sent INTEGER,
    scan_seconds REAL,
    error TEXT
);
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA)
        await db.commit()


async def get_open_opportunity(symbol: str, direction: str):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """SELECT * FROM opportunities
               WHERE symbol=? AND direction=? AND status='OPEN'
               ORDER BY id DESC LIMIT 1""",
            (symbol, direction),
        )
        return await cur.fetchone()


async def save_signal(sig: Signal) -> tuple[int, bool]:
    existing = await get_open_opportunity(sig.symbol, sig.direction)
    ts = now_local().isoformat()
    stage_col = {
        "EARLY": ("early_at", "early_price"),
        "CONFIRMED": ("confirmed_at", "confirmed_price"),
        "EXPLOSION": ("explosion_at", "explosion_price"),
    }[sig.stage]

    async with aiosqlite.connect(DB_PATH) as db:
        if not existing:
            values = {
                "early_at": None, "confirmed_at": None, "explosion_at": None,
                "early_price": None, "confirmed_price": None, "explosion_price": None,
            }
            values[stage_col[0]] = ts
            values[stage_col[1]] = sig.price
            cur = await db.execute(
                """INSERT INTO opportunities (
                    symbol,direction,current_stage,status,opened_at,updated_at,
                    early_at,confirmed_at,explosion_at,
                    early_price,confirmed_price,explosion_price,
                    score,explosion_score,entry_score,safety_score,
                    score_15m,score_1h,score_4h,
                    entry_low,entry_high,stop,tp1,tp2,tp3,rr1,rr2,rr3,
                    recipe,details_json
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    sig.symbol,sig.direction,sig.stage,"OPEN",ts,ts,
                    values["early_at"],values["confirmed_at"],values["explosion_at"],
                    values["early_price"],values["confirmed_price"],values["explosion_price"],
                    sig.score,sig.explosion_score,sig.entry_score,sig.safety_score,
                    sig.scores_by_tf.get("15m",0),sig.scores_by_tf.get("1h",0),sig.scores_by_tf.get("4h",0),
                    sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,
                    sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.recipe),json.dumps(sig.details),
                ),
            )
            await db.commit()
            return cur.lastrowid, True

        stages = {"EARLY": 1, "CONFIRMED": 2, "EXPLOSION": 3}
        if stages[sig.stage] <= stages[existing["current_stage"]]:
            return int(existing["id"]), False

        await db.execute(
            f"""UPDATE opportunities SET
                current_stage=?,updated_at=?,
                {stage_col[0]}=?,{stage_col[1]}=?,
                score=?,explosion_score=?,entry_score=?,safety_score=?,
                score_15m=?,score_1h=?,score_4h=?,
                entry_low=?,entry_high=?,stop=?,tp1=?,tp2=?,tp3=?,
                rr1=?,rr2=?,rr3=?,recipe=?,details_json=?
                WHERE id=?""",
            (
                sig.stage,ts,ts,sig.price,
                sig.score,sig.explosion_score,sig.entry_score,sig.safety_score,
                sig.scores_by_tf.get("15m",0),sig.scores_by_tf.get("1h",0),sig.scores_by_tf.get("4h",0),
                sig.entry_low,sig.entry_high,sig.stop,sig.tp1,sig.tp2,sig.tp3,
                sig.rr1,sig.rr2,sig.rr3,json.dumps(sig.recipe),json.dumps(sig.details),
                existing["id"],
            ),
        )
        await db.commit()
        return int(existing["id"]), True


async def record_checkpoint(scan_no, symbols, candidates, analyzed, alerts, seconds, error=None):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO checkpoints
            (created_at,scan_number,symbols_total,candidates_total,analyzed_total,alerts_sent,scan_seconds,error)
            VALUES (?,?,?,?,?,?,?,?)""",
            (now_local().isoformat(), scan_no, symbols, candidates, analyzed, alerts, seconds, error),
        )
        await db.commit()


# =========================================================
# عميل Binance
# =========================================================

class BinanceClient:
    def __init__(self):
        self.session: aiohttp.ClientSession | None = None
        self.sem = asyncio.Semaphore(MAX_CONCURRENCY)
        self.cache: dict[str, tuple[float, Any]] = {}

    async def start(self):
        timeout = aiohttp.ClientTimeout(total=20)
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self.session:
            await self.session.close()

    async def get(self, path: str, params=None, cache_key: str | None = None, cache_ttl: int = 0):
        assert self.session
        key = cache_key or (path + "?" + json.dumps(params or {}, sort_keys=True))
        cached = self.cache.get(key)
        if cached and cache_ttl and time.time() - cached[0] <= cache_ttl:
            return cached[1]

        last_error = None
        async with self.sem:
            for attempt in range(5):
                try:
                    async with self.session.get(BINANCE_BASE + path, params=params) as r:
                        if r.status in (418, 429):
                            retry_after = float(r.headers.get("Retry-After", 1 + attempt * 2))
                            await asyncio.sleep(min(12, retry_after))
                            continue
                        if r.status >= 500:
                            await asyncio.sleep(min(8, 0.8 * (2 ** attempt)))
                            continue
                        r.raise_for_status()
                        data = await r.json()
                        self.cache[key] = (time.time(), data)
                        return data
                except Exception as exc:
                    last_error = exc
                    await asyncio.sleep(min(8, 0.6 * (2 ** attempt)))

        # عند تعطل Binance مؤقتًا نستخدم آخر نسخة ناجحة بدل إسقاط الدورة كاملة.
        if cached:
            log.warning("Using stale Binance cache for %s after error: %r", path, last_error)
            return cached[1]
        raise RuntimeError(f"Binance request failed: {path}: {last_error!r}")

    async def symbols(self) -> list[str]:
        data = await self.get("/fapi/v1/exchangeInfo", cache_key="exchangeInfo", cache_ttl=3600)
        return [
            s["symbol"] for s in data["symbols"]
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        ]

    async def tickers(self):
        return await self.get("/fapi/v1/ticker/24hr", cache_key="ticker24h", cache_ttl=2)

    async def klines(self, symbol: str, interval: str):
        return await self.get("/fapi/v1/klines", {
            "symbol": symbol, "interval": interval, "limit": KLINE_LIMIT
        })

    async def oi(self, symbol: str):
        return await self.get("/fapi/v1/openInterest", {"symbol": symbol})

    async def oi_hist(self, symbol: str, period: str):
        return await self.get("/futures/data/openInterestHist", {
            "symbol": symbol, "period": period, "limit": 8
        })

    async def depth(self, symbol: str):
        return await self.get("/fapi/v1/depth", {"symbol": symbol, "limit": 100})

    async def premium(self, symbol: str):
        return await self.get("/fapi/v1/premiumIndex", {"symbol": symbol})


# =========================================================
# التحليل
# =========================================================

def candle_features(rows: list[list[Any]], direction: str) -> dict[str, float]:
    closes = [float(x[4]) for x in rows]
    highs = [float(x[2]) for x in rows]
    lows = [float(x[3]) for x in rows]
    volumes = [float(x[5]) for x in rows]
    taker_buy = [float(x[9]) for x in rows]

    price = closes[-1]
    a = atr(rows)
    avg_vol = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
    vol_ratio = safe_div(volumes[-1], avg_vol, 1.0)

    deltas = [(2 * tb - v) for tb, v in zip(taker_buy, volumes)]
    delta_now = sum(deltas[-3:])
    delta_prev = sum(deltas[-6:-3])
    delta_accel = pct_change(delta_now, delta_prev) if delta_prev else 0.0
    cvd = sum(deltas[-20:])
    cvd_prev = sum(deltas[-25:-5])

    recent_range = max(highs[-6:]) - min(lows[-6:])
    range_atr = safe_div(recent_range, a, 0.0)
    compression = clamp((2.5 - range_atr) / 2.0 * 100)

    ema9 = ema(closes[-30:], 9)
    ema21 = ema(closes[-50:], 21)
    trend = pct_change(ema9, ema21)

    breakout_up = price > max(highs[-8:-1])
    breakout_down = price < min(lows[-8:-1])
    near_up = safe_div(max(highs[-8:-1]) - price, a, 99) < 0.35
    near_down = safe_div(price - min(lows[-8:-1]), a, 99) < 0.35

    price_move = pct_change(closes[-1], closes[-4])
    compression_mid = (max(highs[-6:]) + min(lows[-6:])) / 2
    extension_atr = safe_div(abs(price - compression_mid), a, 0.0)
    cvd_div = (cvd > cvd_prev and abs(price_move) < 0.8) if direction == "BUY" else (cvd < cvd_prev and abs(price_move) < 0.8)

    signed_delta = delta_now if direction == "BUY" else -delta_now
    signed_cvd = cvd if direction == "BUY" else -cvd
    signed_trend = trend if direction == "BUY" else -trend
    breakout = breakout_up if direction == "BUY" else breakout_down
    near_break = near_up if direction == "BUY" else near_down

    return {
        "price": price,
        "atr": a,
        "vol_ratio": vol_ratio,
        "delta_strength": clamp(50 + safe_div(signed_delta, max(sum(volumes[-3:]), 1), 0) * 250),
        "delta_accel": clamp(50 + (delta_accel if direction == "BUY" else -delta_accel) * 0.35),
        "cvd_strength": clamp(50 + safe_div(signed_cvd, max(sum(volumes[-20:]), 1), 0) * 300),
        "cvd_divergence": 100.0 if cvd_div else 35.0,
        "compression": compression,
        "volume": clamp((vol_ratio - 0.7) * 65),
        "trend": clamp(50 + signed_trend * 20),
        "breakout": 100.0 if breakout else (72.0 if near_break else 25.0),
        "is_breakout": breakout,
        "near_break": near_break,
        "extension_atr": extension_atr,
        "price_move_pct": price_move,
        "swing_low": min(lows[-12:]),
        "swing_high": max(highs[-12:]),
    }


def orderbook_features(depth: dict, direction: str) -> dict[str, float]:
    bids = [(float(p), float(q)) for p, q in depth.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in depth.get("asks", [])]
    bid_notional = sum(p*q for p, q in bids[:30])
    ask_notional = sum(p*q for p, q in asks[:30])
    imbalance = safe_div(bid_notional - ask_notional, bid_notional + ask_notional, 0.0)
    signed = imbalance if direction == "BUY" else -imbalance

    # تركيز أوامر كبيرة: مؤشر Iceberg/Absorption احتمالي وليس إثباتًا.
    bid_sizes = [q for _, q in bids[:50]]
    ask_sizes = [q for _, q in asks[:50]]
    side = bid_sizes if direction == "BUY" else ask_sizes
    opp = ask_sizes if direction == "BUY" else bid_sizes
    side_peak = max(side, default=0)
    side_avg = sum(side)/max(1,len(side))
    opp_avg = sum(opp)/max(1,len(opp))
    wall = safe_div(side_peak, side_avg, 0)

    return {
        "imbalance_raw": imbalance,
        "imbalance": clamp(50 + signed * 180),
        "absorption": clamp(35 + max(0, wall - 2) * 12),
        "iceberg": clamp(30 + max(0, wall - 3) * 14),
        "spoof_risk": clamp(max(0, wall - 8) * 14 + (20 if side_avg > opp_avg*4 else 0)),
    }


def oi_features(hist: list[dict], direction: str) -> dict[str, float]:
    if len(hist) < 2:
        return {"oi_change": 0, "oi_score": 50}
    vals = [float(x.get("sumOpenInterest", 0)) for x in hist]
    change = pct_change(vals[-1], vals[-4] if len(vals) >= 4 else vals[0])
    # ارتفاع OI مفيد للاتجاهين؛ اتجاه السعر والدلتا يحددان BUY/SELL.
    return {"oi_change": change, "oi_score": clamp(50 + change * 18)}


def funding_features(data: dict, direction: str) -> dict[str, float]:
    funding = float(data.get("lastFundingRate", 0)) * 100
    # التمويل عامل مساعد: التمويل السلبي يدعم BUY، والموجب يدعم SELL.
    signed = -funding if direction == "BUY" else funding
    return {"funding": funding, "funding_score": clamp(50 + signed * 900)}


def combine_score(c: dict, ob: dict, oi: dict, fund: dict, tf: str) -> float:
    # الأوزان تركز على الدلتا/CVD/OI/الدفتر قبل الكسر.
    weights = {
        "15m": (0.22,0.17,0.12,0.15,0.13,0.08,0.05,0.04,0.04),
        "1h":  (0.19,0.15,0.11,0.13,0.14,0.09,0.09,0.06,0.04),
        "4h":  (0.14,0.11,0.08,0.10,0.13,0.08,0.18,0.14,0.04),
    }[tf]
    vals = [
        c["delta_strength"], c["cvd_strength"], c["cvd_divergence"],
        ob["imbalance"], oi["oi_score"], c["volume"],
        c["trend"], c["breakout"], fund["funding_score"],
    ]
    score = sum(w*v for w,v in zip(weights, vals))

    # مكافأة الضغط المبكر، وعقوبة مخاطرة الأوامر الوهمية ومطاردة الحركة.
    score += c["compression"] * (0.07 if tf == "15m" else 0.03)
    score += ob["absorption"] * (0.05 if tf == "15m" else 0.02)
    score -= ob["spoof_risk"] * 0.12
    score -= clamp((c.get("extension_atr", 0.0) - 0.45) * 35, 0, 18)
    return clamp(score)



def _ohlcv(rows: list[list[Any]]) -> dict[str, list[float]]:
    return {
        "o": [float(x[1]) for x in rows],
        "h": [float(x[2]) for x in rows],
        "l": [float(x[3]) for x in rows],
        "c": [float(x[4]) for x in rows],
        "v": [float(x[5]) for x in rows],
        "tb": [float(x[9]) for x in rows],
    }


def detect_fvg(rows: list[list[Any]], direction: str) -> dict[str, float | bool]:
    if len(rows) < 5:
        return {"active": False, "low": 0.0, "high": 0.0, "distance_atr": 99.0}
    d = _ohlcv(rows)
    a = atr(rows)
    price = d["c"][-1]
    best = None
    for i in range(max(2, len(rows)-18), len(rows)-1):
        if direction == "BUY":
            # bullish FVG: low candle i > high candle i-2
            if d["l"][i] > d["h"][i-2]:
                low, high = d["h"][i-2], d["l"][i]
                if price >= low:
                    best = (low, high)
        else:
            # bearish FVG: high candle i < low candle i-2
            if d["h"][i] < d["l"][i-2]:
                low, high = d["h"][i], d["l"][i-2]
                if price <= high:
                    best = (low, high)
    if not best:
        return {"active": False, "low": 0.0, "high": 0.0, "distance_atr": 99.0}
    mid = (best[0] + best[1]) / 2
    return {
        "active": True,
        "low": best[0],
        "high": best[1],
        "distance_atr": safe_div(abs(price-mid), a, 99.0),
    }


def detect_order_block(rows: list[list[Any]], direction: str) -> dict[str, float | bool]:
    if len(rows) < 10:
        return {"active": False, "low": 0.0, "high": 0.0, "distance_atr": 99.0}
    d = _ohlcv(rows)
    a = atr(rows)
    price = d["c"][-1]
    best = None
    # آخر شمعة معاكسة قبل displacement واضح
    for i in range(max(2, len(rows)-22), len(rows)-3):
        body = abs(d["c"][i+1] - d["o"][i+1])
        avg_body = sum(abs(d["c"][j]-d["o"][j]) for j in range(max(0,i-8), i+1)) / max(1, min(9, i+1))
        displacement = body >= max(avg_body * 1.55, a * 0.35)
        if not displacement:
            continue
        if direction == "BUY":
            opposing = d["c"][i] < d["o"][i]
            moved = d["c"][i+1] > d["h"][i]
        else:
            opposing = d["c"][i] > d["o"][i]
            moved = d["c"][i+1] < d["l"][i]
        if opposing and moved:
            best = (d["l"][i], d["h"][i])
    if not best:
        return {"active": False, "low": 0.0, "high": 0.0, "distance_atr": 99.0}
    mid = (best[0]+best[1])/2
    return {
        "active": True,
        "low": best[0],
        "high": best[1],
        "distance_atr": safe_div(abs(price-mid), a, 99.0),
    }



def rsi_series(values: list[float], length: int = 14) -> list[float]:
    if len(values) < length + 1:
        return [50.0] * len(values)
    gains, losses = [], []
    out = [50.0] * length
    for i in range(1, len(values)):
        ch = values[i] - values[i-1]
        gains.append(max(ch, 0.0))
        losses.append(max(-ch, 0.0))
        if i < length:
            continue
        if i == length:
            avg_gain = sum(gains[-length:]) / length
            avg_loss = sum(losses[-length:]) / length
        else:
            avg_gain = (avg_gain * (length - 1) + gains[-1]) / length
            avg_loss = (avg_loss * (length - 1) + losses[-1]) / length
        rs = avg_gain / avg_loss if avg_loss else 999.0
        out.append(100 - 100 / (1 + rs))
    while len(out) < len(values):
        out.insert(0, 50.0)
    return out[-len(values):]


def ema_series(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def stddev(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((x-mean)**2 for x in values) / len(values))


def indicator_features(rows: list[list[Any]], direction: str) -> dict[str, Any]:
    d = _ohlcv(rows)
    c, h, l, o, v = d["c"], d["h"], d["l"], d["o"], d["v"]
    if len(c) < 35:
        return {}

    sign = 1 if direction == "BUY" else -1
    price = c[-1]
    a = atr(rows)

    # Bollinger 20,2
    basis = sum(c[-20:]) / 20
    dev = stddev(c[-20:])
    upper = basis + 2 * dev
    lower = basis - 2 * dev
    band_width = safe_div(upper-lower, basis, 0) * 100
    if direction == "BUY":
        bb_touch = l[-1] <= lower or c[-2] <= lower
        bb_reclaim = c[-1] > lower and c[-2] <= lower
        bb_distance = safe_div(price-lower, a, 99)
    else:
        bb_touch = h[-1] >= upper or c[-2] >= upper
        bb_reclaim = c[-1] < upper and c[-2] >= upper
        bb_distance = safe_div(upper-price, a, 99)

    # RSI + simple divergence
    rsis = rsi_series(c, 14)
    rsi_now = rsis[-1]
    rsi_prev = rsis[-3]
    if direction == "BUY":
        rsi_turn = rsi_now > rsi_prev and rsi_prev < 48
        rsi_extreme = rsi_now <= 38
        price_extreme = l[-1] <= min(l[-8:-1])
        rsi_div = price_extreme and rsi_now > min(rsis[-8:-1])
    else:
        rsi_turn = rsi_now < rsi_prev and rsi_prev > 52
        rsi_extreme = rsi_now >= 62
        price_extreme = h[-1] >= max(h[-8:-1])
        rsi_div = price_extreme and rsi_now < max(rsis[-8:-1])

    # MACD 12,26,9
    e12 = ema_series(c, 12)
    e26 = ema_series(c, 26)
    macd = [x-y for x,y in zip(e12,e26)]
    signal = ema_series(macd, 9)
    hist = [x-y for x,y in zip(macd,signal)]
    hist_now, hist_prev, hist_prev2 = hist[-1], hist[-2], hist[-3]
    if direction == "BUY":
        macd_turn = hist_now > hist_prev > hist_prev2
        macd_cross_early = hist_now > 0 and hist_prev <= 0
    else:
        macd_turn = hist_now < hist_prev < hist_prev2
        macd_cross_early = hist_now < 0 and hist_prev >= 0

    # EMA 7/25/50/200
    e7 = ema_series(c, 7)[-1]
    e25 = ema_series(c, 25)[-1]
    e50 = ema_series(c, 50)[-1]
    e200 = ema_series(c, min(200, len(c)))[-1]
    if direction == "BUY":
        ema_fast_reclaim = price > e7 and c[-2] <= ema_series(c,7)[-2]
        ema_alignment = e7 > e25
        ema_context = price > e50
        major_context = price > e200
    else:
        ema_fast_reclaim = price < e7 and c[-2] >= ema_series(c,7)[-2]
        ema_alignment = e7 < e25
        ema_context = price < e50
        major_context = price < e200

    # ROC / momentum
    roc3 = pct_change(c[-1], c[-4])
    roc6 = pct_change(c[-1], c[-7])
    signed_roc3 = roc3 * sign
    signed_roc6 = roc6 * sign
    momentum_turn = signed_roc3 > signed_roc6 and signed_roc3 > -0.25

    # volume pressure
    avg_v = sum(v[-21:-1]) / 20
    volume_ratio = safe_div(v[-1], avg_v, 1.0)
    volume_expansion = volume_ratio >= 1.20

    return {
        "bb_touch": bb_touch,
        "bb_reclaim": bb_reclaim,
        "bb_distance_atr": bb_distance,
        "band_width_pct": band_width,
        "rsi": rsi_now,
        "rsi_turn": rsi_turn,
        "rsi_extreme": rsi_extreme,
        "rsi_divergence": rsi_div,
        "macd_turn": macd_turn,
        "macd_cross_early": macd_cross_early,
        "macd_hist": hist_now,
        "ema_fast_reclaim": ema_fast_reclaim,
        "ema_alignment": ema_alignment,
        "ema_context": ema_context,
        "major_context": major_context,
        "momentum_turn": momentum_turn,
        "roc3": roc3,
        "roc6": roc6,
        "volume_ratio": volume_ratio,
        "volume_expansion": volume_expansion,
        "ema7": e7,
        "ema25": e25,
        "ema50": e50,
        "ema200": e200,
    }


def balanced_vote(
    direction: str,
    zone: dict,
    m1: dict,
    m3: dict,
    ind1: dict,
    ind3: dict,
    ind15: dict,
    ob: dict,
    oi15: dict,
    smart: dict,
) -> dict[str, Any]:
    # -------------------------
    # Location: max 25
    # -------------------------
    location = 0.0
    reasons_location = []
    if zone.get("near_structure_atr",99) <= 0.85:
        location += 8; reasons_location.append("قرب سيولة/دعم أو مقاومة")
    if zone.get("ob_distance_atr",99) <= 0.85:
        location += 7; reasons_location.append("قرب Order Block")
    if zone.get("fvg_distance_atr",99) <= 0.75:
        location += 4; reasons_location.append("قرب FVG")
    if ind15.get("bb_touch"):
        location += 4; reasons_location.append("ملامسة Bollinger")
    if m1.get("sweep") or m3.get("sweep"):
        location += 5; reasons_location.append("سحب سيولة")
    location = min(location, WEIGHT_LOCATION)

    # -------------------------
    # Flow: max 30
    # -------------------------
    flow = 0.0
    reasons_flow = []
    if m1.get("delta_flip"):
        flow += 8; reasons_flow.append("Delta Flip على 1M")
    elif m1.get("delta_accel",0) >= 58:
        flow += 5; reasons_flow.append("تسارع Delta")
    if m3.get("delta_flip"):
        flow += 5; reasons_flow.append("تأكيد Delta على 3M")
    if ob.get("imbalance",50) >= 58:
        flow += 6; reasons_flow.append("Order Book يميل للاتجاه")
    if ob.get("absorption",35) >= 55 or m1.get("absorption_candle"):
        flow += 5; reasons_flow.append("Absorption")
    if smart.get("smart_score",50) >= 64:
        flow += 4; reasons_flow.append("استمرار بصمة التدفق")
    if oi15.get("oi_change",0) > 0.10:
        flow += 2; reasons_flow.append("OI يرتفع")
    if ob.get("spoof_risk",0) >= 65:
        flow -= 4; reasons_flow.append("خصم بسبب Spoofing محتمل")
    flow = clamp(flow, 0, WEIGHT_FLOW)

    # -------------------------
    # Candles/structure: max 20
    # -------------------------
    candles = 0.0
    reasons_candles = []
    if m1.get("rejection"):
        candles += 5; reasons_candles.append("شمعة رفض")
    if m1.get("engulf"):
        candles += 5; reasons_candles.append("Engulfing")
    if m1.get("micro_break"):
        candles += 6; reasons_candles.append("Micro BOS على 1M")
    if m3.get("micro_break"):
        candles += 3; reasons_candles.append("تأكيد بنية على 3M")
    if m1.get("structure"):
        candles += 2; reasons_candles.append("بداية Higher Low/Lower High")
    candles = min(candles, WEIGHT_CANDLES)

    # -------------------------
    # Momentum: max 15
    # -------------------------
    momentum = 0.0
    reasons_momentum = []
    if ind1.get("rsi_divergence"):
        momentum += 5; reasons_momentum.append("RSI Divergence")
    elif ind1.get("rsi_turn"):
        momentum += 3; reasons_momentum.append("RSI بدأ ينعكس")
    if ind1.get("macd_turn"):
        momentum += 4; reasons_momentum.append("MACD Histogram ينعكس")
    if ind1.get("macd_cross_early"):
        momentum += 2; reasons_momentum.append("MACD Cross مبكر")
    if ind3.get("momentum_turn"):
        momentum += 2; reasons_momentum.append("الزخم يتحول على 3M")
    if ind1.get("volume_expansion"):
        momentum += 2; reasons_momentum.append("Volume Expansion")
    momentum = min(momentum, WEIGHT_MOMENTUM)

    # -------------------------
    # Trend/averages: max 10
    # -------------------------
    trend = 0.0
    reasons_trend = []
    if ind1.get("ema_fast_reclaim"):
        trend += 4; reasons_trend.append("استعادة/فقدان EMA7")
    if ind3.get("ema_alignment"):
        trend += 3; reasons_trend.append("EMA7/25 متوافق")
    if ind15.get("ema_context"):
        trend += 2; reasons_trend.append("سياق EMA50 داعم")
    if ind15.get("major_context"):
        trend += 1; reasons_trend.append("سياق EMA200 داعم")
    elif ALLOW_COUNTER_TREND:
        trend -= 1
    trend = clamp(trend, 0, WEIGHT_TREND)

    total = clamp(location + flow + candles + momentum + trend)
    extension = min(
        m1.get("move_from_pivot_atr",99),
        m3.get("move_from_pivot_atr",99),
    )

    # Strong conflicts deduct, not automatic rejection
    conflict = 0.0
    if ob.get("imbalance",50) < 42:
        conflict += 5
    if m1.get("delta_accel",50) < 42:
        conflict += 5
    if extension > MAX_SIGNAL_EXTENSION_ATR:
        conflict += min(20, (extension-MAX_SIGNAL_EXTENSION_ATR)*25)
    total = clamp(total - conflict)

    stage = None
    # Balanced requirements: one location clue + one flow clue, but not every condition
    if location >= MIN_LOCATION_POINTS and flow >= MIN_FLOW_POINTS:
        if total >= ENTRY_SCORE and candles >= 6 and extension <= MAX_SIGNAL_EXTENSION_ATR:
            stage = "EXPLOSION"
        elif total >= READY_SCORE:
            stage = "CONFIRMED"
        elif total >= WATCH_SCORE:
            stage = "EARLY"

    return {
        "stage": stage,
        "total": total,
        "location": location,
        "flow": flow,
        "candles": candles,
        "momentum": momentum,
        "trend": trend,
        "conflict": conflict,
        "extension_atr": extension,
        "reasons": reasons_location + reasons_flow + reasons_candles + reasons_momentum + reasons_trend,
    }


def reversal_features(rows: list[list[Any]], direction: str) -> dict[str, Any]:
    d = _ohlcv(rows)
    if len(d["c"]) < 12:
        return {}
    a = atr(rows)
    o,h,l,c,v,tb = d["o"],d["h"],d["l"],d["c"],d["v"],d["tb"]
    price = c[-1]
    avg_v = sum(v[-12:-1]) / max(1, len(v[-12:-1]))
    avg_body = sum(abs(c[i]-o[i]) for i in range(len(c)-10, len(c)-1)) / 9
    body = abs(c[-1]-o[-1])
    rng = max(h[-1]-l[-1], 1e-12)
    upper_wick = h[-1]-max(o[-1],c[-1])
    lower_wick = min(o[-1],c[-1])-l[-1]

    # delta series from taker buy base volume
    deltas = [(2*tb[i]-v[i]) for i in range(len(v))]
    prev_delta = sum(deltas[-5:-2])
    now_delta = sum(deltas[-2:])
    signed_prev = prev_delta if direction=="BUY" else -prev_delta
    signed_now = now_delta if direction=="BUY" else -now_delta
    delta_flip = signed_prev < 0 and signed_now > 0
    delta_accel = clamp(50 + safe_div(signed_now-signed_prev, max(sum(v[-5:]),1),0)*500)

    recent_high = max(h[-8:-1])
    recent_low = min(l[-8:-1])
    if direction == "BUY":
        sweep = l[-1] < recent_low and c[-1] > recent_low
        rejection = lower_wick/rng >= 0.42 and c[-1] >= o[-1]
        engulf = c[-1] > o[-1] and c[-1] >= o[-2] and o[-1] <= c[-2]
        micro_break = c[-1] > max(h[-4:-1])
        structure = (l[-1] >= min(l[-4:-1])) and (c[-1] > c[-2])
        pivot = min(l[-8:])
        move_from_pivot_atr = safe_div(price-pivot, a, 99)
    else:
        sweep = h[-1] > recent_high and c[-1] < recent_high
        rejection = upper_wick/rng >= 0.42 and c[-1] <= o[-1]
        engulf = c[-1] < o[-1] and c[-1] <= o[-2] and o[-1] >= c[-2]
        micro_break = c[-1] < min(l[-4:-1])
        structure = (h[-1] <= max(h[-4:-1])) and (c[-1] < c[-2])
        pivot = max(h[-8:])
        move_from_pivot_atr = safe_div(pivot-price, a, 99)

    high_volume_small_range = v[-1] >= avg_v*1.35 and rng <= max(a*0.8, avg_body*1.4)
    exhaustion = clamp(
        (100 if high_volume_small_range else 35)
        + (20 if sweep else 0)
        + (15 if rejection else 0)
    )

    return {
        "price": price,
        "atr": a,
        "delta_flip": delta_flip,
        "delta_accel": delta_accel,
        "sweep": sweep,
        "rejection": rejection,
        "engulf": engulf,
        "micro_break": micro_break,
        "structure": structure,
        "absorption_candle": high_volume_small_range,
        "exhaustion": clamp(exhaustion),
        "move_from_pivot_atr": move_from_pivot_atr,
        "pivot": pivot,
        "volume_ratio": safe_div(v[-1], avg_v, 1),
    }


def zone_context(direction: str, price: float, a: float, k15, k1, k4) -> dict[str, Any]:
    fvg15 = detect_fvg(k15, direction)
    ob15 = detect_order_block(k15, direction)
    ob1 = detect_order_block(k1, direction)
    d15 = _ohlcv(k15)
    d1 = _ohlcv(k1)
    d4 = _ohlcv(k4)

    swing15_low, swing15_high = min(d15["l"][-20:]), max(d15["h"][-20:])
    swing1_low, swing1_high = min(d1["l"][-20:]), max(d1["h"][-20:])
    swing4_low, swing4_high = min(d4["l"][-20:]), max(d4["h"][-20:])

    if direction == "BUY":
        dist15 = safe_div(price-swing15_low, a, 99)
        dist1 = safe_div(price-swing1_low, a, 99)
        dist4 = safe_div(price-swing4_low, a, 99)
    else:
        dist15 = safe_div(swing15_high-price, a, 99)
        dist1 = safe_div(swing1_high-price, a, 99)
        dist4 = safe_div(swing4_high-price, a, 99)

    near_structure = min(dist15, dist1, dist4)
    ob_near = min(ob15["distance_atr"], ob1["distance_atr"])
    fvg_near = fvg15["distance_atr"]

    zone_score = clamp(
        42
        + max(0, 1.2-near_structure)*24
        + max(0, 1.0-ob_near)*18
        + max(0, 0.9-fvg_near)*12
    )
    labels = []
    if near_structure <= 0.75: labels.append("قرب دعم/مقاومة وسيولة مهمة")
    if ob_near <= 0.75: labels.append("داخل أو قرب Order Block")
    if fvg_near <= 0.65: labels.append("داخل أو قرب FVG")
    return {
        "zone_score": zone_score,
        "near_structure_atr": near_structure,
        "ob_distance_atr": ob_near,
        "fvg_distance_atr": fvg_near,
        "labels": labels,
        "ob15": ob15,
        "ob1h": ob1,
        "fvg15": fvg15,
    }


def reversal_stage(zone: dict, m1: dict, m3: dict, ob: dict, smart: dict) -> tuple[str | None, float, float, float]:
    location = zone["zone_score"]
    exhaustion = 0.55*m1.get("exhaustion",0) + 0.45*m3.get("exhaustion",0)
    flow = clamp(
        0.30*m1.get("delta_accel",0)
        + 0.20*m3.get("delta_accel",0)
        + 0.22*ob.get("imbalance",50)
        + 0.15*ob.get("absorption",35)
        + 0.13*smart.get("smart_score",50)
        + (10 if m1.get("delta_flip") else 0)
        + (8 if m3.get("delta_flip") else 0)
    )
    trigger = clamp(
        38
        + (18 if m1.get("micro_break") else 0)
        + (14 if m3.get("micro_break") else 0)
        + (12 if m1.get("engulf") else 0)
        + (10 if m1.get("sweep") else 0)
        + (8 if m1.get("rejection") else 0)
    )

    late = min(m1.get("move_from_pivot_atr",99), m3.get("move_from_pivot_atr",99)) > MAX_LATE_MOVE_ATR
    if late:
        return None, location, flow, trigger

    if location >= REVERSAL_ZONE_SCORE and flow >= FLOW_FLIP_SCORE and trigger >= ENTRY_TRIGGER_SCORE:
        return "EXPLOSION", location, flow, trigger
    if location >= REVERSAL_ZONE_SCORE and flow >= FLOW_FLIP_SCORE:
        return "CONFIRMED", location, flow, trigger
    if location >= REVERSAL_ZONE_SCORE and exhaustion >= 62:
        return "EARLY", location, flow, trigger
    return None, location, flow, trigger


def build_trade_plan(direction: str, price: float, a: float, swing_low: float, swing_high: float, stage: str):
    zone = max(a * (0.18 if stage == "EXPLOSION" else 0.28), price * 0.0008)
    if direction == "BUY":
        entry_low, entry_high = price - zone, price + zone * 0.25
        structural = min(swing_low, entry_low - a * 0.75)
        stop = structural - a * 0.15
        risk = max(((entry_low + entry_high)/2) - stop, a * 0.65)
        mid = (entry_low + entry_high)/2
        tp1, tp2, tp3 = mid+risk, mid+2*risk, mid+3*risk
    else:
        entry_low, entry_high = price - zone * 0.25, price + zone
        structural = max(swing_high, entry_high + a * 0.75)
        stop = structural + a * 0.15
        risk = max(stop - ((entry_low + entry_high)/2), a * 0.65)
        mid = (entry_low + entry_high)/2
        tp1, tp2, tp3 = mid-risk, mid-2*risk, mid-3*risk

    rr = lambda tp: abs(tp-mid)/max(abs(mid-stop), 1e-12)
    return entry_low, entry_high, stop, tp1, tp2, tp3, rr(tp1), rr(tp2), rr(tp3)


def choose_stage(scores: dict[str,float], f15: dict, f1: dict, f4: dict) -> str | None:
    """
    EARLY: قبل الكسر، يعتمد على التدفق والضغط فقط.
    CONFIRMED: اقتراب من الكسر مع استمرار 15m و1h.
    EXPLOSION: بداية الكسر، بشرط ألا تكون الحركة ممتدة.
    """
    s15, s1, s4 = scores["15m"], scores["1h"], scores["4h"]
    breakout = f15["is_breakout"] or f1["is_breakout"]
    near_break = f15["near_break"] or f1["near_break"]
    volume_expansion = max(f15["vol_ratio"], f1["vol_ratio"]) >= 1.22

    explosion = 0.52*s15 + 0.36*s1 + 0.12*s4
    confirmed = 0.46*s15 + 0.42*s1 + 0.12*s4
    early = 0.72*s15 + 0.20*s1 + 0.08*s4

    # لا نطارد شمعة تحركت كثيرًا بالفعل.
    extension_atr = max(f15.get("extension_atr", 0.0), f1.get("extension_atr", 0.0))
    if breakout and volume_expansion and explosion >= EXPLOSION_SCORE and extension_atr <= MAX_EXTENSION_ATR:
        return "EXPLOSION"

    if confirmed >= CONFIRMED_SCORE and s15 >= 69 and s1 >= 67 and (near_break or volume_expansion):
        return "CONFIRMED"

    # لا يوجد شرط كسر في المرحلة المبكرة.
    if early >= EARLY_SCORE and s15 >= EARLY_SCORE:
        return "EARLY"

    return None



def trend_slope(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    n = len(values)
    x_mean = (n - 1) / 2
    y_mean = sum(values) / n
    num = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values))
    den = sum((i - x_mean) ** 2 for i in range(n))
    return num / den if den else 0.0


def persistence_score(history: list[dict], direction: str) -> dict[str, float]:
    if len(history) < 2:
        return {
            "persistence": 0.0,
            "oi_accel": 50.0,
            "book_persistence": 50.0,
            "delta_persistence": 50.0,
            "compression_persistence": 50.0,
            "smart_score": 50.0,
        }

    sign = 1 if direction == "BUY" else -1
    oi_vals = [float(x.get("oi_change", 0.0)) for x in history]
    book_vals = [float(x.get("imbalance_raw", 0.0)) * sign for x in history]
    delta_vals = [float(x.get("delta_strength", 50.0)) for x in history]
    compression_vals = [float(x.get("compression", 50.0)) for x in history]

    positive_book = sum(v > 0.04 for v in book_vals) / len(book_vals)
    positive_delta = sum(v >= 56 for v in delta_vals) / len(delta_vals)
    compressed = sum(v >= 55 for v in compression_vals) / len(compression_vals)
    oi_positive = sum(v > 0.05 for v in oi_vals) / len(oi_vals)

    persistence = 100 * (
        0.30 * positive_book
        + 0.30 * positive_delta
        + 0.20 * compressed
        + 0.20 * oi_positive
    )

    oi_slope = trend_slope(oi_vals)
    book_slope = trend_slope(book_vals)
    delta_slope = trend_slope(delta_vals)

    oi_accel = clamp(50 + oi_slope * 35)
    book_persistence = clamp(50 + positive_book * 35 + book_slope * 120)
    delta_persistence = clamp(45 + positive_delta * 40 + delta_slope * 1.5)
    compression_persistence = clamp(40 + compressed * 45)

    smart_score = clamp(
        0.34 * persistence
        + 0.18 * oi_accel
        + 0.20 * book_persistence
        + 0.20 * delta_persistence
        + 0.08 * compression_persistence
    )

    return {
        "persistence": persistence,
        "oi_accel": oi_accel,
        "book_persistence": book_persistence,
        "delta_persistence": delta_persistence,
        "compression_persistence": compression_persistence,
        "smart_score": smart_score,
    }


def is_invalidated(sig: Signal) -> bool:
    d = sig.details
    ob = d.get("orderbook", {})
    f15 = d.get("features_15m", {})
    oi15 = d.get("oi_15m", {})
    if sig.direction == "BUY":
        book_bad = ob.get("imbalance_raw", 0) < -0.08
        delta_bad = f15.get("delta_strength", 50) < 42
    else:
        book_bad = ob.get("imbalance_raw", 0) > 0.08
        delta_bad = f15.get("delta_strength", 50) < 42
    oi_bad = oi15.get("oi_change", 0) < -0.35
    return sum((book_bad, delta_bad, oi_bad)) >= 2



RECIPE_LIBRARY = [
    {"id":"REV-01","type":"REVERSAL","name":"Liquidity Sweep + Delta Flip",
     "required":["sweep","delta_flip","location"],
     "optional":["rejection","orderbook","rsi_div","absorption"],"base":58},

    {"id":"REV-02","type":"REVERSAL","name":"Bollinger Reversal",
     "required":["bollinger","momentum_turn","location"],
     "optional":["delta_flip","rsi_div","rejection","absorption"],"base":56},

    {"id":"REV-03","type":"REVERSAL","name":"Order Block Rejection",
     "required":["order_block","location","rejection"],
     "optional":["delta_flip","engulf","orderbook","micro_bos"],"base":58},

    {"id":"REV-04","type":"REVERSAL","name":"Absorption Reversal",
     "required":["absorption","location","flow_shift"],
     "optional":["sweep","rsi_div","macd_turn","rejection"],"base":57},

    {"id":"CONT-01","type":"CONTINUATION","name":"EMA Pullback Continuation",
     "required":["ema_pullback","trend_context","flow_shift"],
     "optional":["order_block","fvg","micro_bos","volume"],"base":56},

    {"id":"CONT-02","type":"CONTINUATION","name":"FVG Continuation",
     "required":["fvg","trend_context","delta_support"],
     "optional":["ema_alignment","micro_bos","orderbook","volume"],"base":57},

    {"id":"CONT-03","type":"CONTINUATION","name":"Order Block Continuation",
     "required":["order_block","trend_context","delta_support"],
     "optional":["engulf","micro_bos","orderbook","volume"],"base":58},

    {"id":"EXP-01","type":"BREAKOUT","name":"Compression Breakout",
     "required":["compression","oi_rise","volume"],
     "optional":["orderbook","delta_support","micro_bos","macd_turn"],"base":57},

    {"id":"EXP-02","type":"BREAKOUT","name":"Order Flow Ignition",
     "required":["flow_shift","volume","micro_bos"],
     "optional":["oi_rise","orderbook","compression","ema_alignment"],"base":60},
]


def build_evidence(direction, zone, m1, m3, ind1, ind3, ind15, ob, oi15, smart, c15):
    location = (
        zone.get("near_structure_atr", 99) <= 1.05
        or zone.get("ob_distance_atr", 99) <= 1.0
        or zone.get("fvg_distance_atr", 99) <= 0.90
        or ind15.get("bb_touch", False)
    )

    return {
        "location": location,
        "order_block": zone.get("ob_distance_atr", 99) <= 1.0,
        "fvg": zone.get("fvg_distance_atr", 99) <= 0.90,
        "bollinger": ind15.get("bb_touch", False) or ind1.get("bb_reclaim", False),
        "sweep": bool(m1.get("sweep") or m3.get("sweep")),
        "rejection": bool(m1.get("rejection") or m3.get("rejection")),
        "engulf": bool(m1.get("engulf") or m3.get("engulf")),
        "micro_bos": bool(m1.get("micro_break") or m3.get("micro_break")),
        "delta_flip": bool(m1.get("delta_flip") or m3.get("delta_flip")),
        "flow_shift": (
            m1.get("delta_flip", False)
            or m3.get("delta_flip", False)
            or m1.get("delta_accel", 50) >= 54
            or ob.get("imbalance", 50) >= 55
        ),
        "delta_support": m1.get("delta_accel", 50) >= 53,
        "orderbook": ob.get("imbalance", 50) >= 55,
        "absorption": ob.get("absorption", 35) >= 52 or m1.get("absorption_candle", False),
        "rsi_div": ind1.get("rsi_divergence", False),
        "macd_turn": ind1.get("macd_turn", False) or ind3.get("macd_turn", False),
        "momentum_turn": (
            ind1.get("rsi_divergence", False)
            or ind1.get("rsi_turn", False)
            or ind1.get("macd_turn", False)
            or ind3.get("momentum_turn", False)
        ),
        "ema_alignment": ind3.get("ema_alignment", False),
        "trend_context": (
            ind15.get("ema_context", False)
            or ind15.get("major_context", False)
            or ind3.get("ema_alignment", False)
        ),
        "ema_pullback": abs(
            m1.get("price", 0) - ind3.get("ema25", m1.get("price", 0))
        ) <= max(m3.get("atr", 0) * 0.75, 1e-12),
        "volume": (
            ind1.get("volume_expansion", False)
            or ind3.get("volume_expansion", False)
            or c15.get("volume", 0) >= 52
        ),
        "compression": c15.get("compression", 0) >= 52,
        "oi_rise": oi15.get("oi_change", 0) > 0.03,
    }


def evaluate_recipes(evidence, extension_atr, conflict_penalty=0.0):
    results = []

    for recipe in RECIPE_LIBRARY:
        required_hits = sum(bool(evidence.get(x)) for x in recipe["required"])
        optional_hits = sum(bool(evidence.get(x)) for x in recipe["optional"])

        # مرن ومتوازن:
        # شرطان أساسيان، أو شرط أساسي واحد مع دليلين اختياريين.
        if required_hits < 2 and not (
            required_hits >= 1 and optional_hits >= 2
        ):
            continue

        score = float(recipe["base"])
        score += required_hits * 5
        score += optional_hits * 3
        score -= (len(recipe["required"]) - required_hits) * 4
        score -= conflict_penalty

        if extension_atr > MAX_RECIPE_EXTENSION_ATR:
            score -= min(25, (extension_atr - MAX_RECIPE_EXTENSION_ATR) * 30)

        results.append({
            "id": recipe["id"],
            "type": recipe["type"],
            "name": recipe["name"],
            "score": clamp(score),
            "required_hits": required_hits,
            "optional_hits": optional_hits,
            "matches": required_hits + optional_hits,
            "evidence": [k for k, v in evidence.items() if v],
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results


async def recipe_learning_weight(recipe_id):
    async with aiosqlite.connect(DB_PATH) as db:
        row = await (await db.execute(
            "SELECT cases,wins,losses,tp1_hits,learned_weight FROM recipe_stats WHERE recipe_id=?",
            (recipe_id,)
        )).fetchone()

    if not row:
        return 0.0

    cases, wins, losses, tp1_hits, stored = row
    if cases < LEARNING_MIN_CASES:
        return float(stored or 0)

    hit_rate = safe_div(tp1_hits, cases, 0)
    win_rate = safe_div(wins, max(wins + losses, 1), 0)
    learned = (hit_rate - 0.5) * 12 + (win_rate - 0.5) * 8
    return clamp(learned, -LEARNING_WEIGHT_MAX, LEARNING_WEIGHT_MAX)


async def save_rejection(symbol, direction, recipe_id, score, reason, details):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO rejected_signals
            (created_at,symbol,direction,recipe_id,score,reason,details_json)
            VALUES (?,?,?,?,?,?,?)""",
            (
                now_local().isoformat(),
                symbol,
                direction,
                recipe_id,
                score,
                reason,
                json.dumps(details),
            )
        )
        await db.commit()



def classify_market_regime(c15: dict, c1h: dict, c4h: dict, ind15: dict) -> str:
    trend_strength = (
        0.45 * c1h.get("trend", 50)
        + 0.35 * c4h.get("trend", 50)
        + 0.20 * (70 if ind15.get("ema_alignment") else 40)
    )
    compression = c15.get("compression", 0)
    volume = c15.get("volume", 0)

    if compression >= BREAKOUT_COMPRESSION and volume <= 60:
        return "COMPRESSED"

    if trend_strength >= TREND_ADX_PROXY:
        return "TRENDING"

    if compression >= RANGE_COMPRESSION:
        return "RANGING"

    if c15.get("cvd_divergence", 0) >= 80 and c15.get("delta_strength", 50) < 55:
        return "EXHAUSTED"

    return "MIXED"


def adaptive_strategy_score(
    regime: str,
    evidence: dict,
    zone: dict,
    m1: dict,
    m3: dict,
    ind1: dict,
    ind3: dict,
    ind15: dict,
    ob: dict,
    oi15: dict,
    c15: dict,
) -> dict:
    scores = {}

    # 1) Trend Pullback Continuation
    pullback = 0.0
    if regime == "TRENDING": pullback += 18
    if evidence.get("trend_context"): pullback += 14
    if evidence.get("ema_pullback"): pullback += 14
    if evidence.get("order_block") or evidence.get("fvg"): pullback += 10
    if evidence.get("delta_support") or evidence.get("flow_shift"): pullback += 14
    if evidence.get("micro_bos"): pullback += 12
    if evidence.get("volume"): pullback += 8
    if evidence.get("rejection") or evidence.get("engulf"): pullback += 6
    scores["TREND_PULLBACK"] = clamp(pullback)

    # 2) Liquidity Sweep Reversal
    reversal = 0.0
    if regime in ("RANGING", "EXHAUSTED", "MIXED"): reversal += 10
    if evidence.get("sweep"): reversal += 20
    if evidence.get("location"): reversal += 12
    if evidence.get("delta_flip"): reversal += 16
    if evidence.get("rejection") or evidence.get("engulf"): reversal += 12
    if evidence.get("rsi_div"): reversal += 10
    if evidence.get("absorption"): reversal += 10
    if evidence.get("orderbook"): reversal += 6
    scores["LIQUIDITY_REVERSAL"] = clamp(reversal)

    # 3) Compression Breakout
    breakout = 0.0
    if regime == "COMPRESSED": breakout += 20
    if evidence.get("compression"): breakout += 16
    if evidence.get("oi_rise"): breakout += 14
    if evidence.get("volume"): breakout += 14
    if evidence.get("flow_shift") or evidence.get("delta_support"): breakout += 14
    if evidence.get("orderbook"): breakout += 10
    if evidence.get("micro_bos"): breakout += 12
    scores["COMPRESSION_BREAKOUT"] = clamp(breakout)

    # 4) Absorption Reversal
    absorption = 0.0
    if regime in ("EXHAUSTED", "RANGING", "MIXED"): absorption += 10
    if evidence.get("absorption"): absorption += 22
    if evidence.get("location"): absorption += 12
    if evidence.get("delta_flip") or evidence.get("flow_shift"): absorption += 14
    if evidence.get("rejection"): absorption += 12
    if evidence.get("rsi_div") or evidence.get("macd_turn"): absorption += 10
    if evidence.get("orderbook"): absorption += 8
    if evidence.get("sweep"): absorption += 8
    scores["ABSORPTION_REVERSAL"] = clamp(absorption)

    best_name = max(scores, key=scores.get)
    best_score = scores[best_name]

    thresholds = {
        "TREND_PULLBACK": PULLBACK_MIN_SCORE,
        "LIQUIDITY_REVERSAL": LIQUIDITY_REVERSAL_MIN_SCORE,
        "COMPRESSION_BREAKOUT": BREAKOUT_MIN_SCORE,
        "ABSORPTION_REVERSAL": ABSORPTION_REVERSAL_MIN_SCORE,
    }

    return {
        "regime": regime,
        "scores": scores,
        "strategy": best_name,
        "strategy_score": best_score,
        "threshold": thresholds[best_name],
        "accepted": best_score >= thresholds[best_name],
    }


# =========================================================
# تيليجرام
# =========================================================

async def send_telegram(session: aiohttp.ClientSession, text: str) -> bool:
    if not BOT_TOKEN or not CHAT_ID:
        log.warning("Telegram variables are missing")
        return False
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        async with session.post(url, json=payload, timeout=20) as r:
            body = await r.text()
            if r.status != 200:
                log.error("Telegram %s: %s", r.status, body)
                return False
            return True
    except Exception:
        log.exception("Telegram send failed")
        return False


def signal_message(sig: Signal) -> str:
    stage_title = {
        "EARLY": "🟡 رصد منطقة انعكاس",
        "CONFIRMED": "🟠 انقلاب التدفق مؤكد",
        "EXPLOSION": "🔥 دخول الآن — بداية الانعكاس",
    }[sig.stage]
    side = "شراء" if sig.direction == "BUY" else "بيع"
    checks = "\n".join(f"✅ {html.escape(x)}" for x in sig.recipe[:6])
    action = {
        "EARLY": "👀 الحالة: السعر داخل منطقة انعكاس — ننتظر انقلاب السيطرة",
        "CONFIRMED": "📍 الحالة: Delta/Order Flow انقلبا — دخول مبكر مقترح",
        "EXPLOSION": "⚡ الحالة: Trigger صغير تحقق والحركة لم تمتد بعد",
    }[sig.stage]
    historical = "يتعلم"
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{sig.symbol}.P"
    bn = f"https://www.binance.com/en/futures/{sig.symbol}"
    ts = now_local().strftime("%d-%m-%Y %H:%M:%S")

    return f"""<b>{stage_title} — {side}</b>

💰 العملة: <b>#{sig.symbol}.P</b>
⏰ التوقيت: 1M / 3M داخليًا — السياق 15M / 1H / 4H
💵 السعر: <b>{fmt_price(sig.price)}</b>

🧠 درجة الانفجار: <b>{sig.explosion_score:.1f}%</b>
🎯 جودة الدخول: <b>{sig.entry_score:.1f}%</b>
🛡️ درجة الأمان: <b>{sig.safety_score:.1f}%</b>
🧬 بصمة الأموال الذكية: <b>{sig.details.get("smart_persistence", {}).get("smart_score", 0):.1f}%</b>
📍 درجة الموقع: <b>{sig.details.get("reversal", {}).get("location_score", 0):.1f}%</b>
⚡ انقلاب التدفق: <b>{sig.details.get("reversal", {}).get("flow_score", 0):.1f}%</b>
🔑 Trigger الدخول: <b>{sig.details.get("reversal", {}).get("trigger_score", 0):.1f}%</b>
📏 امتداد الحركة: <b>{sig.details.get("reversal", {}).get("late_atr", 0):.2f} ATR</b>
🧩 الوصفة: <b>{sig.details.get("recipe", {}).get("id", "N/A")}</b>
🧠 نوع السيناريو: <b>{sig.details.get("recipe", {}).get("type", "N/A")}</b>
🌦️ حالة السوق: <b>{sig.details.get("adaptive_strategy", {}).get("regime", "N/A")}</b>
🧭 الاستراتيجية: <b>{sig.details.get("adaptive_strategy", {}).get("strategy", "N/A")}</b>
📊 درجة الاستراتيجية: <b>{sig.details.get("adaptive_strategy", {}).get("strategy_score", 0):.1f}%</b>
🗳️ درجة الوصفة: <b>{sig.details.get("recipe", {}).get("score", 0):.1f}%</b>
📚 الاحتمال التاريخي: <b>{historical}</b>

📊 توافق الفريمات:
15M: {sig.scores_by_tf['15m']:.1f}%
1H: {sig.scores_by_tf['1h']:.1f}%
4H: {sig.scores_by_tf['4h']:.1f}%

🎯 منطقة الدخول: <b>{fmt_price(sig.entry_low)} – {fmt_price(sig.entry_high)}</b>
🛑 وقف الخسارة: <b>{fmt_price(sig.stop)}</b>
✅ الهدف الأول: <b>{fmt_price(sig.tp1)}</b> ({sig.rr1:.1f}R)
✅ الهدف الثاني: <b>{fmt_price(sig.tp2)}</b> ({sig.rr2:.1f}R)
✅ الهدف الثالث: <b>{fmt_price(sig.tp3)}</b> ({sig.rr3:.1f}R)

{checks}

{action}

🕒 {ts} (السعودية)
🔗 <a href="{bn}">Binance</a> | <a href="{tv}">TradingView</a>

⚠️ خطة إحصائية مقترحة وليست ضمانًا أو تنفيذًا تلقائيًا."""


# =========================================================
# محرك المتابعة
# =========================================================

def fast_market_score(ticker: dict, prev: dict | None) -> tuple[float, dict[str, float]]:
    price = float(ticker.get("lastPrice", 0) or 0)
    high = float(ticker.get("highPrice", price) or price)
    low = float(ticker.get("lowPrice", price) or price)
    open_price = float(ticker.get("openPrice", price) or price)
    quote_volume = float(ticker.get("quoteVolume", 0) or 0)
    base_volume = float(ticker.get("volume", 0) or 0)
    trades = float(ticker.get("count", 0) or 0)
    change_24h = float(ticker.get("priceChangePercent", 0) or 0)

    price_burst = volume_burst = trade_burst = 0.0
    if prev:
        price_burst = abs(pct_change(price, prev.get("price", price)))
        volume_burst = max(0.0, pct_change(quote_volume, prev.get("quote_volume", quote_volume)))
        trade_burst = max(0.0, pct_change(trades, prev.get("trades", trades)))

    day_range = max(high-low, price*1e-9)
    range_position = safe_div(price-low, day_range, 0.5)
    edge_pressure = abs(range_position-0.5) * 2
    day_move = abs(pct_change(price, open_price))
    liquidity = clamp((math.log10(max(quote_volume, 1)) - 5.4) * 22)

    # الرادار لا يبحث عن الاتجاه فقط؛ يبحث عن تسارع/حجم/تداول غير طبيعي.
    score = clamp(
        0.30 * clamp(price_burst * 1100)
        + 0.27 * clamp(volume_burst * 12)
        + 0.20 * clamp(trade_burst * 10)
        + 0.10 * liquidity
        + 0.08 * clamp(day_move * 8)
        + 0.05 * clamp(edge_pressure * 100)
    )

    return score, {
        "price": price,
        "quote_volume": quote_volume,
        "base_volume": base_volume,
        "trades": trades,
        "price_burst": price_burst,
        "volume_burst": volume_burst,
        "trade_burst": trade_burst,
        "range_position": range_position,
        "day_move": day_move,
        "fast_score": score,
    }


def update_heat(previous_heat: float, fast_score: float, state: dict[str, float]) -> float:
    impulse = (
        fast_score
        + clamp(state.get("price_burst", 0) * 900) * 0.20
        + clamp(state.get("volume_burst", 0) * 10) * 0.15
        + clamp(state.get("trade_burst", 0) * 8) * 0.10
    )
    return clamp(previous_heat * RADAR_HEAT_DECAY + impulse * (1 - RADAR_HEAT_DECAY))




class Engine:
    def __init__(self):
        self.client = BinanceClient()
        self.telegram_session: aiohttp.ClientSession | None = None
        self.running = True
        self.scan_no = 0
        self.last_scan = None
        self.last_error = None
        self.symbol_count = 0
        self.candidate_count = 0
        self.alert_count = 0
        self.stage_streaks: dict[tuple[str, str, str], int] = {}
        self.last_stage_seen: dict[tuple[str, str], str] = {}
        self.market_history: dict[tuple[str, str], deque] = {}
        self.invalidation_streaks: dict[tuple[str, str], int] = {}
        self.fast_state: dict[str, dict[str, float]] = {}
        self.fast_top: list[dict[str, Any]] = []
        self.heat_map: dict[str, float] = {}
        self.hot_watchlist: dict[str, int] = {}

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()
        if SEND_STARTUP_MESSAGE:
            await send_telegram(
                self.telegram_session,
                "✅ <b>Ahmed Early Reversal AI v9 ADAPTIVE بدأ العمل</b>\n\n"
                "🧭 الاستراتيجيات:\n"
                "• Trend Pullback\n"
                "• Liquidity Reversal\n"
                "• Compression Breakout\n"
                "• Absorption Reversal\n\n"
                "⏰ التوقيت: 1M / 3M داخليًا\n"
                "📊 السياق: 15M / 1H / 4H\n"
                "⚠️ لا ينفذ صفقات تلقائيًا."
            )

        if SEND_TEST_MESSAGE:
            await send_telegram(
                self.telegram_session,
                "🧪 <b>رسالة اختبار ناجحة</b>\n\n"
                "✅ Telegram متصل\n"
                "✅ Railway يعمل\n"
                "✅ محرك v9 Adaptive جاهز\n"
                f"🕒 {now_local().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)"
            )
        asyncio.create_task(self.loop())
        asyncio.create_task(self.track_open_positions())

    async def close(self):
        self.running = False
        await self.client.close()
        if self.telegram_session:
            await self.telegram_session.close()

    async def loop(self):
        while self.running:
            started = time.monotonic()
            self.scan_no += 1
            alerts = 0
            analyzed = 0
            error = None
            try:
                alerts, analyzed = await self.scan()
                self.last_error = None
            except Exception as e:
                error = repr(e)
                self.last_error = error
                log.exception("Scan failed")
            elapsed = time.monotonic() - started
            self.last_scan = now_local().isoformat()
            await record_checkpoint(
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed, error
            )
            log.info(
                "scan=%s symbols=%s candidates=%s analyzed=%s alerts=%s seconds=%.1f",
                self.scan_no, self.symbol_count, self.candidate_count,
                analyzed, alerts, elapsed
            )
            await asyncio.sleep(max(5, SCAN_SECONDS - elapsed))

    async def scan(self) -> tuple[int,int]:
        symbols_task = asyncio.create_task(self.client.symbols())
        tickers_task = asyncio.create_task(self.client.tickers())
        symbols, tickers = await asyncio.gather(symbols_task, tickers_task)

        self.symbol_count = len(symbols)
        allowed = set(symbols)
        radar_ranked = []
        new_state = {}

        for ticker in tickers:
            symbol = ticker.get("symbol")
            if symbol not in allowed:
                continue

            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < MIN_QUOTE_VOLUME:
                continue

            fast_score, state = fast_market_score(
                ticker, self.fast_state.get(symbol)
            )
            heat = update_heat(
                self.heat_map.get(symbol, 0.0), fast_score, state
            )
            self.heat_map[symbol] = heat
            state["heat_score"] = heat
            new_state[symbol] = state

            # أولوية للعملة الساخنة حتى لو هبطت قراءة لحظية واحدة.
            watch_bonus = 12 if self.hot_watchlist.get(symbol, 0) > 0 else 0
            radar_score = clamp(
                0.58 * heat
                + 0.32 * fast_score
                + 0.10 * clamp(
                    state.get("volume_burst", 0) * 12
                    + state.get("trade_burst", 0) * 8
                )
                + watch_bonus
            )
            radar_ranked.append(
                (radar_score, heat, quote_volume, symbol, state)
            )

        self.fast_state.update(new_state)

        # خفض مدة بقاء قائمة المراقبة الديناميكية.
        for symbol in list(self.hot_watchlist):
            self.hot_watchlist[symbol] -= 1
            if self.hot_watchlist[symbol] <= 0:
                del self.hot_watchlist[symbol]

        radar_ranked.sort(
            key=lambda x: (x[0], x[1], x[2]), reverse=True
        )

        # في أول دورتين نخلط الأعلى سيولة مع الأعلى حرارة.
        if self.scan_no <= 2:
            liquid = sorted(
                radar_ranked, key=lambda x: x[2], reverse=True
            )[:RADAR_DEEP_CANDIDATES // 2]
            hot = radar_ranked[:RADAR_DEEP_CANDIDATES]
            merged = []
            seen = set()
            for item in hot + liquid:
                if item[3] not in seen:
                    seen.add(item[3])
                    merged.append(item)
            selected = merged[:RADAR_DEEP_CANDIDATES]
        else:
            pool = [
                item for item in radar_ranked[:RADAR_POOL]
                if item[1] >= RADAR_MIN_HEAT
                or item[3] in self.hot_watchlist
            ]
            if len(pool) < RADAR_DEEP_CANDIDATES:
                pool = radar_ranked[:RADAR_POOL]
            selected = pool[:RADAR_DEEP_CANDIDATES]

        candidates = [x[3] for x in selected]
        self.candidate_count = len(candidates)
        self.fast_top = [
            {
                "symbol": x[3],
                "radar_score": round(x[0], 2),
                "heat_score": round(x[1], 2),
                **x[4],
            }
            for x in selected[:30]
        ]

        tasks = [
            asyncio.create_task(self.analyze_symbol(symbol))
            for symbol in candidates
        ]

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=max(12, SCAN_SECONDS * 1.8),
            )
        except asyncio.TimeoutError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            results = [
                task.result()
                if task.done() and not task.cancelled()
                else RuntimeError("deep timeout")
                for task in tasks
            ]
            log.warning("Radar deep scan timeout; unfinished tasks cancelled")

        alerts = 0
        analyzed = 0

        for symbol, result in zip(candidates, results):
            if isinstance(result, Exception):
                log.debug("symbol analysis failed: %r", result)
                continue

            analyzed += 1

            if result:
                self.hot_watchlist[symbol] = RADAR_HOT_KEEP

            for sig in result:
                inv_key = (sig.symbol, sig.direction)

                if is_invalidated(sig):
                    self.invalidation_streaks[inv_key] = (
                        self.invalidation_streaks.get(inv_key, 0) + 1
                    )
                else:
                    self.invalidation_streaks[inv_key] = 0

                if (
                    self.invalidation_streaks[inv_key]
                    >= INVALIDATION_STREAK
                ):
                    continue

                _, changed = await save_signal(sig)

                if changed:
                    ok = await send_telegram(
                        self.telegram_session,
                        signal_message(sig),
                    )
                    alerts += int(ok)
                    self.alert_count += int(ok)

        return alerts, analyzed

    async def analyze_symbol(self, symbol: str) -> list[Signal]:
        k1m, k3m, k15, k1, k4, oi15, oi1, depth, premium = await asyncio.gather(
            self.client.klines(symbol, "1m"),
            self.client.klines(symbol, "3m"),
            self.client.klines(symbol, "15m"),
            self.client.klines(symbol, "1h"),
            self.client.klines(symbol, "4h"),
            self.client.oi_hist(symbol, "15m"),
            self.client.oi_hist(symbol, "1h"),
            self.client.depth(symbol),
            self.client.premium(symbol),
        )

        signals = []

        for direction in ("BUY", "SELL"):
            c15 = candle_features(k15, direction)
            c1h = candle_features(k1, direction)
            c4h = candle_features(k4, direction)

            m1 = reversal_features(k1m, direction)
            m3 = reversal_features(k3m, direction)

            ind1 = indicator_features(k1m, direction)
            ind3 = indicator_features(k3m, direction)
            ind15 = indicator_features(k15, direction)

            if not m1 or not m3 or not ind1 or not ind3 or not ind15:
                continue

            ob = orderbook_features(depth, direction)
            o15 = oi_features(oi15, direction)
            o1 = oi_features(oi1, direction)
            fund = funding_features(premium, direction)

            hist_key = (symbol, direction)
            hist = self.market_history.setdefault(
                hist_key, deque(maxlen=STATE_HISTORY)
            )
            hist.append({
                "ts": time.time(),
                "oi_change": o15["oi_change"],
                "imbalance_raw": ob["imbalance_raw"],
                "delta_strength": m1.get("delta_accel", 50),
                "cvd_strength": c15["cvd_strength"],
                "compression": c15["compression"],
                "volume": c15["volume"],
                "price": m1["price"],
            })

            smart = persistence_score(list(hist), direction)
            zone = zone_context(
                direction, m1["price"], m1["atr"], k15, k1, k4
            )

            evidence = build_evidence(
                direction, zone, m1, m3, ind1, ind3, ind15,
                ob, o15, smart, c15
            )

            regime = classify_market_regime(c15, c1h, c4h, ind15)
            adaptive = adaptive_strategy_score(
                regime, evidence, zone, m1, m3, ind1, ind3, ind15,
                ob, o15, c15
            )

            extension_atr = min(
                m1.get("move_from_pivot_atr", 99),
                m3.get("move_from_pivot_atr", 99),
            )

            conflict_penalty = 0.0
            if ob.get("imbalance", 50) < 42:
                conflict_penalty += 4
            if m1.get("delta_accel", 50) < 42:
                conflict_penalty += 4
            if ob.get("spoof_risk", 0) > 72:
                conflict_penalty += 3

            recipes = evaluate_recipes(
                evidence, extension_atr, conflict_penalty
            )

            if not adaptive["accepted"]:
                await save_rejection(
                    symbol,
                    direction,
                    None,
                    adaptive["strategy_score"],
                    "ADAPTIVE_STRATEGY_REJECTED",
                    adaptive,
                )
                continue

            if not recipes:
                continue

            best = recipes[0]
            best["score"] = clamp(
                0.60 * best["score"]
                + 0.40 * adaptive["strategy_score"]
                + await recipe_learning_weight(best["id"])
            )

            if best["matches"] < RECIPE_MIN_MATCHES:
                continue

            if extension_atr > MAX_RECIPE_EXTENSION_ATR:
                await save_rejection(
                    symbol, direction, best["id"], best["score"],
                    "LATE_MOVE",
                    {"extension_atr": extension_atr, "recipe": best},
                )
                continue

            if (
                best["score"] >= RADAR_ENTRY_SCORE
                and (
                    evidence.get("micro_bos")
                    or evidence.get("delta_flip")
                    or (
                        evidence.get("flow_shift")
                        and evidence.get("rejection")
                    )
                )
            ):
                stage = "EXPLOSION"
            elif best["score"] >= RADAR_READY_SCORE:
                stage = "CONFIRMED"
            elif best["score"] >= RADAR_WATCH_SCORE:
                stage = "EARLY"
            else:
                continue

            s15 = combine_score(c15, ob, o15, fund, "15m")
            s1 = combine_score(c1h, ob, o1, fund, "1h")
            s4 = combine_score(c4h, ob, o1, fund, "4h")

            plan = build_trade_plan(
                direction,
                m1["price"],
                max(m1["atr"], m3["atr"]),
                min(float(x[3]) for x in k3m[-12:]),
                max(float(x[2]) for x in k3m[-12:]),
                stage,
            )

            reasons = [
                x.replace("_", " ").title()
                for x in best["evidence"][:10]
            ]

            details = {
                "orderbook": ob,
                "oi_15m": o15,
                "oi_1h": o1,
                "funding": fund,
                "features_15m": c15,
                "features_1h": c1h,
                "features_4h": c4h,
                "features_1m": m1,
                "features_3m": m3,
                "indicators_1m": ind1,
                "indicators_3m": ind3,
                "indicators_15m": ind15,
                "smart_persistence": smart,
                "zone": zone,
                "recipe": best,
                "adaptive_strategy": adaptive,
                "balanced_vote": {"total": best["score"]},
                "reversal": {
                    "location_score": zone.get("zone_score", 0),
                    "flow_score": clamp(
                        0.50 * m1.get("delta_accel", 50)
                        + 0.30 * ob.get("imbalance", 50)
                        + 0.20 * smart.get("smart_score", 50)
                    ),
                    "trigger_score": clamp(
                        (30 if evidence.get("micro_bos") else 0)
                        + (25 if evidence.get("rejection") else 0)
                        + (20 if evidence.get("engulf") else 0)
                        + (15 if evidence.get("sweep") else 0)
                    ),
                    "late_atr": extension_atr,
                },
            }

            signals.append(Signal(
                symbol=symbol,
                direction=direction,
                stage=stage,
                score=best["score"],
                explosion_score=best["score"],
                entry_score=clamp(
                    best["score"] + (4 if evidence.get("micro_bos") else 0)
                ),
                safety_score=clamp(
                    54
                    + zone.get("zone_score", 0) * 0.25
                    + max(0, 50 - ob.get("spoof_risk", 0)) * 0.15
                    - conflict_penalty
                ),
                scores_by_tf={"15m": s15, "1h": s1, "4h": s4},
                price=m1["price"],
                entry_low=plan[0],
                entry_high=plan[1],
                stop=plan[2],
                tp1=plan[3],
                tp2=plan[4],
                tp3=plan[5],
                rr1=plan[6],
                rr2=plan[7],
                rr3=plan[8],
                recipe=[
                    f"الاستراتيجية: {adaptive['strategy']}",
                    f"حالة السوق: {adaptive['regime']}",
                    f"{best['id']} — {best['name']}",
                ] + reasons,
                details=details,
            ))

        if len(signals) == 2:
            signals.sort(key=lambda x: x.explosion_score, reverse=True)
            if (
                signals[0].explosion_score
                - signals[1].explosion_score
                < DIRECTION_GAP
            ):
                return []
            signals = [signals[0]]

        stable = []

        for sig in signals:
            key = (sig.symbol, sig.direction, sig.stage)
            self.stage_streaks[key] = self.stage_streaks.get(key, 0) + 1

            if sig.stage == "EXPLOSION":
                need = 1
            elif sig.stage == "CONFIRMED":
                need = RADAR_READY_STREAK
            else:
                need = RADAR_EARLY_STREAK

            if self.stage_streaks[key] >= need:
                stable.append(sig)

        active = {
            (x.symbol, x.direction, x.stage)
            for x in signals
        }

        for key in list(self.stage_streaks):
            if key[0] == symbol and key not in active:
                self.stage_streaks[key] = 0

        return stable

    async def track_open_positions(self):
        while self.running:
            try:
                async with aiosqlite.connect(DB_PATH) as db:
                    db.row_factory = aiosqlite.Row
                    rows = await (await db.execute(
                        "SELECT * FROM opportunities WHERE status='OPEN' ORDER BY id DESC LIMIT 500"
                    )).fetchall()
                if not rows:
                    await asyncio.sleep(60)
                    continue

                prices = {
                    x["symbol"]: float(x["price"])
                    for x in await self.client.get("/fapi/v1/ticker/price")
                    if "symbol" in x and "price" in x
                }
                async with aiosqlite.connect(DB_PATH) as db:
                    for r in rows:
                        p = prices.get(r["symbol"])
                        if not p:
                            continue
                        direction = r["direction"]
                        mid = (r["entry_low"] + r["entry_high"]) / 2
                        entered = r["entered_at"] is not None
                        in_zone = r["entry_low"] <= p <= r["entry_high"]

                        if not entered and in_zone:
                            await db.execute(
                                "UPDATE opportunities SET entered_at=?,entered_price=?,best_price=?,worst_price=? WHERE id=?",
                                (now_local().isoformat(), p, p, p, r["id"])
                            )
                            entered = True

                        if not entered:
                            continue

                        best = r["best_price"] if r["best_price"] is not None else p
                        worst = r["worst_price"] if r["worst_price"] is not None else p
                        best = max(best,p) if direction=="BUY" else min(best,p)
                        worst = min(worst,p) if direction=="BUY" else max(worst,p)
                        mfe = pct_change(best, mid) * (1 if direction=="BUY" else -1)
                        mae = pct_change(worst, mid) * (-1 if direction=="BUY" else 1)

                        updates = {"best_price":best, "worst_price":worst, "mfe_pct":max(0,mfe), "mae_pct":max(0,mae)}
                        hit_stop = p <= r["stop"] if direction=="BUY" else p >= r["stop"]
                        hit1 = p >= r["tp1"] if direction=="BUY" else p <= r["tp1"]
                        hit2 = p >= r["tp2"] if direction=="BUY" else p <= r["tp2"]
                        hit3 = p >= r["tp3"] if direction=="BUY" else p <= r["tp3"]
                        ts = now_local().isoformat()

                        if hit1 and not r["tp1_at"]: updates["tp1_at"] = ts
                        if hit2 and not r["tp2_at"]: updates["tp2_at"] = ts
                        if hit3 and not r["tp3_at"]:
                            updates.update({"tp3_at":ts,"closed_at":ts,"status":"CLOSED","outcome":"TP3"})
                        elif hit_stop and not r["stop_at"]:
                            outcome = "SL_AFTER_TP" if (r["tp1_at"] or hit1) else "SL"
                            updates.update({"stop_at":ts,"closed_at":ts,"status":"CLOSED","outcome":outcome})

                        set_sql = ", ".join(f"{k}=?" for k in updates)
                        await db.execute(
                            f"UPDATE opportunities SET {set_sql},updated_at=? WHERE id=?",
                            (*updates.values(), ts, r["id"])
                        )
                    await db.commit()
            except Exception:
                log.exception("Position tracker failed")
            await asyncio.sleep(60)


engine = Engine()


# =========================================================
# واجهة المتابعة
# =========================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await engine.start()
    yield
    await engine.close()

app = FastAPI(title="Ahmed Early Explosion Trader", lifespan=lifespan)



@app.get("/test-telegram")
async def test_telegram():
    if not ENABLE_MANUAL_TEST_ENDPOINT:
        return JSONResponse(
            {"ok": False, "error": "Manual test endpoint disabled"},
            status_code=403,
        )

    ok = await send_telegram(
        engine.telegram_session,
        "🧪 <b>اختبار يدوي من رابط البوت</b>\n\n"
        "✅ Telegram متصل بنجاح\n"
        "✅ Railway يعمل\n"
        "✅ Ahmed Early Reversal AI v9 ADAPTIVE جاهز\n"
        f"🕒 {now_local().strftime('%d-%m-%Y %H:%M:%S')} (السعودية)"
    )

    return {
        "ok": ok,
        "message": "Telegram test sent" if ok else "Telegram test failed",
        "time": now_local().isoformat(),
    }


@app.get("/health")
async def health():
    return {
        "ok": engine.last_error is None,
        "service": "Ahmed Early Explosion Trader",
        "last_scan": engine.last_scan,
        "last_error": engine.last_error,
        "scan_number": engine.scan_no,
        "symbols": engine.symbol_count,
        "candidates": engine.candidate_count,
        "alerts_sent_since_start": engine.alert_count,
        "fast_top": engine.fast_top,
        "time": now_local().isoformat(),
    }


@app.get("/opportunities")
async def opportunities(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT * FROM opportunities ORDER BY id DESC LIMIT ?", (limit,)
        )).fetchall()
    return [dict(r) for r in rows]


@app.get("/checkpoints")
async def checkpoints(limit: int = 100):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        rows = await (await db.execute(
            "SELECT * FROM checkpoints ORDER BY id DESC LIMIT ?", (limit,)
        )).fetchall()
    return [dict(r) for r in rows]


@app.get("/stats")
async def stats():
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        overall = await (await db.execute("""
            SELECT COUNT(*) total,
                   SUM(status='OPEN') open_count,
                   SUM(outcome='TP3') tp3,
                   SUM(outcome='SL') sl,
                   AVG(mfe_pct) avg_mfe,
                   AVG(mae_pct) avg_mae
            FROM opportunities
        """)).fetchone()
        by_group = await (await db.execute("""
            SELECT direction,current_stage,COUNT(*) cases,
                   SUM(outcome='TP3') tp3,
                   SUM(outcome='SL') sl,
                   AVG(mfe_pct) avg_mfe,
                   AVG(mae_pct) avg_mae
            FROM opportunities
            GROUP BY direction,current_stage
        """)).fetchall()
    return {"overall": dict(overall), "groups": [dict(x) for x in by_group]}


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    h = await health()
    s = await stats()
    status = "يعمل ✅" if h["ok"] else "يوجد خطأ ⚠️"
    return f"""<!doctype html>
<html lang="ar" dir="rtl">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ahmed Early Explosion Trader</title>
<style>
body{{font-family:Arial;background:#0b1020;color:#eef2ff;margin:0;padding:24px}}
.wrap{{max-width:1000px;margin:auto}}
.card{{background:#151c33;border:1px solid #2a3558;border-radius:16px;padding:18px;margin:12px 0}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}}
.k{{font-size:13px;color:#aab4d6}} .v{{font-size:24px;font-weight:bold;margin-top:6px}}
a{{color:#8bb8ff}} code{{color:#f6c86f}}
</style></head>
<body><div class="wrap">
<h1>Ahmed Early Explosion Trader</h1>
<div class="card"><b>الحالة: {status}</b><br>آخر فحص: {h["last_scan"] or "لم يبدأ"}</div>
<div class="grid">
<div class="card"><div class="k">رقم الفحص</div><div class="v">{h["scan_number"]}</div></div>
<div class="card"><div class="k">العقود</div><div class="v">{h["symbols"]}</div></div>
<div class="card"><div class="k">التحليل العميق</div><div class="v">{h["candidates"]}</div></div>
<div class="card"><div class="k">التنبيهات</div><div class="v">{h["alerts_sent_since_start"]}</div></div>
<div class="card"><div class="k">الفرص الكلية</div><div class="v">{s["overall"].get("total") or 0}</div></div>
<div class="card"><div class="k">الفرص المفتوحة</div><div class="v">{s["overall"].get("open_count") or 0}</div></div>
</div>
<div class="card">
<h3>الروابط</h3>
<a href="/health">Health</a> ·
<a href="/opportunities">Opportunities</a> ·
<a href="/stats">Stats</a> ·
<a href="/checkpoints">Checkpoints</a>
</div>
<div class="card">الفريمات: <code>15M / 1H / 4H</code><br>
لا ينفذ البوت صفقات تلقائيًا، وجميع الخطط إحصائية مقترحة.</div>
</div></body></html>"""


if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=PORT, log_level="info")

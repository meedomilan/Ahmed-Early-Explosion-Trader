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

DB_PATH = os.getenv("DB_PATH", "data/early_explosion.db")
SEND_STARTUP_MESSAGE = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
SEND_TEST_MESSAGE = os.getenv("SEND_TEST_MESSAGE", "true").lower() == "true"

# v5 FAST SMART PRO
PRO_EARLY_SCORE = float(os.getenv("PRO_EARLY_SCORE", "64"))
PRO_CONFIRMED_SCORE = float(os.getenv("PRO_CONFIRMED_SCORE", "72"))
PRO_EXPLOSION_SCORE = float(os.getenv("PRO_EXPLOSION_SCORE", "80"))

PRO_MIN_FILTERS_EARLY = int(os.getenv("PRO_MIN_FILTERS_EARLY", "3"))
PRO_MIN_FILTERS_CONFIRMED = int(os.getenv("PRO_MIN_FILTERS_CONFIRMED", "4"))
PRO_MIN_FILTERS_EXPLOSION = int(os.getenv("PRO_MIN_FILTERS_EXPLOSION", "5"))

PRO_MIN_GROUPS_EARLY = int(os.getenv("PRO_MIN_GROUPS_EARLY", "2"))
PRO_MIN_GROUPS_CONFIRMED = int(os.getenv("PRO_MIN_GROUPS_CONFIRMED", "3"))
PRO_MIN_GROUPS_EXPLOSION = int(os.getenv("PRO_MIN_GROUPS_EXPLOSION", "3"))

PRO_MAX_BOOST = float(os.getenv("PRO_MAX_BOOST", "18"))
PRO_MAX_PENALTY = float(os.getenv("PRO_MAX_PENALTY", "16"))
PRO_MAX_EARLY_EXTENSION_ATR = float(os.getenv("PRO_MAX_EARLY_EXTENSION_ATR", "0.85"))
PRO_MAX_ENTRY_EXTENSION_ATR = float(os.getenv("PRO_MAX_ENTRY_EXTENSION_ATR", "0.65"))

PRO_REQUIRE_CORE_FLOW = os.getenv("PRO_REQUIRE_CORE_FLOW", "true").lower() == "true"
PRO_COOLDOWN_MINUTES = int(os.getenv("PRO_COOLDOWN_MINUTES", "90"))

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



def _ema_series(values: list[float], length: int) -> list[float]:
    if not values:
        return []
    alpha = 2.0 / (length + 1.0)
    out = [values[0]]
    for value in values[1:]:
        out.append(alpha * value + (1 - alpha) * out[-1])
    return out


def _rsi_series(values: list[float], length: int = 14) -> list[float]:
    if len(values) < length + 1:
        return [50.0] * len(values)
    gains, losses = [], []
    result = [50.0] * len(values)
    avg_gain = avg_loss = 0.0
    for i in range(1, len(values)):
        change = values[i] - values[i - 1]
        gains.append(max(change, 0))
        losses.append(max(-change, 0))
        if i < length:
            continue
        if i == length:
            avg_gain = sum(gains[-length:]) / length
            avg_loss = sum(losses[-length:]) / length
        else:
            avg_gain = (avg_gain * (length - 1) + gains[-1]) / length
            avg_loss = (avg_loss * (length - 1) + losses[-1]) / length
        rs = avg_gain / avg_loss if avg_loss else 999.0
        result[i] = 100 - 100 / (1 + rs)
    return result


def _std(values: list[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    return math.sqrt(sum((x - mean) ** 2 for x in values) / len(values))


def _session_vwap(rows: list[list[Any]]) -> float:
    pv = 0.0
    volume = 0.0
    for row in rows:
        high = float(row[2])
        low = float(row[3])
        close = float(row[4])
        vol = float(row[5])
        typical = (high + low + close) / 3
        pv += typical * vol
        volume += vol
    return safe_div(pv, volume, float(rows[-1][4]) if rows else 0.0)


def helper_filter_pack(
    direction: str,
    k15: list[list[Any]],
    k1h: list[list[Any]],
    k4h: list[list[Any]],
    c15: dict,
    c1h: dict,
    c4h: dict,
    ob: dict,
    oi15: dict,
) -> dict[str, Any]:
    sign = 1 if direction == "BUY" else -1

    closes = [float(x[4]) for x in k15]
    highs = [float(x[2]) for x in k15]
    lows = [float(x[3]) for x in k15]
    volumes = [float(x[5]) for x in k15]
    price = closes[-1]
    a = max(c15.get("atr", 0), 1e-12)

    # Bollinger
    basis = sum(closes[-20:]) / 20
    dev = _std(closes[-20:])
    upper = basis + 2 * dev
    lower = basis - 2 * dev
    width_pct = safe_div(upper - lower, basis, 0) * 100
    bb_compression = width_pct <= max(1.2, sum(
        safe_div(
            (sum(closes[max(0, i-19):i+1]) / min(20, i+1) + 2*_std(closes[max(0, i-19):i+1]))
            - (sum(closes[max(0, i-19):i+1]) / min(20, i+1) - 2*_std(closes[max(0, i-19):i+1])),
            max(sum(closes[max(0, i-19):i+1]) / min(20, i+1), 1e-12),
            0
        ) * 100
        for i in range(max(20, len(closes)-12), len(closes))
    ) / max(1, min(12, len(closes)-20)))
    bb_side = (price <= basis if direction == "BUY" else price >= basis)

    # VWAP
    vwap = _session_vwap(k15[-48:])
    prev_close = closes[-2]
    if direction == "BUY":
        vwap_signal = price > vwap and prev_close <= vwap
        vwap_hold = price >= vwap and lows[-1] <= vwap
    else:
        vwap_signal = price < vwap and prev_close >= vwap
        vwap_hold = price <= vwap and highs[-1] >= vwap

    # Delta/CVD/OI core
    delta_strong = c15.get("delta_strength", 50) >= 58
    cvd_shift = c15.get("cvd_strength", 50) >= 58 or c15.get("cvd_divergence", 0) >= 80
    oi_expansion = oi15.get("oi_change", 0) >= 0.10
    orderbook_support = ob.get("imbalance", 50) >= 57
    absorption = ob.get("absorption", 0) >= 55

    # Volume / volatility
    volume_expansion = c15.get("volume", 0) >= 58
    volatility_ignition = c15.get("compression", 0) >= 55 and (
        volume_expansion or c15.get("breakout", 0) >= 72
    )

    # Failed breakout/breakdown and liquidity sweep proxy
    recent_high = max(highs[-8:-1])
    recent_low = min(lows[-8:-1])
    if direction == "BUY":
        failed_break = lows[-1] < recent_low and price > recent_low
        liquidity_sweep = lows[-1] < recent_low and closes[-1] > closes[-2]
    else:
        failed_break = highs[-1] > recent_high and price < recent_high
        liquidity_sweep = highs[-1] > recent_high and closes[-1] < closes[-2]

    # Momentum shift: MACD histogram + RSI turn
    e12 = _ema_series(closes, 12)
    e26 = _ema_series(closes, 26)
    macd = [x - y for x, y in zip(e12, e26)]
    signal = _ema_series(macd, 9)
    hist = [x - y for x, y in zip(macd, signal)]
    rsis = _rsi_series(closes, 14)

    if direction == "BUY":
        macd_turn = hist[-1] > hist[-2] > hist[-3]
        rsi_turn = rsis[-1] > rsis[-3] and rsis[-3] < 52
    else:
        macd_turn = hist[-1] < hist[-2] < hist[-3]
        rsi_turn = rsis[-1] < rsis[-3] and rsis[-3] > 48
    momentum_shift = macd_turn or rsi_turn

    # EMA alignment - background confirmation only
    ema20 = _ema_series(closes, 20)[-1]
    ema50 = _ema_series(closes, 50)[-1]
    ema_alignment = (
        price > ema20 > ema50 if direction == "BUY"
        else price < ema20 < ema50
    )

    # Structure / FVG / Order block proxies
    near_structure = (
        safe_div(price - recent_low, a, 99) <= 0.8 if direction == "BUY"
        else safe_div(recent_high - price, a, 99) <= 0.8
    )
    fvg_proxy = c15.get("compression", 0) >= 52 and c15.get("breakout", 0) >= 60
    order_block_proxy = near_structure and absorption

    filters = {
        "Delta Acceleration": {
            "active": delta_strong, "weight": 4.0, "group": "FLOW"
        },
        "CVD Shift": {
            "active": cvd_shift, "weight": 3.5, "group": "FLOW"
        },
        "OI Expansion": {
            "active": oi_expansion, "weight": 3.0, "group": "FLOW"
        },
        "Order Book Support": {
            "active": orderbook_support, "weight": 3.0, "group": "FLOW"
        },
        "Absorption": {
            "active": absorption, "weight": 3.0, "group": "FLOW"
        },
        "VWAP Reclaim/Reject": {
            "active": vwap_signal or vwap_hold, "weight": 2.5, "group": "PRICE"
        },
        "Bollinger Compression": {
            "active": bb_compression and bb_side, "weight": 2.5, "group": "PRICE"
        },
        "Volume Expansion": {
            "active": volume_expansion, "weight": 2.5, "group": "PRICE"
        },
        "Volatility Ignition": {
            "active": volatility_ignition, "weight": 2.5, "group": "PRICE"
        },
        "Failed Break": {
            "active": failed_break, "weight": 3.5, "group": "STRUCTURE"
        },
        "Liquidity Sweep": {
            "active": liquidity_sweep, "weight": 3.5, "group": "STRUCTURE"
        },
        "Order Block Context": {
            "active": order_block_proxy, "weight": 2.0, "group": "STRUCTURE"
        },
        "FVG Context": {
            "active": fvg_proxy, "weight": 1.5, "group": "STRUCTURE"
        },
        "Momentum Shift": {
            "active": momentum_shift, "weight": 2.0, "group": "MOMENTUM"
        },
        "EMA Alignment": {
            "active": ema_alignment, "weight": 1.5, "group": "MOMENTUM"
        },
    }

    active = [name for name, data in filters.items() if data["active"]]
    groups = sorted({
        data["group"] for data in filters.values() if data["active"]
    })
    boost = min(
        PRO_MAX_BOOST,
        sum(data["weight"] for data in filters.values() if data["active"])
    )

    # Penalize dangerous contradictions instead of sending weak alerts.
    penalty_reasons = []
    penalty = 0.0

    if ob.get("imbalance", 50) < 42:
        penalty += 5
        penalty_reasons.append("Order Book يعاكس الاتجاه")
    if c15.get("delta_strength", 50) < 43:
        penalty += 5
        penalty_reasons.append("Delta يعاكس الاتجاه")
    if ob.get("spoof_risk", 0) >= 65:
        penalty += 4
        penalty_reasons.append("Spoofing محتمل")
    extension = c15.get("extension_atr", 0)
    if extension > PRO_MAX_EARLY_EXTENSION_ATR:
        penalty += min(8, (extension - PRO_MAX_EARLY_EXTENSION_ATR) * 12)
        penalty_reasons.append("الحركة ممتدة")

    penalty = min(PRO_MAX_PENALTY, penalty)

    return {
        "filters": filters,
        "active": active,
        "groups": groups,
        "filter_count": len(active),
        "group_count": len(groups),
        "boost": boost,
        "penalty": penalty,
        "penalty_reasons": penalty_reasons,
        "vwap": vwap,
        "bb_width_pct": width_pct,
        "core_flow": delta_strong or cvd_shift or oi_expansion,
        "extension_atr": extension,
    }


def pro_stage(
    base_stage: str,
    final_score: float,
    pack: dict[str, Any],
) -> str | None:
    count = pack["filter_count"]
    groups = pack["group_count"]
    extension = pack["extension_atr"]

    if PRO_REQUIRE_CORE_FLOW and not pack["core_flow"]:
        return None

    if (
        base_stage == "EXPLOSION"
        and final_score >= PRO_EXPLOSION_SCORE
        and count >= PRO_MIN_FILTERS_EXPLOSION
        and groups >= PRO_MIN_GROUPS_EXPLOSION
        and extension <= PRO_MAX_ENTRY_EXTENSION_ATR
    ):
        return "EXPLOSION"

    if (
        base_stage in ("CONFIRMED", "EXPLOSION")
        and final_score >= PRO_CONFIRMED_SCORE
        and count >= PRO_MIN_FILTERS_CONFIRMED
        and groups >= PRO_MIN_GROUPS_CONFIRMED
        and extension <= PRO_MAX_EARLY_EXTENSION_ATR
    ):
        return "CONFIRMED"

    if (
        final_score >= PRO_EARLY_SCORE
        and count >= PRO_MIN_FILTERS_EARLY
        and groups >= PRO_MIN_GROUPS_EARLY
        and extension <= PRO_MAX_EARLY_EXTENSION_ATR
    ):
        return "EARLY"

    return None


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
        "EARLY": "🟡 رصد تجميع/تصريف مبكر",
        "CONFIRMED": "🟠 اقتراب الانفجار",
        "EXPLOSION": "🔥 بدأ الاشتعال",
    }[sig.stage]
    side = "شراء" if sig.direction == "BUY" else "بيع"
    checks = "\n".join(f"✅ {html.escape(x)}" for x in sig.recipe[:6])
    action = {
        "EARLY": "👀 الحالة: رصد مبكر قبل الكسر — مراقبة دقيقة",
        "CONFIRMED": "📍 الحالة: اقتراب من الكسر — منطقة دخول مقترحة",
        "EXPLOSION": "⚡ الحالة: بدأ الاشتعال ولم تمتد الحركة بعد",
    }[sig.stage]
    historical = "يتعلم"
    tv = f"https://www.tradingview.com/chart/?symbol=BINANCE:{sig.symbol}.P"
    bn = f"https://www.binance.com/en/futures/{sig.symbol}"
    ts = now_local().strftime("%d-%m-%Y %H:%M:%S")

    return f"""<b>{stage_title} — {side}</b>

💰 العملة: <b>#{sig.symbol}.P</b>
⏰ الفريمات: 15M / 1H / 4H
💵 السعر: <b>{fmt_price(sig.price)}</b>

🧠 درجة الانفجار: <b>{sig.explosion_score:.1f}%</b>
🎯 جودة الدخول: <b>{sig.entry_score:.1f}%</b>
🛡️ درجة الأمان: <b>{sig.safety_score:.1f}%</b>
🧬 بصمة الأموال الذكية: <b>{sig.details.get("smart_persistence", {}).get("smart_score", 0):.1f}%</b>
🧩 الفلاتر المتوافقة: <b>{sig.details.get("pro_filters", {}).get("filter_count", 0)}</b>
🗂️ مجموعات التأكيد: <b>{sig.details.get("pro_filters", {}).get("group_count", 0)}</b>
➕ دعم الفلاتر: <b>+{sig.details.get("pro_filters", {}).get("boost", 0):.1f}</b>
➖ العقوبات: <b>-{sig.details.get("pro_filters", {}).get("penalty", 0):.1f}</b>
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
    quote_volume = float(ticker.get("quoteVolume", 0) or 0)
    trades = float(ticker.get("count", 0) or 0)
    change_24h = abs(float(ticker.get("priceChangePercent", 0) or 0))

    price_burst = 0.0
    volume_burst = 0.0
    trade_burst = 0.0
    if prev:
        price_burst = abs(pct_change(price, prev.get("price", price)))
        volume_burst = max(0.0, pct_change(quote_volume, prev.get("quote_volume", quote_volume)))
        trade_burst = max(0.0, pct_change(trades, prev.get("trades", trades)))

    liquidity = clamp((math.log10(max(quote_volume, 1)) - 5.5) * 22)
    score = clamp(
        0.35 * clamp(price_burst * 900)
        + 0.30 * clamp(volume_burst * 10)
        + 0.20 * clamp(trade_burst * 9)
        + 0.10 * liquidity
        + 0.05 * clamp(change_24h * 5)
    )
    return score, {
        "price": price,
        "quote_volume": quote_volume,
        "trades": trades,
        "price_burst": price_burst,
        "volume_burst": volume_burst,
        "trade_burst": trade_burst,
        "fast_score": score,
    }


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

    async def start(self):
        await init_db()
        await self.client.start()
        self.telegram_session = aiohttp.ClientSession()
        if SEND_STARTUP_MESSAGE:
            await send_telegram(
                self.telegram_session,
                "✅ <b>Ahmed Early Explosion Trader v5 FAST SMART PRO بدأ العمل</b>\n\n"
                "الفريمات: 15M / 1H / 4H\n"
                "المراحل: استعداد مبكر جدًا / استعداد مؤكد / بداية الانفجار\n"
                "⚠️ لا ينفذ صفقات تلقائيًا."
            )
        if SEND_TEST_MESSAGE:
            await send_telegram(
                self.telegram_session,
                "🧪 <b>اختبار v5 FAST SMART PRO ناجح</b>\n\n"
                "✅ Telegram متصل\n"
                "✅ Railway يعمل\n"
                "✅ المحرك الأساسي والفلاتر الخلفية جاهزة"
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

        # المرحلة السريعة: نقطة بيانات واحدة تفحص جميع العقود.
        fast_ranked = []
        new_state = {}
        for ticker in tickers:
            symbol = ticker.get("symbol")
            if symbol not in allowed:
                continue
            quote_volume = float(ticker.get("quoteVolume", 0) or 0)
            if quote_volume < MIN_QUOTE_VOLUME:
                continue
            score, state = fast_market_score(ticker, self.fast_state.get(symbol))
            new_state[symbol] = state
            # السيولة تمنع العملات الضعيفة، والتسارع القصير يرفع الأولوية.
            fast_ranked.append((score, quote_volume, symbol, state))

        self.fast_state.update(new_state)
        fast_ranked.sort(key=lambda x: (x[0], x[1]), reverse=True)
        pool = fast_ranked[:FAST_POOL]

        # احتفظ ببعض الأعلى سيولة حتى لا يفوت أول فحص قبل توفر مقارنة سابقة.
        if self.scan_no <= 2:
            pool = sorted(fast_ranked, key=lambda x: x[1], reverse=True)[:FAST_POOL]

        candidates = [x[2] for x in pool[:DEEP_CANDIDATES]]
        self.fast_top = [
            {"symbol": x[2], "fast_score": round(x[0], 2), **x[3]}
            for x in pool[:min(20, len(pool))]
        ]
        self.candidate_count = len(candidates)

        # المرحلة العميقة: فقط أفضل المرشحين، مع حد زمني يحمي الدورة.
        tasks = [asyncio.create_task(self.analyze_symbol(s)) for s in candidates]
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=max(12, SCAN_SECONDS * 1.6),
            )
        except asyncio.TimeoutError:
            for task in tasks:
                if not task.done():
                    task.cancel()
            results = [task.result() if task.done() and not task.cancelled() else RuntimeError("deep timeout") for task in tasks]
            log.warning("Deep scan timeout; unfinished candidates were cancelled")

        alerts = 0
        analyzed = 0
        for result in results:
            if isinstance(result, Exception):
                log.debug("symbol analysis failed: %r", result)
                continue
            analyzed += 1
            for sig in result:
                inv_key = (sig.symbol, sig.direction)
                if is_invalidated(sig):
                    self.invalidation_streaks[inv_key] = self.invalidation_streaks.get(inv_key, 0) + 1
                else:
                    self.invalidation_streaks[inv_key] = 0
                if self.invalidation_streaks[inv_key] >= INVALIDATION_STREAK:
                    continue
                _, changed = await save_signal(sig)
                if changed:
                    ok = await send_telegram(self.telegram_session, signal_message(sig))
                    alerts += int(ok)
                    self.alert_count += int(ok)
        return alerts, analyzed

    async def analyze_symbol(self, symbol: str) -> list[Signal]:
        k15, k1, k4, oi15, oi1, depth, premium = await asyncio.gather(
            self.client.klines(symbol, "15m"),
            self.client.klines(symbol, "1h"),
            self.client.klines(symbol, "4h"),
            self.client.oi_hist(symbol, "15m"),
            self.client.oi_hist(symbol, "1h"),
            self.client.depth(symbol),
            self.client.premium(symbol),
        )

        out = []
        for direction in ("BUY", "SELL"):
            c15 = candle_features(k15, direction)
            c1 = candle_features(k1, direction)
            c4 = candle_features(k4, direction)
            ob = orderbook_features(depth, direction)
            o15 = oi_features(oi15, direction)
            o1 = oi_features(oi1, direction)
            fund = funding_features(premium, direction)

            s15 = combine_score(c15, ob, o15, fund, "15m")
            s1 = combine_score(c1, ob, o1, fund, "1h")
            s4 = combine_score(c4, ob, o1, fund, "4h")
            scores = {"15m": s15, "1h": s1, "4h": s4}
            base_stage = choose_stage(scores, c15, c1, c4)
            if not base_stage:
                continue

            explosion_score = clamp(0.45*s15 + 0.40*s1 + 0.15*s4)
            entry_score = clamp(
                0.32*c15["compression"] + 0.23*c15["breakout"]
                + 0.20*ob["imbalance"] + 0.15*c1["trend"] + 0.10*c15["volume"]
            )
            safety_score = clamp(
                0.30*s1 + 0.25*s4 + 0.20*oi_features(oi1,direction)["oi_score"]
                + 0.15*(100-ob["spoof_risk"]) + 0.10*fund["funding_score"]
            )
            score = clamp((explosion_score + entry_score + safety_score)/3)

            recipe = []
            if o15["oi_change"] > 0.3: recipe.append(f"OI يرتفع {o15['oi_change']:+.2f}%")
            if c15["cvd_strength"] >= 60: recipe.append("CVD متسارع في اتجاه الفرصة")
            if c15["cvd_divergence"] >= 80: recipe.append("CVD يتحرك والسعر ما زال متماسكًا")
            if ob["imbalance"] >= 60: recipe.append("اختلال دفتر الأوامر داعم")
            if ob["absorption"] >= 60: recipe.append("امتصاص محتمل")
            if c15["volume"] >= 60: recipe.append("زيادة ملحوظة في الحجم")
            if c15["compression"] >= 60: recipe.append("ضغط حجم داخل نطاق ضيق")
            if c15["breakout"] >= 72: recipe.append("اقتراب أو تحقق كسر بنية")
            if not recipe: recipe.append("توافق مركب بين الدلتا والحجم والبنية")

            pack = helper_filter_pack(
                direction, k15, k1, k4, c15, c1, c4, ob, o15
            )

            base_score = clamp(
                0.55 * explosion_score
                + 0.25 * entry_score
                + 0.20 * safety_score
            )
            final_score = clamp(
                base_score + pack["boost"] - pack["penalty"]
            )

            stage = pro_stage(base_stage, final_score, pack)
            if not stage:
                continue

            plan_tf = c1 if stage != "EARLY" else c15
            plan = build_trade_plan(
                direction, c15["price"], plan_tf["atr"],
                plan_tf["swing_low"], plan_tf["swing_high"], stage
            )

            hist_key = (symbol, direction)
            hist = self.market_history.setdefault(hist_key, deque(maxlen=STATE_HISTORY))
            hist.append({
                "ts": time.time(),
                "oi_change": o15["oi_change"],
                "imbalance_raw": ob["imbalance_raw"],
                "delta_strength": c15["delta_strength"],
                "cvd_strength": c15["cvd_strength"],
                "compression": c15["compression"],
                "volume": c15["volume"],
                "price": c15["price"],
            })
            smart = persistence_score(list(hist), direction)

            # البصمة المستمرة أهم من اللقطة الواحدة.
            explosion_score = clamp(0.72 * explosion_score + 0.28 * smart["smart_score"])
            entry_score = clamp(0.82 * entry_score + 0.18 * smart["persistence"])
            safety_score = clamp(0.78 * safety_score + 0.22 * smart["book_persistence"])
            score = clamp((explosion_score + entry_score + safety_score) / 3)

            # النتيجة النهائية: المحرك الأساسي + فلاتر الخلفية فقط.
            final_score = clamp(
                0.65 * score
                + 0.35 * final_score
            )
            explosion_score = final_score
            entry_score = clamp(
                entry_score + pack["boost"] * 0.35 - pack["penalty"] * 0.25
            )
            safety_score = clamp(
                safety_score + pack["group_count"] * 2 - pack["penalty"] * 0.35
            )
            score = clamp((explosion_score + entry_score + safety_score) / 3)

            if smart["smart_score"] >= SMART_SCORE:
                recipe.append(f"بصمة أموال ذكية مستمرة {smart['smart_score']:.1f}%")
            if smart["oi_accel"] >= 60:
                recipe.append("تسارع OI مستمر عبر عدة قراءات")
            if smart["book_persistence"] >= 62:
                recipe.append("اختلال دفتر الأوامر مستمر وليس لقطة واحدة")

            recipe.append(
                f"توافق الفلاتر: {pack['filter_count']} فلتر / "
                f"{pack['group_count']} مجموعات"
            )
            for filter_name in pack["active"][:7]:
                recipe.append(filter_name)

            details = {
                "orderbook": ob,
                "oi_15m": o15,
                "oi_1h": o1,
                "funding": fund,
                "features_15m": c15,
                "features_1h": c1,
                "features_4h": c4,
                "smart_persistence": smart,
                "pro_filters": pack,
                "base_stage": base_stage,
                "base_score": base_score,
                "final_score": final_score,
            }

            # المرحلة المبكرة لا تنتظر اكتمال كل الشروط،
            # لكنها تتطلب توافق فلاتر كافٍ من مجموعات مختلفة.

            out.append(Signal(
                symbol=symbol, direction=direction, stage=stage,
                score=score, explosion_score=explosion_score,
                entry_score=entry_score, safety_score=safety_score,
                scores_by_tf=scores, price=c15["price"],
                entry_low=plan[0], entry_high=plan[1], stop=plan[2],
                tp1=plan[3], tp2=plan[4], tp3=plan[5],
                rr1=plan[6], rr2=plan[7], rr3=plan[8],
                recipe=recipe, details=details,
            ))

        # لا نرسل شراء وبيع لنفس العملة معًا. نختار الاتجاه الأقوى فقط.
        if len(out) == 2:
            out.sort(key=lambda x: x.explosion_score, reverse=True)
            if out[0].explosion_score - out[1].explosion_score < DIRECTION_GAP:
                return []
            out = [out[0]]

        stable = []
        for sig in out:
            key = (sig.symbol, sig.direction, sig.stage)
            self.stage_streaks[key] = self.stage_streaks.get(key, 0) + 1
            need = 1 if sig.stage == "EXPLOSION" else (CONFIRMED_STREAK if sig.stage == "CONFIRMED" else EARLY_STREAK)
            if self.stage_streaks[key] >= need:
                stable.append(sig)

        # صفّر العدادات المعاكسة للعملة نفسها حتى لا تبقى قديمة.
        active_keys = {(x.symbol, x.direction, x.stage) for x in out}
        for key in list(self.stage_streaks):
            if key[0] == symbol and key not in active_keys:
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

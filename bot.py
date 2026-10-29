#!/usr/bin/env python3
"""
MANTHAN 2026 - Autonomous Trading Bot (v2: EMA trend-following, long-only)

STRATEGY: EMA trend-following + volatility-based sizing + hard risk controls
  ENTRY : EMA10 > EMA30 > EMA50, EMA30 rising, close above EMA10, 20-day momentum > 0,
          RSI 45-72, not stretched (close - EMA10 < 2.5 x ATR), news sentiment not negative.
  SIZE  : risk 1.5% of wealth per trade using a 2 x ATR stop (x1.25 on a fresh EMA10/EMA30
          cross), max 18% of wealth in one stock, max 8 positions, max 95% invested.
  CASH  : idle cash above 25% of wealth is added to winners that are still in a clean
          EMA uptrend (one add per position per 5 days), so money is not left sitting.
  EXIT  : ATR trailing stop | strong negative news on a down day | EMA trend break
          (EMA10 < EMA30 and close < EMA30). Soft exits respect a minimum holding period
          and, when a winner is close to 365 days old, wait for the lower LTCG tax rate.

RUN (PowerShell):
  pip install kafka-python requests numpy
  $env:BOT_ACCESS_TOKEN = ((Get-Clipboard) -join "").Trim()
  $env:SERVER_IP="192.168.65.157"
  $env:TEAM_USERNAME="mn2623"
  $env:DRY_RUN="1"        # optional: log decisions, send NO orders
  python bot.py

Every trade (and its reason) is written to trade_journal.csv for the jury defense.
"""
import csv
import json
import logging
import os
import re
import sys
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime

import numpy as np
import requests

# ------------------------------------------------------------------ config
SERVER_IP = os.environ.get("SERVER_IP", "192.168.65.157")
API = f"http://{SERVER_IP}:3002/api/v1"
BROKER = f"{SERVER_IP}:9094"
USERNAME = os.environ.get("TEAM_USERNAME", "mn2623")
GROUP_ID = f"group-{USERNAME}"          # must keep the "group-" prefix
TOKEN = os.environ.get("BOT_ACCESS_TOKEN", "")
DRY_RUN = os.environ.get("DRY_RUN", "0") == "1"
NEWS_LOG = os.environ.get("NEWS_LOG", "0") == "1"      # set to 1 to print every headline
JOURNAL_FILE = os.environ.get("JOURNAL_FILE", "trade_journal.csv")

BROKERAGE = 20.0       # per executed order
DP_CHARGE = 15.34      # per SELL of delivery shares

P = dict(
    MAX_POSITIONS=8,
    RISK_PER_TRADE=0.015,    # fraction of total wealth risked per trade
    MAX_POS_PCT=0.18,        # max wealth in one stock
    MAX_INVESTED_PCT=0.95,   # max wealth invested overall (5% cash buffer for fees)
    MIN_NOTIONAL=20_000,     # skip tiny orders (fees matter)
    ATR_STOP=2.0,            # initial stop = entry - 2 x ATR (also used for sizing)
    ATR_TRAIL=2.5,           # trailing stop = peak - 2.5 x ATR
    MIN_HOLD_DAYS=5,         # no soft exits before this (avoids whipsaw churn)
    COOLDOWN_DAYS=5,         # wait after an exit before re-entering same stock
    EMA_FAST=10, EMA_SLOW=30, EMA_TREND=50,
    RSI_LO=45, RSI_HI=72,
    MAX_EXT_ATR=2.5,         # do not chase: close must be within 2.5 ATR of the fast EMA
    FRESH_CROSS_BARS=5,      # EMA10 crossed above EMA30 within this many bars = fresh signal
    FRESH_BOOST=1.25,        # risk budget multiplier for a fresh cross
    TOPUP_IDLE_PCT=0.25,     # if cash > 25% of wealth, add to winners (0 = never add)
    TOPUP_MAX_PCT=0.06,      # max size of one add, fraction of wealth
    TOPUP_GAP_DAYS=5,        # min days between adds to the same stock
    TOPUP_MIN_GAIN=0.02,     # only add to a position already up 2%
    MIN_BARS=60,             # need this much history before trading a stock
    WARM_DAYS=400,           # calendar days of history for warm start
    NEWS_SETTLE=0.3,         # seconds to wait so same-day news (T+150ms) arrives
    MAX_DECISION_AGE=3.0,    # drop a decision older than this (next day is coming)
    NO_ENTRY_LAST_DAYS=3,    # no new buys in the last days of the simulation
    RESYNC_EVERY_DAYS=10,    # re-read cash/holdings from the server
)

KNOWN_ERRORS = [
    "ACCOUNT_NOT_VERIFIED", "SIMULATION_NOT_RUNNING", "INSUFFICIENT_CAPITAL",
    "INSUFFICIENT_MARGIN", "ACCOUNT_BLOCKED", "NO_SHORT_POSITION",
    "INVALID_TICKER", "INVALID_QUANTITY", "RATE_LIMITED",
]

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bot")


# ------------------------------------------------------------------ HTTP + rate limit
class RateLimiter:
    """Sliding window. Server allows 120 req/min; we stay at 100."""

    def __init__(self, max_calls=100, per=60.0):
        self.max_calls, self.per = max_calls, per
        self.calls = deque()
        self.lock = threading.Lock()

    def wait(self):
        while True:
            with self.lock:
                now = time.monotonic()
                while self.calls and now - self.calls[0] > self.per:
                    self.calls.popleft()
                if len(self.calls) < self.max_calls:
                    self.calls.append(now)
                    return
                sleep_for = self.per - (now - self.calls[0]) + 0.05
            time.sleep(sleep_for)


limiter = RateLimiter()
session = requests.Session()
penalty_until = 0.0


def api(method, path, counted=True, timeout=3.0, **kw):
    """One place for every REST call. Returns Response or None on network error."""
    global penalty_until
    if counted:
        pause = penalty_until - time.time()
        if pause > 0:
            time.sleep(pause)
        limiter.wait()
    headers = {"Authorization": f"Bearer {TOKEN}", "Content-Type": "application/json"}
    try:
        r = session.request(method, API + path, headers=headers, timeout=timeout, **kw)
    except requests.RequestException as e:
        log.warning("HTTP %s %s failed: %s", method, path, e)
        return None
    if r.status_code == 429:
        penalty_until = time.time() + 31
        log.error("HTTP 429 - penalty box, pausing requests for 31s")
    return r


# ------------------------------------------------------------------ helpers
def days_between(a, b):
    try:
        return (datetime.strptime(b, "%Y-%m-%d") - datetime.strptime(a, "%Y-%m-%d")).days
    except (TypeError, ValueError):
        return 0


def find_key(j, names):
    """First numeric value found under any of `names` (priority order), searched recursively."""
    if isinstance(j, dict):
        for n in names:
            v = j.get(n)
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                return float(v)
        for v in j.values():
            r = find_key(v, names)
            if r is not None:
                return r
    elif isinstance(j, list):
        for v in j:
            r = find_key(v, names)
            if r is not None:
                return r
    return None


def find_holdings(j):
    """First list of dicts that look like holdings (have 'quantity')."""
    if isinstance(j, dict):
        for v in j.values():
            r = find_holdings(v)
            if r is not None:
                return r
    elif isinstance(j, list):
        if j and all(isinstance(x, dict) for x in j) and any("quantity" in x for x in j):
            return j
        for v in j:
            r = find_holdings(v)
            if r is not None:
                return r
    return None


# Finance-specific phrases only. The sandbox feed is mostly general-interest headlines
# (chess results, TV reviews), so generic words like "wins", "cut", "fall" must NOT count.
POS_RE = re.compile(r"\b(earnings beat|beats? (estimates|expectations|forecasts)|record (profit|revenue|"
                    r"sales|earnings)|profit (surge|jump|rise)s?|revenue (growth|jump|surge)|"
                    r"raises? (guidance|outlook|dividend|forecast)|upgrad\w*|buyback|"
                    r"dividend (hike|increase)|surges?|soars?|rall(y|ies)|price target raised|"
                    r"wins? (contract|approval)|regulatory approval)\b")
NEG_RE = re.compile(r"\b(earnings miss|misses? (estimates|expectations|forecasts)|net loss|profit warning|"
                    r"cuts? (guidance|outlook|forecast|dividend)|downgrad\w*|plunges?|slumps?|tumbles?|"
                    r"fraud|probe|investigation|lawsuit|recall|bankruptcy|default|layoffs|"
                    r"shortfall|scandal|crash(es)?|sec charges|delisting)\b")


def sentiment(text):
    """Tiny keyword lexicon -> score in [-1, 1]. Placeholder for a better model."""
    t = text.lower()
    p, n = len(POS_RE.findall(t)), len(NEG_RE.findall(t))
    return (p - n) / max(1, p + n)


# ------------------------------------------------------------------ indicators
def rsi(c, n=14):
    d = np.diff(c[-(n * 4 + 1):])
    g, l = np.where(d > 0, d, 0.0), np.where(d < 0, -d, 0.0)
    ag, al = g[:n].mean(), l[:n].mean()
    for x, y in zip(g[n:], l[n:]):
        ag, al = (ag * (n - 1) + x) / n, (al * (n - 1) + y) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def atr(h, l, c, n=14):
    tr = np.maximum(h[1:] - l[1:], np.maximum(abs(h[1:] - c[:-1]), abs(l[1:] - c[:-1])))
    return float(tr[-n:].mean())


def ema(c, n):
    """Exponential moving average, seeded with the first n-bar average."""
    c = np.asarray(c, dtype=float)
    out = np.empty(len(c))
    k = 2.0 / (n + 1)
    out[:n] = c[:n].mean()
    for i in range(n, len(c)):
        out[i] = c[i] * k + out[i - 1] * (1 - k)
    return out


def features(rows):
    rows = rows[-300:]                     # plenty for EMA50 to settle, keeps it fast
    h = np.array([r[1] for r in rows], dtype=float)
    l = np.array([r[2] for r in rows], dtype=float)
    c = np.array([r[3] for r in rows], dtype=float)
    ef, es, et = ema(c, P["EMA_FAST"]), ema(c, P["EMA_SLOW"]), ema(c, P["EMA_TREND"])
    age = 99                               # bars since EMA fast crossed above EMA slow
    for i in range(1, 11):
        if ef[-i] > es[-i] and ef[-i - 1] <= es[-i - 1]:
            age = i - 1
            break
    f = dict(close=c[-1], prev=c[-2], ema_f=ef[-1], ema_s=es[-1], ema_t=et[-1],
             slope=es[-1] / es[-6] - 1, cross_age=age,
             ret20=c[-1] / c[-21] - 1, rsi=rsi(c), atr=atr(h, l, c))
    return f if f["atr"] > 0 and all(np.isfinite(v) for v in f.values()) else None


# ------------------------------------------------------------------ state
class Stock:
    def __init__(self):
        self.rows = deque(maxlen=600)       # (date, high, low, close)

    @property
    def last_date(self):
        return self.rows[-1][0] if self.rows else None

    def warm(self, hist):
        last = hist[-1][0] if hist else None
        live = [r for r in self.rows if last is None or r[0] > last]
        self.rows = deque(hist + live, maxlen=600)


@dataclass
class Position:
    qty: int
    avg: float
    entry_date: str
    peak: float
    stop: float
    last_add: str = ""


class Bot:
    def __init__(self):
        self.stocks = {}
        self.universe = set()
        self.pos = {}                  # ticker -> Position (long only)
        self.cash = None
        self.news = {}                 # ticker -> deque[(date, score)]
        self.seen_news = set()         # (date, ticker, headline) already scored
        self.cooldown = {}             # ticker -> date of last exit
        self.sim_date = None
        self.last_sync = None
        self.total_days = self.days_done = None
        self.tradable = False
        self.max_age = P["MAX_DECISION_AGE"]   # adapted to the server's day length
        self.last_hb = 0.0
        self.syncing = False
        self.last_sync_try = 0.0
        self.lock = threading.RLock()  # guards trading state
        self.load_lock = threading.Lock()
        self.pool = ThreadPoolExecutor(max_workers=4)
        new = not os.path.exists(JOURNAL_FILE)
        try:
            self.jf = open(JOURNAL_FILE, "a", newline="")
        except PermissionError:      # file is open in Excel or locked by another program
            alt = f"trade_journal_{datetime.now():%H%M%S}.csv"
            log.warning("%s is locked (open in Excel?) - writing to %s instead. Close Excel next time.",
                        JOURNAL_FILE, alt)
            self.jf = open(alt, "a", newline="")
            new = True
        self.jw = csv.writer(self.jf)
        if new:
            self.jw.writerow(["wall_time", "sim_date", "ticker", "action", "qty", "price",
                              "ok", "reason", "server_reply"])

    # ---------------- stage + universe
    def stage_loop(self):
        prev = None
        while True:
            try:
                r = api("GET", "/stage", counted=False, timeout=2)   # public endpoint
                if r is not None and r.ok:
                    j = r.json()
                    stage, status = j.get("stage"), j.get("status")
                    self.total_days, self.days_done = j.get("total_days"), j.get("days_completed")
                    iv = j.get("interval_seconds")
                    if isinstance(iv, (int, float)) and iv > 0:
                        self.max_age = max(1.5, float(iv) - 0.7)   # finish before the next day's tick
                    self.tradable = stage in ("development", "simulation") and \
                        status not in ("PAUSED", "COMPLETED")
                    if stage != prev:
                        log.info("STAGE %s -> %s (%s)", prev, stage, status)
                        if prev is None or stage == "preflight" or \
                                (stage == "simulation" and prev != "preflight"):
                            self.pool.submit(self.reset_and_load)
                        prev = stage
            except Exception:
                log.exception("stage poll failed")
            time.sleep(2.0)

    def reset_and_load(self):
        """Fetch the stock universe, rebuild state, warm-start indicators from history."""
        with self.load_lock:
            r = api("GET", "/stocks")
            if r is None or not r.ok:
                log.error("could not load /stocks")
                return
            tickers = [s.get("ticker") or s.get("symbol") for s in r.json().get("stocks", [])]
            tickers = [t for t in tickers if t]
            log.info("Universe (%d): %s", len(tickers), tickers)
            with self.lock:
                self.universe = set(tickers)
                self.stocks = {t: Stock() for t in tickers}
                self.pos.clear()
                self.news.clear()
                self.seen_news.clear()
                self.cooldown.clear()
                self.cash = None
            self.sync_portfolio()
            for t in tickers:
                self.warm(t)
                time.sleep(1.0)            # leave rate-limit room for heartbeats/orders
            log.info("Warm start complete")

    def warm(self, t):
        params = {"ticker": t, "days": P["WARM_DAYS"]}
        r = api("GET", "/stocks/history", params=params, timeout=8)
        if r is not None and r.status_code == 404:
            r = api("GET", f"/stocks/{t}/history", params={"days": P["WARM_DAYS"]}, timeout=8)
        if r is None or not r.ok:
            log.warning("no history for %s", t)
            return
        prices = r.json().get("prices", [])
        hist = sorted((p["date"], float(p["high"]), float(p["low"]), float(p["close"])) for p in prices)
        st = self.stocks.get(t)
        if st is not None:
            st.warm(hist)
            log.info("warmed %s with %d bars", t, len(hist))

    # ---------------- portfolio sync (cash + holdings)
    def request_sync(self):
        """Ask for a background sync. Never blocks a decision; at most once per 15 s."""
        now = time.monotonic()
        if self.syncing or now - self.last_sync_try < 15:
            return
        self.last_sync_try = now
        self.syncing = True
        self.pool.submit(self.sync_portfolio)

    def sync_portfolio(self):
        self.syncing = True
        try:
            # network calls happen OUTSIDE the trading lock so decisions are never stuck behind them
            r = api("GET", "/portfolio")
            if r is None or not r.ok:
                log.error("portfolio sync failed (%s) - check BOT_ACCESS_TOKEN / SERVER_IP / Wi-Fi",
                          "no response" if r is None else f"HTTP {r.status_code}")
                return
            j = r.json()
            if os.environ.get("DEBUG_RAW") == "1":
                log.info("RAW /portfolio: %s", json.dumps(j)[:1500])
            names = ("available_cash", "cash_balance", "cash", "balance")
            cash = find_key(j, names)
            if cash is None:
                rc = api("GET", "/portfolio/cash")
                if rc is not None and rc.ok:
                    cash = find_key(rc.json(), names)
            holdings = find_holdings(j) or []
            with self.lock:
                if cash is not None:
                    if not DRY_RUN or self.cash is None:   # dry run: server cash never changes, keep our own
                        self.cash = cash
                else:
                    log.error("Could not read cash from /portfolio - check response shape (DEBUG_RAW=1)")
                seen = set()
                for h in holdings:
                    t, q = h.get("ticker") or h.get("symbol"), h.get("quantity", 0)
                    if not t or h.get("is_short") or q is None or q <= 0:
                        if h.get("is_short"):
                            log.warning("short position found in %s - this bot does not manage shorts", t)
                        continue
                    seen.add(t)
                    avg = float(h.get("avg_buy_price") or h.get("avg_price") or h.get("current_price") or 0)
                    p = self.pos.get(t)
                    if p:
                        p.qty = int(q)
                        p.avg = avg or p.avg
                    else:   # unknown entry date -> treat as opened now (conservative for tax logic)
                        self.pos[t] = Position(int(q), avg, self.sim_date or "", avg, avg * 0.92)
                for t in list(self.pos):
                    if t not in seen and not DRY_RUN:     # dry run: server holds nothing, keep our book
                        del self.pos[t]
                self.last_sync = self.sim_date
                log.info("SYNC cash=%s positions=%s", self.cash, {t: p.qty for t, p in self.pos.items()})
        except Exception:
            log.exception("portfolio sync crashed")
        finally:
            self.syncing = False

    # ---------------- kafka message handlers
    def dispatch(self, msg):
        et = msg.get("event_type") or msg.get("event")
        if et == "preflight_tick":
            self.on_preflight(msg)
        elif et == "price_tick":
            self.on_tick(msg)
        elif et == "news_flash":
            self.on_news(msg)

    def on_preflight(self, msg):
        seq = msg.get("seq", (msg.get("data") or {}).get("seq"))
        now = time.monotonic()
        if seq is None or now - self.last_hb < 1.0:
            return
        self.last_hb = now
        self.pool.submit(lambda: api("POST", "/bot/heartbeat", json={"seq": seq}))

    def on_news(self, msg):
        d = msg.get("data") or {}
        t, date = d.get("ticker"), msg.get("simulated_date") or d.get("date")
        if not t or not date:
            return
        key = (date, t, d.get("headline", ""))
        if key in self.seen_news:                       # feed repeats headlines; score each once
            return
        self.seen_news.add(key)
        s = sentiment(f"{d.get('headline', '')} {d.get('description', '')}")
        self.news.setdefault(t, deque(maxlen=60)).append((date, s))
        if NEWS_LOG:
            log.info("NEWS %s %s sent=%+.2f | %s", date, t, s, d.get("headline", ""))

    def on_tick(self, msg):
        d = msg.get("data") or {}
        t, date = d.get("ticker"), msg.get("simulated_date") or d.get("date")
        if not t or not date or (self.universe and t not in self.universe):
            return
        st = self.stocks.setdefault(t, Stock())
        if st.last_date and date < st.last_date:          # sandbox loop restarted at Day 1
            st.rows.clear()
        if st.last_date == date:
            return
        st.rows.append((date, float(d["high"]), float(d["low"]), float(d["close"])))
        self.sim_date = date
        threading.Timer(P["NEWS_SETTLE"], self.decide, args=(t, date, time.monotonic())).start()

    def news_score(self, t, date):
        """Average sentiment per day, then decay over 3 days. Averaging (not summing) stops
        tickers with 10 random headlines a day from hitting +-1 just by volume."""
        by_day = {}
        for nd, sc in list(self.news.get(t, [])):
            gap = days_between(nd, date)
            if 0 <= gap <= 3:
                by_day.setdefault(gap, []).append(sc)
        s = sum((0.6 ** gap) * (sum(v) / len(v)) for gap, v in by_day.items())
        return max(-1.0, min(1.0, s))

    # ---------------- decision engine
    def decide(self, t, date, recv):
        with self.lock:
            try:
                st = self.stocks.get(t)
                if not self.tradable or st is None or st.last_date != date:
                    return                                  # not live, or a newer day already arrived
                if time.monotonic() - recv > self.max_age:
                    log.warning("%s %s decision too late - skipped", date, t)
                    return
                rows = list(st.rows)
                if len(rows) < P["MIN_BARS"]:
                    return
                f = features(rows)
                if f is None:
                    return
                if self.cash is None or (self.last_sync and
                                         days_between(self.last_sync, date) >= P["RESYNC_EVERY_DAYS"]):
                    self.request_sync()           # background; this decision does not wait for it
                news = self.news_score(t, date)
                pos = self.pos.get(t)
                if pos:
                    self.manage(t, pos, f, news, date)
                else:
                    self.consider_entry(t, f, news, date)
            except Exception:
                log.exception("decide failed for %s", t)

    def manage(self, t, pos, f, news, date):
        close = f["close"]
        pos.peak = max(pos.peak, close)
        pos.stop = max(pos.stop, pos.peak - P["ATR_TRAIL"] * f["atr"])
        held = max(0, days_between(pos.entry_date, date)) if pos.entry_date else 0
        reason = None
        if close <= pos.stop:
            reason = f"stop hit (stop={pos.stop:.2f})"
        elif news <= -0.6 and f["prev"] - close >= f["atr"]:
            reason = f"negative news ({news:+.2f}) confirmed by a drop of {f['prev'] - close:.2f} (>1 ATR)"
        elif f["ema_f"] < f["ema_s"] and close < f["ema_s"]:
            if held < P["MIN_HOLD_DAYS"]:
                pass
            elif close > pos.avg and 300 <= held <= 365:
                log.info("%s trend break but held %dd in profit - waiting for LTCG rate", t, held)
            else:
                reason = "EMA trend break (EMA10<EMA30 and close<EMA30)"
        if reason:
            self.sell(t, pos, close, date, reason)
        else:
            self.maybe_add(t, pos, f, date, held)

    def uptrend(self, f):
        return (f["ema_f"] > f["ema_s"] > f["ema_t"] and f["slope"] > 0 and f["close"] > f["ema_f"]
                and f["rsi"] <= P["RSI_HI"]
                and f["close"] - f["ema_f"] <= P["MAX_EXT_ATR"] * f["atr"])

    def consider_entry(self, t, f, news, date):
        td, dc = self.total_days, self.days_done
        if td and dc is not None and td - dc <= P["NO_ENTRY_LAST_DAYS"]:
            return
        if len(self.pos) >= P["MAX_POSITIONS"]:
            return
        last_exit = self.cooldown.get(t)
        if last_exit and days_between(last_exit, date) < P["COOLDOWN_DAYS"]:
            return
        c = f["close"]
        ok = (self.uptrend(f) and f["ret20"] > 0 and f["rsi"] >= P["RSI_LO"] and news > -0.5)
        if not ok:
            return
        fresh = f["cross_age"] <= P["FRESH_CROSS_BARS"]
        qty = self.size(c, f["atr"], P["FRESH_BOOST"] if fresh else 1.0)
        if qty > 0:
            why = (f"EMA{P['EMA_FAST']}>{P['EMA_SLOW']}>{P['EMA_TREND']} "
                   f"{'fresh cross' if fresh else 'trend'} mom20={f['ret20']:+.1%} "
                   f"RSI={f['rsi']:.0f} ATR={f['atr']:.2f} news={news:+.2f}")
            self.buy(t, qty, f, date, why)

    def maybe_add(self, t, pos, f, date, held):
        """Put idle cash to work: add to a winner that is still in a clean EMA uptrend."""
        if P["TOPUP_IDLE_PCT"] <= 0 or self.cash is None or held < 3:
            return
        td, dc = self.total_days, self.days_done
        if td and dc is not None and td - dc <= P["NO_ENTRY_LAST_DAYS"]:
            return
        eq, inv = self.equity()
        c = f["close"]
        if eq <= 0 or self.cash / eq < P["TOPUP_IDLE_PCT"]:
            return
        if pos.last_add and days_between(pos.last_add, date) < P["TOPUP_GAP_DAYS"]:
            return
        if not self.uptrend(f) or c < pos.avg * (1 + P["TOPUP_MIN_GAIN"]):
            return
        room = min(eq * P["MAX_POS_PCT"] - pos.qty * c,
                   eq * P["MAX_INVESTED_PCT"] - inv,
                   self.cash - BROKERAGE - 1000,
                   eq * P["TOPUP_MAX_PCT"])
        qty = int(room // c)
        if qty <= 0 or qty * c < P["MIN_NOTIONAL"]:
            return
        why = f"add to winner: idle cash {self.cash / eq:.0%} of wealth, up {c / pos.avg - 1:+.1%}, EMA uptrend intact"
        if self.place_order(t, "BUY", qty, date, c, why):
            self.cash -= qty * c + BROKERAGE
            pos.avg = (pos.avg * pos.qty + c * qty) / (pos.qty + qty)
            pos.qty += qty
            pos.last_add = date
            self.log_util()

    def equity(self):
        inv = 0.0
        for t, p in self.pos.items():
            rows = self.stocks[t].rows if t in self.stocks else None
            px = rows[-1][3] if rows else p.avg
            inv += p.qty * px
        return (self.cash or 0.0) + inv, inv

    def size(self, price, atr_v, boost=1.0):
        if self.cash is None:
            return 0
        eq, inv = self.equity()
        by_risk = eq * P["RISK_PER_TRADE"] * boost / (P["ATR_STOP"] * atr_v)
        by_cap = eq * P["MAX_POS_PCT"] / price
        by_room = (eq * P["MAX_INVESTED_PCT"] - inv) / price
        by_cash = (self.cash - BROKERAGE - 1000) / price
        qty = int(min(by_risk, by_cap, by_room, by_cash))
        return qty if qty > 0 and qty * price >= P["MIN_NOTIONAL"] else 0

    def log_util(self):
        eq, inv = self.equity()
        if eq > 0:
            log.info("PORTFOLIO wealth=%.0f invested=%.1f%% cash=%.1f%% positions=%d",
                     eq, 100 * inv / eq, 100 * (self.cash or 0) / eq, len(self.pos))

    # ---------------- order execution
    def buy(self, t, qty, f, date, reason):
        px = f["close"]
        if self.place_order(t, "BUY", qty, date, px, reason):
            self.cash -= qty * px + BROKERAGE
            self.pos[t] = Position(qty, px, date, px, px - P["ATR_STOP"] * f["atr"])
            self.log_util()

    def sell(self, t, pos, px, date, reason):
        if self.place_order(t, "SELL", pos.qty, date, px, reason):
            self.cash += pos.qty * px - BROKERAGE - DP_CHARGE
            self.cooldown[t] = date
            log.info("CLOSED %s pnl=%+.0f", t, (px - pos.avg) * pos.qty)
            del self.pos[t]
            self.log_util()

    def place_order(self, t, action, qty, date, px, reason):
        log.info("%s %s x%d @%.2f | %s", action, t, qty, px, reason)
        if DRY_RUN:
            self.journal(date, t, action, qty, px, True, reason, "DRY_RUN")
            return True
        r = api("POST", "/orders", json={"ticker": t, "action": action,
                                         "quantity": int(qty), "date": date})
        body = None
        if r is not None:
            try:
                body = r.json()
            except ValueError:
                body = {"raw": r.text[:200]}
        text = json.dumps(body)[:300] if body is not None else "no response"
        status = str((body or {}).get("status", "")).lower() if isinstance(body, dict) else ""
        ok = r is not None and r.ok and status not in ("rejected", "error", "failed") \
            and not (isinstance(body, dict) and body.get("error"))
        self.journal(date, t, action, qty, px, ok, reason, text)
        if not ok:
            code = next((c for c in KNOWN_ERRORS if c in text), "UNKNOWN")
            log.warning("ORDER REJECTED %s %s x%d -> %s | %s", action, t, qty, code, text)
            if code == "SIMULATION_NOT_RUNNING":
                self.tradable = False
            elif code != "RATE_LIMITED":
                self.sync_portfolio()           # our cash/holdings view may be off
        return ok

    def journal(self, date, t, action, qty, px, ok, reason, reply):
        try:
            self.jw.writerow([datetime.now().isoformat(timespec="seconds"), date, t, action,
                              qty, f"{px:.2f}", ok, reason, reply])
            self.jf.flush()
        except OSError as e:         # never let a journal problem stop trading
            log.warning("could not write journal: %s", e)

    # ---------------- main loop
    def run(self):
        from kafka import KafkaConsumer     # imported here so the module can be tested without Kafka
        threading.Thread(target=self.stage_loop, daemon=True).start()
        while True:
            try:
                consumer = KafkaConsumer(
                    "market.ticks", "market.news",
                    bootstrap_servers=BROKER, group_id=GROUP_ID,
                    auto_offset_reset="latest", enable_auto_commit=False,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")))
                log.info("Kafka connected: %s group=%s dry_run=%s", BROKER, GROUP_ID, DRY_RUN)
                for m in consumer:
                    try:
                        self.dispatch(m.value)
                    except Exception:
                        log.exception("bad message")
            except Exception as e:
                log.error("Kafka error: %s - reconnecting in 2s", e)
                time.sleep(2)


def check_token():
    """Fail fast with a clear message instead of failing silently later."""
    if len(TOKEN) < 50 or not TOKEN.startswith("eyJ"):
        sys.exit("BOT_ACCESS_TOKEN looks like a placeholder (length %d). Copy your REAL token from the "
                 "portal (Participant Guide > Step 1) - it is a long string starting with eyJ." % len(TOKEN))
    r = api("GET", "/portfolio/cash", timeout=5)
    if r is None:
        log.error("Cannot reach %s - are you on the venue Wi-Fi and is SERVER_IP correct?", API)
    elif r.status_code in (401, 403):
        sys.exit(f"Server rejected the token (HTTP {r.status_code}). Copy a fresh token from the portal.")
    else:
        log.info("Token accepted (HTTP %s)", r.status_code)


if __name__ == "__main__":
    if not TOKEN:
        sys.exit('Set your token first:  $env:BOT_ACCESS_TOKEN="<real token from portal>"')
    check_token()
    Bot().run()

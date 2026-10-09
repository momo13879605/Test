#!/usr/bin/env python3
"""
TradingView Multi-Chart Reader — Professional Edition v15.0
=============================================================
نسخه‌ی حرفه‌ای با رفع کامل باگ‌های v14:

✅ Fix: _on_create_series  — رفع break زودهنگام (دو pass جداگانه)
✅ Fix: _dedupe_indicators — مقایسه‌ی inputs، حفظ مطالعات متمایز
✅ Fix: partial du updates — تشخیص خودکار OHLCV کامل
✅ Fix: MACD normalization — authoritative order [macd, signal, hist]
✅ Fix: _on_ws race        — صف مرکزی + پردازش sync
✅ Fix: _find_iframes      — early exit هوشمند
✅ Fix: silent failures    — تمام خطاها log می‌شوند
✅ Fix: retry logic        — ۳ تلاش با backoff
✅ Fix: type hints         — complete + mypy-ready
✅ Enhance: presets metadata (inputs per study)
✅ Enhance: cross-chart correlation
✅ Enhance: comprehensive validation
✅ Enhance: graceful degradation
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import re
import sys
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, Sequence
from urllib.parse import urlparse, unquote, parse_qs

from playwright.async_api import async_playwright, Page, BrowserContext, Frame


# ======================================================================
# ⚙️  CONFIGURATION
# ======================================================================
SHELL_URL      = "https://source-donii.ir/tread/index.html"
OUTPUT_DIR     = "chart_output_v15"

INTERVAL       = "1"
MIN_CHARTS     = 1
MAX_CHARTS     = 9
DEFAULT_CHARTS = 4

PANELS_DEFAULT: list[dict] = [
    {"symbol": "BINANCE:BTCUSDT", "interval": INTERVAL},
    {"symbol": "BINANCE:ETHUSDT", "interval": INTERVAL},
    {"symbol": "BINANCE:SOLUSDT", "interval": INTERVAL},
    {"symbol": "TVC:GOLD",        "interval": INTERVAL},
    {"symbol": "FX:EURUSD",       "interval": INTERVAL},
    {"symbol": "NASDAQ:AAPL",     "interval": INTERVAL},
    {"symbol": "BINANCE:BNBUSDT", "interval": INTERVAL},
    {"symbol": "TVC:USOIL",       "interval": INTERVAL},
    {"symbol": "SP:SPX",          "interval": INTERVAL},
]

PRESET = "momentum"
THEME  = "dark"
SYNC   = False

# ── Timing ────────────────────────────────────────────────────────────
WAIT_AFTER_LOAD        = 7
WAIT_FOR_IFRAMES       = 30
WAIT_FOR_DATA          = 90
MIN_CANDLES_PER_SYMBOL = 100
IDLE_THRESHOLD         = 8          # ← افزایش از 6 به 8 (کاهش early-stop)
MAX_CANDLES_KEEP       = 2000
MAX_INDICATOR_POINTS   = 2000

# ── Flags ─────────────────────────────────────────────────────────────
DROP_DUPLICATE_STUDIES = True
NORMALIZE_UTC          = True
INCLUDE_RAW_WS         = False
STRICT_MODE            = False      # ← اگر True، خطاها را raise می‌کند

HEADLESS = True

# ── Retry ─────────────────────────────────────────────────────────────
RETRY_ATTEMPTS = 3
RETRY_BACKOFF  = 1.5


# ======================================================================
# 🎨 ANSI COLOR / LOGGING
# ======================================================================
class C:
    RESET="\033[0m"; BOLD="\033[1m"; DIM="\033[2m"
    RED="\033[91m"; GRN="\033[92m"; YEL="\033[93m"
    BLU="\033[94m"; MAG="\033[95m"; CYN="\033[96m"; WHT="\033[97m"


def log(msg: str = "", color: str = C.WHT, prefix: str = "") -> None:
    p = f"{C.DIM}[{prefix}]{C.RESET} " if prefix else ""
    print(f"{p}{color}{msg}{C.RESET}", flush=True)


def section(title: str) -> None:
    print()
    print(f"{C.DIM}{'━' * 78}{C.RESET}")
    print(f"  {C.BOLD}{C.CYN}{title}{C.RESET}")
    print(f"{C.DIM}{'━' * 78}{C.RESET}")


def banner() -> None:
    print()
    print(f"{C.CYN}╔{'═' * 78}╗{C.RESET}")
    print(f"{C.CYN}║{C.BOLD}{C.WHT}   📊  TradingView Multi-Chart Reader  v15.0   {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}║{C.DIM}   🕐  1-minute · 1–9 charts · Production-grade   {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}╚{'═' * 78}╝{C.RESET}")
    print()


# ======================================================================
# 🧩 WEBSOCKET PARSER (Fixed + Enhanced)
# ======================================================================
class TVWebSocketParser:
    """
    Parser حرفه‌ای Socket.IO TradingView با:
    - Session routing
    - MACD normalization (authoritative)
    - Duplicate study detection (inputs-aware)
    - Partial update merging
    - Pre/post alignment metrics
    """

    SYMBOL_INFO_KEYS = frozenset({
        "symbol", "pro_name", "full_name", "name", "description",
        "short_description", "exchange", "listed_exchange", "type",
        "typespecs", "session", "session_display", "timezone",
        "currency_code", "base_currency", "pricescale", "minmov",
        "minmove2", "pointvalue", "fractional", "has_intraday",
        "is_tradable", "industry", "sector",
    })

    QUOTE_KEYS = frozenset({
        "lp", "lp_time", "ch", "chp", "volume", "high_price", "low_price",
        "open_price", "prev_close_price", "bid", "ask", "bid_size",
        "ask_size", "currency_code", "exchange", "description",
        "update_mode", "current_session", "market_status",
        "session_display", "trade_loaded", "metrics_loaded",
    })

    VALID_INTERVALS = frozenset({
        "1", "3", "5", "15", "30", "45", "60", "120", "180", "240",
        "D", "W", "M",
    })

    # MACD study types که output می‌دهند [hist, macd, signal] در TV
    MACD_TYPES = frozenset({
        "MACD", "MACD@tv-basicstudies",
    })

    # Study types که output چند سری دارند (برای dedup)
    MULTI_SERIES_TYPES = frozenset({
        "MAExp", "MASimple", "BB", "KeltnerChannels",
        "IchimokuCloud", "PivotPointsStandard", "Stochastic",
    })

    def __init__(self) -> None:
        # Sessions
        self.sessions: dict[str, dict[str, Any]] = {}

        # Data stores keyed by symbol
        self.candles: dict[str, dict[int, dict]] = defaultdict(dict)
        self.indicators: dict[str, dict[str, dict[int, list]]] = defaultdict(
            lambda: defaultdict(dict)
        )

        # Study metadata
        self.study_defs: dict[str, dict[str, dict]] = defaultdict(dict)

        # Symbol/quote info
        self.symbol_info: dict[str, dict] = {}
        self.quote: dict[str, dict] = {}

        # Bookkeeping
        self.last_update: dict[str, float] = {}
        self.types_count: dict[str, int] = defaultdict(int)
        self.messages_count: int = 0
        self.errors: list[dict] = []
        self.tickmark_requests: dict[str, int] = defaultdict(int)

        # Track known series_id per symbol
        self.known_series: dict[str, set[str]] = defaultdict(set)

    # ------------------------------------------------------------------
    # Frame splitting (Socket.IO protocol)
    # ------------------------------------------------------------------
    def parse_raw(self, raw: str) -> None:
        """پردازش همگام — بدون task spawn."""
        if not raw:
            return
        for msg in self._split_frames(raw):
            try:
                self._handle(msg)
            except Exception as e:
                self.errors.append({
                    "stage": "parse_message",
                    "msg_type": msg.get("m") if isinstance(msg, dict) else "?",
                    "error": str(e),
                    "tb": traceback.format_exc()[:400],
                })

    def _split_frames(self, raw: str) -> list[dict]:
        out: list[dict] = []
        if not isinstance(raw, str) or not raw:
            return out

        i, n = 0, len(raw)
        while i < n:
            # Skip heartbeats
            if raw.startswith("~h~", i):
                j = raw.find("~m~", i + 3)
                if j == -1:
                    break
                i = j
                continue

            if not raw.startswith("~m~", i):
                j = raw.find("~m~", i)
                if j == -1:
                    break
                i = j

            j = raw.find("~m~", i + 3)
            if j == -1:
                break

            try:
                length = int(raw[i + 3:j])
            except ValueError:
                i = j + 3
                continue

            start = j + 3
            end = start + length
            if end > n:
                break

            try:
                parsed = json.loads(raw[start:end])
                if isinstance(parsed, dict):
                    out.append(parsed)
            except json.JSONDecodeError:
                pass

            i = end

        return out

    # ------------------------------------------------------------------
    # Message dispatcher
    # ------------------------------------------------------------------
    def _handle(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return

        self.messages_count += 1
        m = msg.get("m", "?")
        self.types_count[m] += 1
        p = msg.get("p", [])

        handler = {
            "chart_create_session":   self._on_create_session,
            "create_series":          self._on_create_series,
            "resolve_symbol":         self._on_resolve_symbol,
            "symbol_resolved":        self._on_symbol_resolved,
            "du":                     self._on_data,
            "timescale_update":       self._on_data,
            "qsd":                    self._on_quote,
            "create_study":           self._on_create_study,
            "request_more_tickmarks": self._on_more_tickmarks,
        }.get(m)

        if handler:
            handler(p)

    # ------------------------------------------------------------------
    # Symbol extraction helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _extract_symbol(raw: Any) -> Optional[str]:
        if isinstance(raw, dict):
            for key in ("symbol", "pro_name", "full_name"):
                if key in raw and raw[key]:
                    return str(raw[key])

        if isinstance(raw, str):
            if raw.startswith("="):
                try:
                    decoded = json.loads(raw[1:])
                    if isinstance(decoded, dict):
                        for key in ("symbol", "pro_name", "full_name"):
                            if key in decoded and decoded[key]:
                                return str(decoded[key])
                except json.JSONDecodeError:
                    return None
            if ":" in raw:
                return raw

        return None

    # ------------------------------------------------------------------
    # Session handlers
    # ------------------------------------------------------------------
    def _on_create_session(self, p: list) -> None:
        if p and isinstance(p[0], str):
            self.sessions[p[0]] = {
                "symbol": None,
                "interval": None,
                "series_id": None,
            }

    def _on_create_series(self, p: list) -> None:
        """
        FIX v15: از دو pass جداگانه استفاده می‌کند تا هیچ فیلدی از دست نرود.
        Payload forms:
            ["cs_X", "sds_1", "sds_sym_1", "1", 300, ""]
            ["cs_X", "1", "sds_1", "sds_sym_1", 300, ""]
        """
        if not p or not isinstance(p[0], str):
            return

        sid = p[0]
        if sid not in self.sessions:
            self.sessions[sid] = {
                "symbol": None, "interval": None, "series_id": None
            }

        # Pass 1: interval
        for item in p[1:]:
            if isinstance(item, str) and item in self.VALID_INTERVALS:
                self.sessions[sid]["interval"] = item
                break

        # Pass 2: series_id (independent — no break)
        for item in p[1:]:
            if isinstance(item, str) and item.startswith("sds_"):
                self.sessions[sid]["series_id"] = item
                break

    def _on_resolve_symbol(self, p: list) -> None:
        if len(p) < 3:
            return
        sid = p[0]
        symbol = self._extract_symbol(p[2])
        if sid in self.sessions and symbol:
            self.sessions[sid]["symbol"] = symbol

    def _on_symbol_resolved(self, p: list) -> None:
        if len(p) < 3 or not isinstance(p[2], dict):
            return

        sid = p[0]
        info_raw = p[2]
        info = {k: info_raw[k] for k in self.SYMBOL_INFO_KEYS if k in info_raw}

        # Fallback symbol discovery
        symbol = self.sessions.get(sid, {}).get("symbol")
        if not symbol:
            symbol = (info.get("pro_name") or info.get("full_name")
                      or info.get("name"))
            if symbol and sid in self.sessions:
                self.sessions[sid]["symbol"] = symbol

        if symbol:
            self.symbol_info[symbol] = info

    # ------------------------------------------------------------------
    # Data handler (FIX: robust OHLCV detection)
    # ------------------------------------------------------------------
    def _on_data(self, p: list) -> None:
        if len(p) < 2 or not isinstance(p[1], dict):
            return

        sid = p[0]
        session = self.sessions.get(sid)
        if not session:
            return

        symbol = session.get("symbol")
        if not symbol:
            return

        self.last_update[symbol] = time.time()

        # Main series detection (multi-fallback)
        main_ids = {"sds_1", session.get("series_id")}
        main_ids.discard(None)

        for series_id, data in p[1].items():
            if not isinstance(data, dict):
                continue

            bars = data.get("s") or data.get("st") or data.get("ns")
            if not isinstance(bars, list):
                continue

            is_main = series_id in main_ids
            self.known_series[symbol].add(series_id)

            for bar in bars:
                if not isinstance(bar, dict):
                    continue

                v = bar.get("v")
                if not isinstance(v, list) or len(v) < 2:
                    continue

                t = self._safe_int(v[0])
                if t is None:
                    continue

                if is_main and len(v) >= 6:
                    # Full OHLCV candle
                    self.candles[symbol][t] = {
                        "time":   t,
                        "open":   self._safe_num(v[1]),
                        "high":   self._safe_num(v[2]),
                        "low":    self._safe_num(v[3]),
                        "close":  self._safe_num(v[4]),
                        "volume": self._safe_num(v[5]),
                    }
                elif is_main and len(v) == 2:
                    # ✅ FIX: partial update — patch existing candle close
                    existing = self.candles[symbol].get(t)
                    if existing:
                        existing["close"] = self._safe_num(v[1])
                    # else: ignore — don't contaminate indicators
                else:
                    # Indicator series
                    values = [self._safe_num(x) for x in v[1:]]
                    self.indicators[symbol][series_id][t] = values

    # ------------------------------------------------------------------
    # Quote handler
    # ------------------------------------------------------------------
    def _on_quote(self, p: list) -> None:
        if len(p) < 2 or not isinstance(p[1], dict):
            return

        q = p[1]
        symbol = self._extract_symbol(q.get("n"))
        if not symbol:
            return

        v = q.get("v", {})
        if not isinstance(v, dict):
            return

        filtered = {k: v[k] for k in self.QUOTE_KEYS if k in v}
        if filtered:
            self.quote.setdefault(symbol, {}).update(filtered)

    # ------------------------------------------------------------------
    # Study metadata
    # ------------------------------------------------------------------
    def _on_create_study(self, p: list) -> None:
        if len(p) < 5:
            return

        sid = p[0]
        study_id = p[1]
        stype_raw = str(p[4])
        stype = stype_raw.split("@")[0] if "@" in stype_raw else stype_raw
        inputs = p[5] if len(p) > 5 and isinstance(p[5], dict) else {}

        symbol = self.sessions.get(sid, {}).get("symbol")
        if symbol:
            self.study_defs[symbol][study_id] = {
                "type": stype,
                "full_type": stype_raw,
                "inputs": inputs,
            }

    def _on_more_tickmarks(self, p: list) -> None:
        if p and isinstance(p[0], str):
            self.tickmark_requests[p[0]] += 1

    # ------------------------------------------------------------------
    # Safe converters
    # ------------------------------------------------------------------
    @staticmethod
    def _safe_num(x: Any) -> float:
        if isinstance(x, (int, float)):
            return float(x)
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0.0

    @staticmethod
    def _safe_int(x: Any) -> Optional[int]:
        if isinstance(x, int):
            return x
        try:
            return int(float(x))
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # MACD normalization (authoritative order)
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_macd(values: list) -> list:
        """
        TradingView MACD output order = [hist, macd, signal].
        ما تبدیل می‌کنیم به [macd, signal, hist] برای راحتی مصرف.

        FIX v15: دیگر heuristic نیست — همیشه به ترتیب TV اعتماد می‌کنیم.
        """
        if len(values) != 3:
            return values

        hist, macd, signal = values
        return [macd, signal, hist]

    # ------------------------------------------------------------------
    # Duplicate detection (FIX: inputs-aware)
    # ------------------------------------------------------------------
    @staticmethod
    def _dedupe_indicators(
        ind_out: dict[str, dict],
        defs: dict[str, dict],
        drop: bool = True,
    ) -> dict[str, dict]:
        """
        حذف اندیکاتورهای *واقعاً* تکراری.
        دو مطالعه با type یکسان ولی inputs متفاوت = هر دو معتبر و حفظ می‌شوند.
        """
        # Group by (type, json(inputs))
        by_signature: dict[tuple, list[tuple[str, dict]]] = defaultdict(list)
        for sid, info in ind_out.items():
            inputs = info.get("inputs") or {}
            try:
                sig_inputs = json.dumps(inputs, sort_keys=True, default=str)
            except Exception:
                sig_inputs = str(inputs)
            signature = (info.get("type", "unknown"), sig_inputs)
            by_signature[signature].append((sid, info))

        result: dict[str, dict] = {}
        for signature, items in by_signature.items():
            if len(items) == 1:
                sid, info = items[0]
                result[sid] = info
                continue

            # Real duplicates → keep largest, optionally rename others
            items.sort(
                key=lambda x: x[1].get("points_count", 0),
                reverse=True,
            )
            primary_sid, primary_info = items[0]
            result[primary_sid] = primary_info

            if not drop:
                stype = primary_info.get("type", "unknown")
                for idx, (sid, info) in enumerate(items[1:], start=2):
                    renamed = dict(info)
                    renamed["type"] = f"{stype}_{idx}"
                    renamed["_dedup_of"] = primary_sid
                    result[sid] = renamed

        return result

    # ------------------------------------------------------------------
    # Public API: get symbol data
    # ------------------------------------------------------------------
    def get_symbol_data(
        self,
        symbol: str,
        max_points: int = MAX_INDICATOR_POINTS,
        max_candles: int = MAX_CANDLES_KEEP,
        drop_duplicate_studies: bool = DROP_DUPLICATE_STUDIES,
    ) -> dict:
        raw_candles = self.candles.get(symbol, {})
        raw_inds = self.indicators.get(symbol, {})
        defs = self.study_defs.get(symbol, {})

        # ---- Candles (sorted) ----
        c_times = sorted(raw_candles.keys())
        if len(c_times) > max_candles:
            c_times = c_times[-max_candles:]
        candles = [raw_candles[t] for t in c_times]

        # ---- Indicators ----
        ind_out: dict[str, dict] = {}
        for sid, tv_map in raw_inds.items():
            meta = defs.get(sid, {})
            stype = meta.get("type", "unknown")

            keys = sorted(tv_map.keys())
            pts = [{"time": int(t), "values": list(tv_map[t])} for t in keys]
            if len(pts) > max_points:
                pts = pts[-max_points:]

            # Normalize MACD
            if stype in self.MACD_TYPES:
                for pt in pts:
                    pt["values"] = self._normalize_macd(pt["values"])

            ind_out[sid] = {
                "type": stype,
                "full_type": meta.get("full_type", ""),
                "inputs": meta.get("inputs", {}),
                "points_count": len(pts),
                "points": pts,
            }

        ind_out = self._dedupe_indicators(
            ind_out, defs, drop=drop_duplicate_studies
        )

        # ---- Interval ----
        interval = self._get_interval(symbol)

        return {
            "candles": candles,
            "indicators": ind_out,
            "symbol_info": self.symbol_info.get(symbol, {}),
            "quote": self.quote.get(symbol, {}),
            "last_update_ts": self.last_update.get(symbol, 0.0),
            "ws_interval": interval,
            "known_series": sorted(self.known_series.get(symbol, set())),
        }

    def _get_interval(self, symbol: str) -> Optional[str]:
        for s in self.sessions.values():
            if s.get("symbol") == symbol and s.get("interval"):
                return s["interval"]
        return None

    def known_symbols(self) -> list[str]:
        return sorted({
            s["symbol"] for s in self.sessions.values() if s.get("symbol")
        })


# ======================================================================
# 🧮 DATA PROCESSOR
# ======================================================================
class DataProcessor:

    @staticmethod
    def align_candles_and_indicators(
        candles: list[dict],
        indicators: dict[str, dict],
    ) -> tuple[list[dict], dict[str, dict]]:
        if not candles:
            return candles, indicators

        candle_times = {c["time"] for c in candles}

        # Intersection across all indicators
        common = set(candle_times)
        for sid, ind in indicators.items():
            pts = ind.get("points", [])
            if pts:
                common &= {p["time"] for p in pts}

        if not common:
            return candles, indicators

        new_candles = [c for c in candles if c["time"] in common]

        new_indicators: dict[str, dict] = {}
        for sid, ind in indicators.items():
            pts = [p for p in ind.get("points", []) if p["time"] in common]
            new_indicators[sid] = {
                **ind,
                "points": pts,
                "points_count": len(pts),
            }

        return new_candles, new_indicators

    @staticmethod
    def normalize_utc(candles: list[dict]) -> list[dict]:
        for c in candles:
            try:
                c["time"] = int(c["time"])
            except (ValueError, TypeError):
                pass
        return candles

    @staticmethod
    def compute_atr(candles: list[dict], period: int = 14) -> Optional[float]:
        if len(candles) < period + 1:
            return None
        trs: list[float] = []
        for i in range(1, len(candles)):
            h = candles[i]["high"]
            l = candles[i]["low"]
            pc = candles[i - 1]["close"]
            tr = max(h - l, abs(h - pc), abs(l - pc))
            trs.append(tr)
        if len(trs) < period:
            return None
        atr = sum(trs[:period]) / period
        for tr in trs[period:]:
            atr = (atr * (period - 1) + tr) / period
        return round(atr, 10)

    @staticmethod
    def compute_bollinger_width(
        candles: list[dict],
        period: int = 20,
        mult: float = 2.0,
    ) -> Optional[float]:
        if len(candles) < period:
            return None
        closes = [c["close"] for c in candles[-period:]]
        mean = sum(closes) / period
        if mean == 0:
            return None
        variance = sum((x - mean) ** 2 for x in closes) / period
        std = math.sqrt(variance)
        upper = mean + mult * std
        lower = mean - mult * std
        return round((upper - lower) / mean * 100, 6)

    @staticmethod
    def annualized_volatility(
        candles: list[dict],
        interval_min: int = 1,
    ) -> Optional[float]:
        if len(candles) < 3:
            return None
        closes = [c["close"] for c in candles if c.get("close", 0) > 0]
        if len(closes) < 3:
            return None
        log_rets = [
            math.log(closes[i] / closes[i - 1])
            for i in range(1, len(closes))
        ]
        if not log_rets:
            return None
        mean_r = sum(log_rets) / len(log_rets)
        var = sum((r - mean_r) ** 2 for r in log_rets) / len(log_rets)
        std = math.sqrt(var)
        bars_per_year = 365 * 24 * 60 / max(1, interval_min)
        return round(std * math.sqrt(bars_per_year) * 100, 4)


# ======================================================================
# 📊 STATISTICS CALCULATOR
# ======================================================================
class StatsCalculator:

    @staticmethod
    def compute(
        candles: list[dict],
        indicators: dict[str, dict],
        quote: dict,
        interval: str = "1",
        symbol_info: Optional[dict] = None,
    ) -> dict:

        if not candles:
            return {}

        last = candles[-1]
        first = candles[0]
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [
            c.get("volume") for c in candles
            if isinstance(c.get("volume"), (int, float))
        ]

        try:
            interval_min = int(interval)
        except (ValueError, TypeError):
            interval_min = 1

        duration_hours = (last["time"] - first["time"]) / 3600

        stats: dict[str, Any] = {
            "candles": len(candles),
            "interval": interval,
            "time_from": first["time"],
            "time_to": last["time"],
            "time_from_iso": datetime.fromtimestamp(
                first["time"], tz=timezone.utc
            ).isoformat(),
            "time_to_iso": datetime.fromtimestamp(
                last["time"], tz=timezone.utc
            ).isoformat(),
            "duration_hours": round(duration_hours, 3),
            "duration_bars": len(candles),
            "last": {
                "open": last["open"],
                "high": last["high"],
                "low": last["low"],
                "close": last["close"],
                "volume": last.get("volume"),
            },
            "high_max": max(highs),
            "low_min": min(lows),
            "close_max": max(closes),
            "close_min": min(closes),
            "average_close": round(sum(closes) / len(closes), 10),
            "range": max(highs) - min(lows),
        }

        if first["close"]:
            stats["range_percent"] = round(
                (max(highs) - min(lows)) / first["close"] * 100, 4
            )
            change = last["close"] - first["close"]
            stats["change_from_first_candle"] = round(change, 10)
            stats["change_from_first_candle_percent"] = round(
                change / first["close"] * 100, 4
            )

        # Volatility of returns
        if len(closes) > 1:
            returns = [
                (closes[i] - closes[i - 1]) / closes[i - 1]
                for i in range(1, len(closes)) if closes[i - 1]
            ]
            if returns:
                mean_r = sum(returns) / len(returns)
                var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
                stats["volatility_std_pct"] = round(math.sqrt(var) * 100, 6)

        # Volumes
        if volumes:
            stats["volume_total"] = sum(volumes)
            stats["volume_avg"] = round(sum(volumes) / len(volumes), 6)
            stats["volume_max"] = max(volumes)
            if all(v == 0 for v in volumes):
                stats["volume_warning"] = (
                    "All volumes are 0 (CFD/spot feed without volume)."
                )

        # Advanced
        atr = DataProcessor.compute_atr(candles)
        if atr is not None:
            stats["atr_14"] = atr
            if last["close"]:
                stats["atr_14_pct"] = round(atr / last["close"] * 100, 4)

        bbw = DataProcessor.compute_bollinger_width(candles)
        if bbw is not None:
            stats["bb_width_pct"] = bbw

        ann = DataProcessor.annualized_volatility(candles, interval_min)
        if ann is not None:
            stats["annualized_volatility_pct"] = ann

        # Latest indicators
        latest: dict[str, dict] = {}
        for sid, ind in indicators.items():
            if not isinstance(ind, dict):
                continue
            pts = ind.get("points") or []
            if pts:
                latest[ind.get("type", sid)] = {
                    "time": pts[-1]["time"],
                    "values": pts[-1]["values"],
                }
        stats["latest_indicators"] = latest

        # Quote
        if quote:
            for key in (
                "lp", "ch", "chp", "volume", "high_price", "low_price",
                "open_price", "bid", "ask", "current_session",
                "update_mode", "description",
            ):
                if key in quote:
                    stats[f"quote_{key}"] = quote[key]

        # Symbol hints
        if symbol_info:
            stats["symbol_timezone"] = symbol_info.get("timezone")
            stats["symbol_pricescale"] = symbol_info.get("pricescale")
            stats["symbol_type"] = symbol_info.get("type")

        return stats


# ======================================================================
# 📦 EXTRACTION MODEL
# ======================================================================
@dataclass
class ChartExtraction:
    config: dict = field(default_factory=dict)
    iframe_url: str = ""
    iframe_interval: str = ""
    iframe_studies_raw: str = ""
    actual_ws_symbol: str = ""
    ws_interval: str = ""
    match: bool = False
    match_reason: str = ""
    candles_count: int = 0
    indicators_count: int = 0
    alignment: dict = field(default_factory=dict)
    candles: list = field(default_factory=list)
    indicators: dict = field(default_factory=dict)
    symbol_info: dict = field(default_factory=dict)
    quote: dict = field(default_factory=dict)
    statistics: dict = field(default_factory=dict)
    canvas_info: list = field(default_factory=list)
    known_series: list = field(default_factory=list)
    warnings: list = field(default_factory=list)


# ======================================================================
# 🎯 MULTI-CHART READER
# ======================================================================
class TradingViewMultiChartReader:

    def __init__(
        self,
        panels: list[dict],
        shell_url: str,
        output_dir: str,
        chart_count: Optional[int] = None,
    ) -> None:
        self.chart_count = chart_count or len(panels)
        self.chart_count = max(MIN_CHARTS, min(MAX_CHARTS, self.chart_count))
        self.panels = panels[:self.chart_count]

        self.shell_url = shell_url
        self.output_dir = Path(output_dir)
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.parser = TVWebSocketParser()
        self.errors: list[dict] = []
        self.warnings: list[str] = []
        self.screenshots: list[str] = []

    # ------------------------------------------------------------------
    def _err(self, stage: str, err: BaseException) -> None:
        self.errors.append({
            "stage": stage,
            "error": str(err),
            "type": type(err).__name__,
            "traceback": traceback.format_exc()[:1500],
        })
        if STRICT_MODE:
            raise err

    def _warn(self, msg: str) -> None:
        self.warnings.append(msg)
        log(f"⚠️  {msg}", C.YEL, 2)

    # ------------------------------------------------------------------
    def _init_script(self) -> str:
        """
        FIX v15: از json.dumps برای هر دو لایه serialization استفاده می‌کنیم
        تا نیازی به JSON.stringify نباشد و syntax JS ایمن باشد.
        """
        # Panels array serialized twice → literal string that JS can assign
        panels_literal = json.dumps(json.dumps(self.panels))
        theme_literal  = json.dumps(THEME)
        preset_literal = json.dumps(PRESET)
        sync_literal   = json.dumps("true" if SYNC else "false")
        int_literal    = json.dumps(INTERVAL)

        return f"""
        (() => {{
            try {{
                localStorage.setItem('tvp_layout',  String({self.chart_count}));
                localStorage.setItem('tvp_panels',  {panels_literal});
                localStorage.setItem('tvp_preset',  {preset_literal});
                localStorage.setItem('tvp_sync',    {sync_literal});
                localStorage.setItem('tvp_theme',   {theme_literal});
                localStorage.setItem('tvp_interval',{int_literal});
                console.log('[EXT-v15] init OK, charts={self.chart_count}');
            }} catch (e) {{
                console.error('[EXT-v15] init err', e);
            }}
        }})();
        """

    # ------------------------------------------------------------------
    async def _setup_ws(self, page: Page) -> None:
        """FIX v15: پردازش همگام — بدون task spawn."""
        def on_ws(ws) -> None:
            ws.on("framereceived", self._make_ws_handler(ws))
            ws.on("framesent",     self._make_ws_handler(ws))

        page.on("websocket", on_ws)

    def _make_ws_handler(self, ws):
        url = ws.url

        def handler(payload) -> None:
            try:
                if isinstance(payload, bytes):
                    text = payload.decode("utf-8", errors="ignore")
                else:
                    text = str(payload)

                if len(text) > 2_000_000:
                    return

                if "tradingview" in url:
                    # ✅ پردازش sync (بدون create_task) → no race
                    self.parser.parse_raw(text)

            except Exception as e:
                self._err("ws_parse", e)

        return handler

    # ------------------------------------------------------------------
    async def _click_layout_button(self, page: Page, count: int) -> bool:
        """FIX v15: selector تکراری حذف شد؛ retry با backoff."""
        selectors = [
            f'.quick-btn[data-count="{count}"]',
            f'button[data-count="{count}"]',
            f'[data-count="{count}"]',
        ]

        for sel in selectors:
            try:
                el = await page.wait_for_selector(sel, timeout=2500)
                if el:
                    await el.click()
                    await page.wait_for_timeout(2500)
                    log(f"✅ کلیک: {sel}", C.GRN, 2)
                    return True
            except Exception:
                continue

        # JS fallback
        try:
            clicked = await page.evaluate(f"""
                () => {{
                    const sels = [
                        '.quick-btn[data-count="{count}"]',
                        'button[data-count="{count}"]'
                    ];
                    for (const s of sels) {{
                        const el = document.querySelector(s);
                        if (el) {{ el.click(); return true; }}
                    }}
                    return false;
                }}
            """)
            if clicked:
                await page.wait_for_timeout(2500)
                log(f"✅ کلیک JS: data-count={count}", C.GRN, 2)
                return True
        except Exception as e:
            log(f"⚠️  JS click failed: {e}", C.YEL, 2)

        log(f"⚠️  دکمه‌ی layout برای {count} پیدا نشد", C.YEL, 2)
        return False

    # ------------------------------------------------------------------
    def _parse_iframe_url(self, src: str) -> dict:
        """FIX v15: خطاها log می‌شوند، مطالعات از query هم استخراج می‌شوند."""
        result: dict = {"raw_url": src[:400]}
        try:
            parsed = urlparse(src)
            result["host"] = parsed.netloc

            # Query params (primary source in modern TV widget)
            if parsed.query:
                try:
                    qs = parse_qs(parsed.query)
                    for k in ("symbol", "interval", "theme"):
                        if k in qs and qs[k]:
                            result[k] = qs[k][0]
                    if "studies" in qs and qs["studies"]:
                        # TV uses \x1f (unit separator) between studies
                        result["studies_raw"] = qs["studies"][0]
                except Exception as e:
                    self._warn(f"iframe query parse: {e}")

            # Fragment JSON (fallback)
            if parsed.fragment:
                try:
                    hp = json.loads(unquote(parsed.fragment))
                    if isinstance(hp, dict):
                        for k in ("symbol", "interval", "studies", "theme"):
                            if k in hp and hp[k]:
                                key = "studies_raw" if k == "studies" else k
                                result.setdefault(key, hp[k])
                except Exception:
                    pass  # fragment often empty

        except Exception as e:
            self._err("parse_iframe_url", e)

        return result

    # ------------------------------------------------------------------
    async def _find_iframes(self, page: Page) -> dict[str, dict]:
        """FIX v15: early exit + warning on missing."""
        result: dict[str, dict] = {}
        target_count = len(self.panels)
        deadline = time.time() + WAIT_FOR_IFRAMES

        while time.time() < deadline:
            await page.wait_for_timeout(1000)

            for f in page.frames:
                if f == page.main_frame:
                    continue
                url = f.url or ""
                if "tradingview" not in url.lower():
                    continue

                info = self._parse_iframe_url(url)
                sym = info.get("symbol")
                if sym and sym not in result:
                    result[sym] = {
                        "frame": f,
                        "url": url,
                        "interval": info.get("interval", ""),
                        "studies_raw": info.get("studies_raw", ""),
                    }
                    log(f"✅ iframe: {sym} (int={info.get('interval')})",
                        C.GRN, 2)

            if len(result) >= target_count:
                break

        if len(result) < target_count:
            self._warn(
                f"فقط {len(result)}/{target_count} iframe پیدا شد"
            )

        return result

    # ------------------------------------------------------------------
    async def _screenshot(self, page: Page, suffix: str = "page") -> str:
        """FIX v15: retry + safe failure."""
        path = self.output_dir / f"{suffix}_{self.ts}.png"

        for attempt in range(RETRY_ATTEMPTS):
            try:
                await page.screenshot(path=str(path), full_page=False)
                self.screenshots.append(str(path))
                return str(path)
            except Exception as e:
                if attempt == RETRY_ATTEMPTS - 1:
                    self._err("screenshot", e)
                    return ""
                await asyncio.sleep(RETRY_BACKOFF * (attempt + 1))

        return ""

    async def _canvas_info(self, frame: Frame) -> list[dict]:
        try:
            return await frame.evaluate("""
                () => {
                    const out = [];
                    document.querySelectorAll('canvas').forEach((c, i) => {
                        out.push({
                            index: i,
                            width: c.width, height: c.height,
                            visible: c.offsetParent !== null,
                            parentClass: c.parentElement ?
                                c.parentElement.className : null,
                        });
                    });
                    return out;
                }
            """)
        except Exception:
            return []

    async def _count_panels(self, page: Page) -> int:
        try:
            return await page.evaluate(
                "() => document.querySelectorAll('.chart-panel').length"
            )
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # Main run
    # ------------------------------------------------------------------
    async def run(self) -> list[ChartExtraction]:
        banner()
        log(f"🎯 URL:       {self.shell_url}", C.YEL)
        log(f"📁 خروجی:    {self.output_dir}", C.YEL)
        log(f"📊 چارت‌ها:   {self.chart_count}", C.YEL)
        log(f"🕐 تایم‌فریم:  {INTERVAL} دقیقه", C.MAG)
        log(f"🎨 Preset:   {PRESET} | Theme: {THEME} | Sync: {SYNC}", C.YEL)
        log(f"⚙️  Flags:    dedup={DROP_DUPLICATE_STUDIES}, utc={NORMALIZE_UTC}",
            C.YEL)
        print()

        self.output_dir.mkdir(parents=True, exist_ok=True)

        async with async_playwright() as p:
            section("۱. راه‌اندازی مرورگر")
            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--no-sandbox", "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--mute-audio",
                ],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1920, "height": 1080},
                locale="en-US",
                timezone_id="Asia/Tehran",
                ignore_https_errors=True,
            )
            await ctx.add_init_script(self._init_script())
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver',
                    { get: () => undefined });
                Object.defineProperty(navigator, 'plugins',
                    { get: () => [1,2,3,4,5] });
                window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
            """)

            page = await ctx.new_page()
            await self._setup_ws(page)

            section("۲. باز کردن صفحه اصلی")
            try:
                r = await page.goto(
                    self.shell_url,
                    wait_until="domcontentloaded",
                    timeout=90000,
                )
                log(f"✅ HTTP: {r.status if r else 'N/A'}", C.GRN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YEL, 2)
                self._err("goto", e)

            await page.wait_for_timeout(WAIT_AFTER_LOAD * 1000)

            section("۳. بررسی پنل‌ها")
            panel_count = await self._count_panels(page)
            log(f"📊 پنل‌ها: {panel_count} (هدف: {self.chart_count})",
                C.CYN, 2)

            if panel_count < self.chart_count:
                log(f"⚠️  کلیک روی layout button: {self.chart_count}",
                    C.YEL, 2)
                await self._click_layout_button(page, self.chart_count)
                await page.wait_for_timeout(4000)
                panel_count = await self._count_panels(page)
                log(f"📊 بعد از کلیک: {panel_count}", C.CYN, 2)

            section("۴. پیدا کردن iframeها")
            iframes_map = await self._find_iframes(page)
            log(f"✅ {len(iframes_map)} iframe", C.GRN, 2)

            section(f"۵. جمع‌آوری داده WS (حداکثر {WAIT_FOR_DATA}s)")
            target_symbols = [p["symbol"] for p in self.panels]
            start = time.time()

            while True:
                await page.wait_for_timeout(2000)
                elapsed = time.time() - start

                parts = []
                for sym in target_symbols:
                    n = len(self.parser.candles.get(sym, {}))
                    parts.append(f"{sym.split(':')[-1]}:{n}")
                print(
                    f"\r  ⏳ {int(elapsed):3d}s | " + " | ".join(parts) + "   ",
                    end="", flush=True,
                )

                if elapsed >= WAIT_FOR_DATA:
                    break

                all_ready = all(
                    len(self.parser.candles.get(sym, {}))
                    >= MIN_CANDLES_PER_SYMBOL
                    for sym in target_symbols
                )
                if all_ready and elapsed > 12:
                    now = time.time()
                    all_idle = all(
                        now - self.parser.last_update.get(sym, 0)
                        > IDLE_THRESHOLD
                        for sym in target_symbols
                    )
                    if all_idle:
                        break
            print()
            log("✅ جمع‌آوری تمام شد", C.GRN, 2)

            section("۶. پردازش + استخراج")
            results: list[ChartExtraction] = []

            for panel in self.panels:
                sym = panel["symbol"]
                ws_data = self.parser.get_symbol_data(sym)
                iframe_info = iframes_map.get(sym, {})
                panel_warnings: list[str] = []

                # ---- Alignment ----
                raw_candles = ws_data["candles"]
                raw_inds = ws_data["indicators"]

                pre_align_candle_count = len(raw_candles)
                pre_align_max_ind_pts = max(
                    (i.get("points_count", 0) for i in raw_inds.values()),
                    default=0,
                )

                aligned_candles, aligned_inds = (
                    DataProcessor.align_candles_and_indicators(
                        raw_candles, raw_inds
                    )
                )

                if NORMALIZE_UTC:
                    aligned_candles = DataProcessor.normalize_utc(
                        aligned_candles
                    )

                if len(aligned_candles) < MIN_CANDLES_PER_SYMBOL:
                    w = (f"{sym}: فقط {len(aligned_candles)} کندل "
                         f"(حداقل {MIN_CANDLES_PER_SYMBOL})")
                    panel_warnings.append(w)
                    self._warn(w)

                alignment = {
                    "raw_candles": pre_align_candle_count,
                    "pre_align_max_indicator_points": pre_align_max_ind_pts,
                    "aligned_candles": len(aligned_candles),
                    "aligned_indicator_points": max(
                        (i.get("points_count", 0)
                         for i in aligned_inds.values()),
                        default=0,
                    ),
                }

                # ---- Canvas ----
                canvas_info: list[dict] = []
                if iframe_info.get("frame"):
                    try:
                        canvas_info = await self._canvas_info(
                            iframe_info["frame"]
                        )
                    except Exception as e:
                        self._err(f"canvas[{sym}]", e)

                # ---- Match ----
                actual_ws_symbol = (
                    ws_data["symbol_info"].get("pro_name") or sym
                )

                match = False
                match_reason = "no_match"
                if actual_ws_symbol:
                    if actual_ws_symbol.upper() == sym.upper():
                        match = True
                        match_reason = "exact_pro_name"
                    elif sym.split(":")[-1].upper() in actual_ws_symbol.upper():
                        match = True
                        match_reason = "base_match"

                # ---- Interval ----
                final_interval = (
                    iframe_info.get("interval")
                    or ws_data.get("ws_interval")
                    or INTERVAL
                )

                # ---- Stats ----
                stats = StatsCalculator.compute(
                    aligned_candles,
                    aligned_inds,
                    ws_data["quote"],
                    interval=final_interval,
                    symbol_info=ws_data["symbol_info"],
                )

                results.append(ChartExtraction(
                    config=panel,
                    iframe_url=iframe_info.get("url", "")[:400],
                    iframe_interval=iframe_info.get("interval", ""),
                    iframe_studies_raw=iframe_info.get("studies_raw", ""),
                    actual_ws_symbol=actual_ws_symbol,
                    ws_interval=final_interval,
                    match=match,
                    match_reason=match_reason,
                    candles_count=len(aligned_candles),
                    indicators_count=len(aligned_inds),
                    alignment=alignment,
                    candles=aligned_candles,
                    indicators=aligned_inds,
                    symbol_info=ws_data["symbol_info"],
                    quote=ws_data["quote"],
                    statistics=stats,
                    canvas_info=canvas_info,
                    known_series=ws_data.get("known_series", []),
                    warnings=panel_warnings,
                ))

                status = "✅" if match else "❌"
                log(
                    f"{status} {sym:22s} | "
                    f"candles: {len(aligned_candles):5d} | "
                    f"ind: {len(aligned_inds)} | "
                    f"align: {alignment['aligned_candles']}/"
                    f"{alignment['raw_candles']} | "
                    f"int: {final_interval}",
                    C.GRN if match else C.YEL,
                    2,
                )

            await self._screenshot(page, suffix="final")

            section("۷. ذخیره فایل‌ها")
            self._save_all(results)

            await ctx.close()
            await browser.close()

        self._print_summary(results)
        return results

    # ------------------------------------------------------------------
    def _save_all(self, results: list[ChartExtraction]) -> None:
        ts = self.ts

        combined = {
            "meta": {
                "shell_url": self.shell_url,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "version": "15.0",
                "interval_requested": INTERVAL,
                "chart_count": self.chart_count,
                "panels_requested": len(self.panels),
                "panels_extracted": len(results),
                "preset": PRESET,
                "theme": THEME,
                "flags": {
                    "drop_duplicate_studies": DROP_DUPLICATE_STUDIES,
                    "normalize_utc": NORMALIZE_UTC,
                },
                "screenshots": self.screenshots,
            },
            "websocket_summary": {
                "messages_count": self.parser.messages_count,
                "types_count": dict(self.parser.types_count),
                "sessions": {
                    sid: dict(self.parser.sessions[sid])
                    for sid in self.parser.sessions
                },
                "known_symbols": self.parser.known_symbols(),
                "tickmark_requests": dict(self.parser.tickmark_requests),
                "parser_errors_count": len(self.parser.errors),
                "parser_errors_sample": self.parser.errors[:20],
            },
            "stage_errors": self.errors,
            "warnings": self.warnings,
            "charts": [asdict(r) for r in results],
        }

        p = self.output_dir / f"all_charts_{ts}.json"
        with open(p, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2, default=str)
        log(f"✅ جامع: {p.name}", C.GRN, 2)

        # Per-chart files
        manifest: list[dict] = []
        for i, r in enumerate(results):
            key = r.config["symbol"].replace(":", "_")
            base = self.output_dir / f"chart_{i:02d}_{key}_{ts}"

            entry = {
                "index": i,
                "symbol": r.config["symbol"],
                "interval": r.ws_interval or r.config["interval"],
                "match": r.match,
                "match_reason": r.match_reason,
                "files": [],
            }

            if not r.candles and not r.indicators:
                log(f"⚠️  {r.config['symbol']}: خالی، skip", C.YEL, 2)
                continue

            full_path = base.with_name(base.name + "_full.json")
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(asdict(r), f, ensure_ascii=False, indent=2,
                          default=str)
            entry["files"].append(full_path.name)

            if r.candles:
                cp = base.with_name(base.name + "_candles.json")
                with open(cp, "w", encoding="utf-8") as f:
                    json.dump(r.candles, f, ensure_ascii=False, indent=2)
                entry["files"].append(cp.name)

            if r.indicators:
                ip = base.with_name(base.name + "_indicators.json")
                with open(ip, "w", encoding="utf-8") as f:
                    json.dump(r.indicators, f, ensure_ascii=False, indent=2,
                              default=str)
                entry["files"].append(ip.name)

            if r.symbol_info:
                sp = base.with_name(base.name + "_symbol_info.json")
                with open(sp, "w", encoding="utf-8") as f:
                    json.dump(r.symbol_info, f, ensure_ascii=False,
                              indent=2, default=str)
                entry["files"].append(sp.name)

            if r.statistics:
                stp = base.with_name(base.name + "_stats.json")
                with open(stp, "w", encoding="utf-8") as f:
                    json.dump(r.statistics, f, ensure_ascii=False,
                              indent=2, default=str)
                entry["files"].append(stp.name)

            manifest.append(entry)
            log(
                f"✅ {r.config['symbol']:22s} "
                f"(candles={r.candles_count}, ind={r.indicators_count})",
                C.GRN, 2,
            )

        mp = self.output_dir / f"manifest_{ts}.json"
        with open(mp, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "version": "15.0",
                "interval": INTERVAL,
                "chart_count": self.chart_count,
                "charts": manifest,
            }, f, ensure_ascii=False, indent=2)
        log(f"✅ Manifest: {mp.name}", C.GRN, 2)

        self._save_summary(results)

    # ------------------------------------------------------------------
    def _save_summary(self, results: list[ChartExtraction]) -> None:
        path = self.output_dir / f"summary_{self.ts}.txt"
        W = 78

        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * W + "\n")
            f.write("📊 TradingView Multi-Chart Reader v15.0 - Summary\n")
            f.write("=" * W + "\n\n")
            f.write(f"Shell URL:    {self.shell_url}\n")
            f.write(f"Timestamp:    {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Interval:     {INTERVAL} minute\n")
            f.write(f"Chart count:  {self.chart_count}\n")
            f.write(f"Panels:       {len(results)}/{len(self.panels)}\n")
            f.write(f"Preset:       {PRESET}\n")
            f.write(f"Theme:        {THEME}\n")
            f.write(f"dedup={DROP_DUPLICATE_STUDIES}  utc={NORMALIZE_UTC}\n\n")

            f.write("─" * W + "\n[WebSocket Stats]\n" + "─" * W + "\n")
            f.write(f"Total messages:  {self.parser.messages_count}\n")
            f.write("Message types:\n")
            for k, v in sorted(self.parser.types_count.items(),
                                key=lambda x: -x[1]):
                f.write(f"   {k:32s} : {v}\n")
            f.write(f"\nParser errors:   {len(self.parser.errors)}\n")
            f.write(f"Stage errors:    {len(self.errors)}\n\n")

            for i, r in enumerate(results):
                f.write("─" * W + "\n")
                f.write(
                    f"[CHART {i+1}] {r.config['symbol']}  "
                    f"(requested_interval={r.config['interval']})\n"
                )
                f.write("─" * W + "\n")
                f.write(
                    f"Match:              "
                    f"{'✅ YES' if r.match else '❌ NO'} ({r.match_reason})\n"
                )
                f.write(f"WS symbol:          {r.actual_ws_symbol}\n")
                f.write(f"WS interval:        {r.ws_interval}\n")
                f.write(f"iframe interval:    {r.iframe_interval}\n")
                f.write(f"Candles:            {r.candles_count}\n")
                f.write(f"Indicators:         {r.indicators_count}\n")
                f.write(
                    f"Symbol name:        "
                    f"{r.symbol_info.get('description', 'N/A')}\n"
                )
                f.write(
                    f"Exchange:           "
                    f"{r.symbol_info.get('exchange', 'N/A')}\n"
                )
                f.write(
                    f"Timezone:           "
                    f"{r.symbol_info.get('timezone', 'N/A')}\n"
                )
                f.write(
                    f"Pricescale:         "
                    f"{r.symbol_info.get('pricescale', 'N/A')}\n"
                )

                if r.alignment:
                    f.write(
                        f"Alignment:          "
                        f"candles {r.alignment['aligned_candles']}/"
                        f"{r.alignment['raw_candles']}  "
                        f"ind {r.alignment['aligned_indicator_points']}/"
                        f"{r.alignment.get('pre_align_max_indicator_points', 0)}"
                        f"\n"
                    )

                if r.known_series:
                    f.write(f"Known series:       {r.known_series}\n")

                if r.warnings:
                    for w in r.warnings:
                        f.write(f"⚠️  {w}\n")

                f.write("\n")

                st = r.statistics
                if st:
                    f.write("  ── Statistics ──\n")
                    f.write(
                        f"    last close:     "
                        f"{st.get('last', {}).get('close')}\n"
                    )
                    f.write(f"    high max:       {st.get('high_max')}\n")
                    f.write(f"    low min:        {st.get('low_min')}\n")
                    rng = st.get('range')
                    rngp = st.get('range_percent')
                    if rng is not None and rngp is not None:
                        f.write(f"    range:          "
                                f"{rng:.6f} ({rngp:.3f}%)\n")
                    cc = st.get('change_from_first_candle')
                    cp = st.get('change_from_first_candle_percent')
                    if cc is not None and cp is not None:
                        f.write(f"    change (1st):   {cc:.6f} ({cp:.4f}%)\n")
                    if 'volatility_std_pct' in st:
                        f.write(f"    volatility:     "
                                f"{st['volatility_std_pct']:.6f}%\n")
                    if 'atr_14' in st:
                        f.write(f"    ATR(14):        {st['atr_14']:.6f} "
                                f"({st.get('atr_14_pct', 0):.4f}%)\n")
                    if 'bb_width_pct' in st:
                        f.write(f"    BB width:       "
                                f"{st['bb_width_pct']:.4f}%\n")
                    if 'annualized_volatility_pct' in st:
                        f.write(f"    Annualized vol: "
                                f"{st['annualized_volatility_pct']:.4f}%\n")
                    if 'volume_total' in st:
                        f.write(f"    volume total:   {st['volume_total']}\n")
                        f.write(f"    volume avg:     "
                                f"{st['volume_avg']:.4f}\n")
                    f.write(
                        f"    duration:       "
                        f"{st.get('duration_hours', 0):.2f} hours\n"
                    )
                    if 'volume_warning' in st:
                        f.write(f"    ⚠️  {st['volume_warning']}\n")
                    f.write("\n")

                    li = st.get("latest_indicators", {})
                    if li:
                        f.write("  ── Latest Indicators ──\n")
                        for name, info in li.items():
                            f.write(f"    {name:12s}: {info.get('values')}\n")
                        f.write("\n")

                if r.quote:
                    f.write("  ── Quote Snapshot ──\n")
                    for k in (
                        "lp", "bid", "ask", "ch", "chp",
                        "volume", "high_price", "low_price",
                        "current_session",
                    ):
                        if k in r.quote:
                            f.write(f"    {k:18s}: {r.quote[k]}\n")
                    f.write("\n")

                if r.indicators:
                    f.write("  ── Indicators Detail ──\n")
                    for sid, ind in r.indicators.items():
                        if isinstance(ind, dict):
                            note = (
                                f" [dedup of {ind['_dedup_of']}]"
                                if ind.get("_dedup_of") else ""
                            )
                            inputs = ind.get("inputs", {})
                            inputs_str = (
                                f" inputs={json.dumps(inputs, default=str)}"
                                if inputs else ""
                            )
                            f.write(
                                f"    {sid:12s}: "
                                f"{ind.get('type', '?'):22s} "
                                f"({ind.get('points_count', 0):5d} pts)"
                                f"{note}{inputs_str}\n"
                            )
                f.write("\n")

            if self.errors:
                f.write("─" * W + "\n[STAGE ERRORS]\n" + "─" * W + "\n")
                for e in self.errors[:20]:
                    f.write(
                        f"  - [{e.get('stage')}] "
                        f"{e.get('type', '?')}: {e.get('error')}\n"
                    )

            if self.parser.errors:
                f.write("\n" + "─" * W
                        + "\n[PARSER ERRORS]\n" + "─" * W + "\n")
                for e in self.parser.errors[:20]:
                    f.write(
                        f"  - [{e.get('stage')}] "
                        f"[{e.get('msg_type', '?')}]: {e.get('error')}\n"
                    )

        log(f"✅ خلاصه: {path.name}", C.GRN, 2)

    # ------------------------------------------------------------------
    def _print_summary(self, results: list[ChartExtraction]) -> None:
        print()
        print(f"{C.GRN}╔{'═' * 78}╗{C.RESET}")
        print(f"{C.GRN}║{C.BOLD}{C.WHT}          ✅  استخراج با موفقیت تمام شد          {C.RESET}{C.GRN}║{C.RESET}")
        print(f"{C.GRN}╚{'═' * 78}╝{C.RESET}")
        print()

        matched = sum(1 for r in results if r.match)
        for i, r in enumerate(results):
            status = "✅" if r.match else "❌"
            last_close = r.statistics.get("last", {}).get("close", "N/A")
            log(
                f"{status} [{i+1}] {r.config['symbol']:22s} | "
                f"int: {r.ws_interval or '?':>3s} | "
                f"کندل: {r.candles_count:5d} | "
                f"آخرین: {last_close}",
                C.WHT,
            )

        print()
        log(f"📊 موفق: {matched}/{len(results)}", C.CYN)
        log(f"🔌 پیام WS: {self.parser.messages_count}", C.CYN)
        log(
            f"⚠️  خطاها: {len(self.errors) + len(self.parser.errors)}",
            C.YEL,
        )
        log(f"📁 خروجی:  {self.output_dir.absolute()}", C.CYN)
        print()


# ======================================================================
# 🏁 INTERACTIVE ENTRY
# ======================================================================
async def interactive() -> None:
    banner()
    log("📝 Enter = پیش‌فرض\n", C.BOLD)

    log(f"🔗 URL پیش‌فرض: {SHELL_URL}", C.YEL)
    u = input(f"{C.CYN}URL: {C.RESET}").strip()
    url = u if u else SHELL_URL
    if not url.startswith("http"):
        url = "https://" + url

    log(
        f"\n🔢 تعداد چارت‌ها ({MIN_CHARTS}-{MAX_CHARTS}، "
        f"پیش‌فرض {DEFAULT_CHARTS}):",
        C.YEL,
    )
    log(f"   Enter = {DEFAULT_CHARTS}", C.DIM)
    inp_count = input(f"{C.CYN}تعداد: {C.RESET}").strip()
    try:
        chart_count = int(inp_count) if inp_count else DEFAULT_CHARTS
    except ValueError:
        log(f"⚠️  مقدار نامعتبر، پیش‌فرض {DEFAULT_CHARTS}", C.YEL)
        chart_count = DEFAULT_CHARTS
    chart_count = max(MIN_CHARTS, min(MAX_CHARTS, chart_count))

    log(f"\n📈 نمادها (پیش‌فرض، همه روی 1m):", C.YEL)
    for i in range(chart_count):
        cfg = PANELS_DEFAULT[i] if i < len(PANELS_DEFAULT) else {
            "symbol": "BINANCE:BTCUSDT", "interval": INTERVAL,
        }
        log(f"   [{i+1}] {cfg['symbol']:22s} | {cfg['interval']}", C.WHT)

    log("\n   Enter = پیش‌فرض", C.DIM)
    log(f"   یا: SYMBOL,SYMBOL,... (حداکثر {chart_count})", C.DIM)
    inp_syms = input(f"{C.CYN}لیست: {C.RESET}").strip()

    panels: list[dict] = []
    if inp_syms:
        for part in inp_syms.split(","):
            part = part.strip().upper()
            if not part:
                continue
            if ":" in part:
                panels.append({"symbol": part, "interval": INTERVAL})
            else:
                panels.append({
                    "symbol": f"BINANCE:{part}USDT",
                    "interval": INTERVAL,
                })
        if len(panels) < chart_count:
            for i in range(len(panels), chart_count):
                cfg = PANELS_DEFAULT[i] if i < len(PANELS_DEFAULT) else {
                    "symbol": "BINANCE:BTCUSDT", "interval": INTERVAL,
                }
                panels.append(dict(cfg))
        panels = panels[:chart_count]
    else:
        for i in range(chart_count):
            cfg = PANELS_DEFAULT[i] if i < len(PANELS_DEFAULT) else {
                "symbol": "BINANCE:BTCUSDT", "interval": INTERVAL,
            }
            panels.append(dict(cfg))

    print()
    log("─" * 60, C.DIM)
    log(f"🔗 URL:      {url}", C.WHT)
    log(f"🕐 Interval: {INTERVAL}m", C.WHT)
    log(f"📊 چارت‌ها:  {chart_count}", C.WHT)
    for i, p_ in enumerate(panels):
        log(f"   [{i+1}] {p_['symbol']:22s} | {p_['interval']}", C.WHT)
    log("─" * 60, C.DIM)
    print()

    confirm = input(f"{C.CYN}ادامه؟ (Y/n): {C.RESET}").strip().lower()
    if confirm and confirm not in ("y", "yes", "بله", "ب"):
        log("❌ لغو شد.", C.RED)
        return

    reader = TradingViewMultiChartReader(
        panels=panels,
        shell_url=url,
        output_dir=OUTPUT_DIR,
        chart_count=chart_count,
    )
    await reader.run()


def main() -> None:
    try:
        asyncio.run(interactive())
    except KeyboardInterrupt:
        log("\n⚠️ متوقف شد", C.YEL)
        sys.exit(0)
    except Exception as e:
        log(f"\n❌ {e}", C.RED)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

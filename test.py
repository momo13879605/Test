"""
TradingView Multi-Chart Reader - Professional Edition v13.0
============================================================
- استخراج کامل داده از چارت‌های TradingView از طریق WebSocket
- پشتیبانی از چندین چارت همزمان با مسیریابی sessionId
- نرمال‌سازی MACD + deduplication Volume + tracking pagination
- تمام چارت‌ها روی تایم‌فریم 1 دقیقه

اجرا:
    python3 chart_reader_v13.py
"""

import asyncio
import json
import os
import re
import sys
import traceback
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse, unquote, parse_qs

from playwright.async_api import async_playwright, Page, BrowserContext, Frame


# ======================================================================
# ⚙️  تنظیمات اصلی
# ======================================================================
SHELL_URL = "https://source-donii.ir/tread/index.html"
OUTPUT_DIR = "chart_output_v13"

# 🕐 تمام چارت‌ها روی تایم‌فریم 1 دقیقه
INTERVAL = "1"

PANELS = [
    {"symbol": "BINANCE:BTCUSDT", "interval": INTERVAL},
    {"symbol": "BINANCE:ETHUSDT", "interval": INTERVAL},
    {"symbol": "BINANCE:SOLUSDT", "interval": INTERVAL},
    {"symbol": "TVC:GOLD",        "interval": INTERVAL},
]

PRESET = "momentum"     # clean | basic | momentum | trend | volatility | pro | scalping | swing
THEME = "dark"
SYNC = False

# زمان‌بندی
WAIT_AFTER_LOAD = 6
WAIT_FOR_IFRAMES = 30
WAIT_FOR_DATA = 90              # برای 1m نیاز به زمان بیشتر
MIN_CANDLES_PER_SYMBOL = 100    # برای 1m حداقل 100 کندل
IDLE_THRESHOLD = 6              # ثانیه
MAX_CANDLES_KEEP = 1500         # ~25 ساعت در 1m
MAX_INDICATOR_POINTS = 1500

HEADLESS = True


# ======================================================================
# 🎨 رنگ‌ها و لاگ
# ======================================================================
class C:
    RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
    RED = "\033[91m"; GREEN = "\033[92m"; YELLOW = "\033[93m"
    BLUE = "\033[94m"; MAGENTA = "\033[95m"; CYAN = "\033[96m"; WHITE = "\033[97m"
    R = RESET; B = BOLD; D = DIM
    GRN = GREEN; YEL = YELLOW; CYN = CYAN; WHT = WHITE; MAG = MAGENTA


def log(msg: str = "", color: str = C.WHITE, prefix: str = "") -> None:
    p = f"{C.DIM}[{prefix}]{C.RESET} " if prefix else ""
    print(f"{p}{color}{msg}{C.RESET}")


def section(title: str) -> None:
    print()
    print(f"{C.DIM}{'━' * 76}{C.RESET}")
    print(f"  {C.BOLD}{C.CYN}{title}{C.RESET}")
    print(f"{C.DIM}{'━' * 76}{C.RESET}")


def banner() -> None:
    print()
    print(f"{C.CYN}╔{'═' * 76}╗{C.RESET}")
    print(f"{C.CYN}║{C.BOLD}{C.WHT}   📊  TradingView Multi-Chart Reader  v13.0  {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}║{C.DIM}   🕐  Timeframe: 1-minute  |  Session-aware routing  {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}╚{'═' * 76}╝{C.RESET}")
    print()


# ======================================================================
# 🧩 پارسر WebSocket
# ======================================================================
class TVWebSocketParser:
    """
    پارسر Socket.IO TradingView.
    مسیریابی از sessionId → symbol با چند لایه fallback.
    """

    # فیلدهای خاص symbol_info که نگه می‌داریم
    SYMBOL_INFO_KEYS = {
        "symbol", "pro_name", "full_name", "description", "short_description",
        "exchange", "listed_exchange", "type", "typespecs", "session",
        "session_display", "timezone", "currency_code", "base_currency",
        "pricescale", "minmov", "minmove2", "pointvalue", "fractional",
        "has_intraday", "has_daily", "has_weekly_and_monthly",
        "is_tradable", "industry", "sector",
    }

    # فیلدهای مهم quote
    QUOTE_KEYS = {
        "lp", "lp_time", "ch", "chp", "volume", "high_price", "low_price",
        "open_price", "prev_close_price", "bid", "ask", "bid_size", "ask_size",
        "currency_code", "exchange", "description", "update_mode",
        "current_session", "market_status", "session_display",
    }

    def __init__(self):
        self.sessions: dict[str, dict] = {}
        self.candles: dict[str, dict] = {}
        self.indicators: dict[str, dict] = {}
        self.study_defs: dict[str, dict] = {}
        self.symbol_info: dict[str, dict] = {}
        self.quote: dict[str, dict] = {}
        self.last_update: dict[str, float] = {}
        self.types_count: dict[str, int] = defaultdict(int)
        self.messages_count = 0
        self.errors: list[dict] = []
        self.tickmark_requests: dict[str, int] = defaultdict(int)

    # ------------------------------------------------------------------
    def parse_raw(self, raw: str) -> None:
        for msg in self._split_frames(raw):
            try:
                self._handle(msg)
            except Exception as e:
                self.errors.append({
                    "stage": "parse_message",
                    "error": str(e),
                    "type": msg.get("m") if isinstance(msg, dict) else "unknown",
                })

    def _split_frames(self, raw: str) -> list[dict]:
        """پارس فریم‌های Socket.IO"""
        out: list[dict] = []
        if not isinstance(raw, str):
            return out
        i, n = 0, len(raw)
        while i < n:
            # heartbeat: ~h~<num>~m~...
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
    def _handle(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return
        self.messages_count += 1
        m = msg.get("m", "?")
        self.types_count[m] += 1
        p = msg.get("p", [])

        handler = {
            "chart_create_session": self._on_create_session,
            "create_series":        self._on_create_series,
            "resolve_symbol":       self._on_resolve_symbol,
            "symbol_resolved":      self._on_symbol_resolved,
            "du":                   self._on_data,
            "timescale_update":     self._on_data,
            "qsd":                  self._on_quote,
            "create_study":         self._on_create_study,
            "request_more_tickmarks": self._on_more_tickmarks,
        }.get(m)

        if handler:
            handler(p)

    # ------------------------------------------------------------------
    @staticmethod
    def _extract_symbol(raw: Any) -> Optional[str]:
        """استخراج symbol از فرمت‌های مختلف"""
        if isinstance(raw, dict):
            return raw.get("symbol") or raw.get("pro_name") or raw.get("full_name")
        if isinstance(raw, str):
            if raw.startswith("="):
                try:
                    return json.loads(raw[1:]).get("symbol")
                except Exception:
                    return None
            if ":" in raw:
                return raw
        return None

    # ------------------------------------------------------------------
    def _on_create_session(self, p: list) -> None:
        if p and isinstance(p[0], str):
            self.sessions[p[0]] = {"symbol": None, "interval": None}

    def _on_create_series(self, p: list) -> None:
        """create_series: ["cs_X", "sds_1", "s1", "1", 1000, ""]"""
        if len(p) < 4:
            return
        sid = p[0]
        if sid in self.sessions and isinstance(p[3], str):
            self.sessions[sid]["interval"] = p[3]

    def _on_resolve_symbol(self, p: list) -> None:
        if len(p) < 3:
            return
        sid = p[0]
        symbol = self._extract_symbol(p[2])
        if sid in self.sessions and symbol:
            self.sessions[sid]["symbol"] = symbol
            self.candles.setdefault(symbol, {})
            self.indicators.setdefault(symbol, {})
            self.study_defs.setdefault(symbol, {})

    def _on_symbol_resolved(self, p: list) -> None:
        if len(p) < 3 or not isinstance(p[2], dict):
            return
        sid = p[0]
        info_raw = p[2]

        # نگه‌داشتن فقط فیلدهای مفید
        info = {k: info_raw[k] for k in self.SYMBOL_INFO_KEYS if k in info_raw}

        # اگر symbol اصلی از resolve_symbol نیامده، از این استفاده کن
        symbol = self.sessions.get(sid, {}).get("symbol")
        if not symbol:
            symbol = info.get("pro_name") or info.get("full_name") or info.get("name")
            if symbol and sid in self.sessions:
                self.sessions[sid]["symbol"] = symbol
                self.candles.setdefault(symbol, {})
                self.indicators.setdefault(symbol, {})
                self.study_defs.setdefault(symbol, {})

        if symbol:
            self.symbol_info[symbol] = info

    # ------------------------------------------------------------------
    def _on_data(self, p: list) -> None:
        """پارس پیام‌های du و timescale_update"""
        if len(p) < 2 or not isinstance(p[1], dict):
            return
        sid = p[0]
        symbol = self.sessions.get(sid, {}).get("symbol")
        if not symbol:
            return
        self.last_update[symbol] = datetime.now(timezone.utc).timestamp()

        candles = self.candles.setdefault(symbol, {})
        indicators = self.indicators.setdefault(symbol, {})

        for series_id, data in p[1].items():
            if not isinstance(data, dict):
                continue
            bars = data.get("s") or data.get("st") or data.get("ns")
            if not isinstance(bars, list):
                continue

            is_main_series = (series_id == "sds_1")
            target = candles if is_main_series else indicators.setdefault(series_id, {})

            for bar in bars:
                if not isinstance(bar, dict):
                    continue
                v = bar.get("v")
                if not isinstance(v, list) or len(v) < 2:
                    continue
                try:
                    t = int(float(v[0]))
                except (ValueError, TypeError):
                    continue

                if is_main_series and len(v) >= 6:
                    candles[t] = {
                        "time": t,
                        "open":   self._num(v[1]),
                        "high":   self._num(v[2]),
                        "low":    self._num(v[3]),
                        "close":  self._num(v[4]),
                        "volume": self._num(v[5]),
                    }
                else:
                    indicators[series_id][t] = [self._num(x) for x in v[1:]]

    # ------------------------------------------------------------------
    def _on_quote(self, p: list) -> None:
        """پارس پیام qsd"""
        if len(p) < 2 or not isinstance(p[1], dict):
            return
        q = p[1]
        symbol = self._extract_symbol(q.get("n"))
        if not symbol:
            return

        v = q.get("v", {})
        if isinstance(v, dict):
            filtered = {k: v[k] for k in self.QUOTE_KEYS if k in v}
            if filtered:
                self.quote.setdefault(symbol, {}).update(filtered)

    # ------------------------------------------------------------------
    def _on_create_study(self, p: list) -> None:
        """create_study: ["cs_X", "study_id", {...}, "st1", "Volume@tv-basicstudies", {...}]"""
        if len(p) < 5:
            return
        sid = p[0]
        study_id = p[1]
        stype_raw = str(p[4])
        stype = stype_raw.split("@")[0] if "@" in stype_raw else stype_raw
        inputs = p[5] if len(p) > 5 and isinstance(p[5], dict) else {}
        symbol = self.sessions.get(sid, {}).get("symbol")
        if symbol:
            self.study_defs.setdefault(symbol, {})[study_id] = {
                "type": stype,
                "inputs": inputs,
                "full_type": stype_raw,
            }

    def _on_more_tickmarks(self, p: list) -> None:
        if p and isinstance(p[0], str):
            self.tickmark_requests[p[0]] += 1

    # ------------------------------------------------------------------
    @staticmethod
    def _num(x: Any) -> Any:
        try:
            return x if isinstance(x, (int, float)) else float(x)
        except (ValueError, TypeError):
            return x

    # ------------------------------------------------------------------
    # 🎯 نرمال‌سازی MACD
    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_macd(values: list) -> list:
        """
        ترتیب مقادیر MACD در TradingView WebSocket:
            [histogram, macd_line, signal_line]
        ترتیب استاندارد خروجی:
            [macd_line, signal_line, histogram]
        """
        if len(values) != 3:
            return values
        try:
            hist, macd, signal = values[0], values[1], values[2]
            # اعتبارسنجی: macd - signal = hist
            if isinstance(hist, (int, float)) and isinstance(macd, (int, float)) \
               and isinstance(signal, (int, float)):
                if abs((macd - signal) - hist) > 1e-6 * max(abs(macd), 1):
                    # اگر اعتبارسنجی رد شد، همان ترتیب اصلی را نگه‌دار
                    return values
            return [macd, signal, hist]
        except Exception:
            return values

    # ------------------------------------------------------------------
    # 🎯 deduplication اندیکاتورهای تکراری
    # ------------------------------------------------------------------
    @staticmethod
    def _dedupe_indicators(ind_out: dict) -> dict:
        """
        اگر چند اندیکاتور با type یکسان وجود داشته باشد (مثل Volume تکراری)،
        آن‌ها را با suffix عددی rename می‌کنیم.
        """
        by_type: dict[str, list[str]] = defaultdict(list)
        for sid, info in ind_out.items():
            by_type[info["type"]].append(sid)

        result: dict = {}
        for stype, sids in by_type.items():
            if len(sids) == 1:
                result[sids[0]] = ind_out[sids[0]]
            else:
                # مرتب‌سازی بر اساس تعداد پوینت‌ها (بیشترین اول) - نگه‌داشتن بدون suffix
                sids_sorted = sorted(
                    sids,
                    key=lambda s: ind_out[s]["points_count"],
                    reverse=True,
                )
                result[sids_sorted[0]] = ind_out[sids_sorted[0]]
                for idx, sid in enumerate(sids_sorted[1:], start=2):
                    info = dict(ind_out[sid])
                    info["type"] = f"{stype}_{idx}"
                    info["_dedup_of"] = sids_sorted[0]
                    result[sid] = info
        return result

    # ------------------------------------------------------------------
    def get_symbol_data(self, symbol: str,
                        max_points: int = MAX_INDICATOR_POINTS,
                        max_candles: int = MAX_CANDLES_KEEP) -> dict:
        """داده‌ی کامل و نرمال‌شده برای یک symbol"""
        raw_candles = self.candles.get(symbol, {})
        raw_inds = self.indicators.get(symbol, {})
        defs = self.study_defs.get(symbol, {})

        # --- کندل‌ها (مرتب‌شده + limit) ---
        c_times = sorted(raw_candles.keys())
        if len(c_times) > max_candles:
            c_times = c_times[-max_candles:]
        candles = [raw_candles[t] for t in c_times]

        # --- اندیکاتورها ---
        ind_out: dict = {}
        for sid, tv_map in raw_inds.items():
            meta = defs.get(sid, {})
            stype = meta.get("type", "unknown")

            keys = sorted(tv_map.keys())
            pts = [{"time": int(t), "values": tv_map[t]} for t in keys]
            if len(pts) > max_points:
                pts = pts[-max_points:]

            # نرمال‌سازی MACD
            if stype == "MACD":
                for pt in pts:
                    pt["values"] = self._normalize_macd(pt["values"])

            ind_out[sid] = {
                "type": stype,
                "full_type": meta.get("full_type", ""),
                "inputs": meta.get("inputs", {}),
                "points_count": len(pts),
                "points": pts,
            }

        # deduplication
        ind_out = self._dedupe_indicators(ind_out)

        return {
            "candles": candles,
            "indicators": ind_out,
            "symbol_info": self.symbol_info.get(symbol, {}),
            "quote": self.quote.get(symbol, {}),
            "last_update_ts": self.last_update.get(symbol, 0),
            "interval": self._get_interval(symbol),
        }

    def _get_interval(self, symbol: str) -> Optional[str]:
        for sid, s in self.sessions.items():
            if s.get("symbol") == symbol:
                return s.get("interval")
        return None

    def known_symbols(self) -> list[str]:
        return sorted(
            self.sessions[s]["symbol"]
            for s in self.sessions
            if self.sessions[s].get("symbol")
        )


# ======================================================================
# 📊 مدل خروجی
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
    candles: list = field(default_factory=list)
    indicators: dict = field(default_factory=dict)
    symbol_info: dict = field(default_factory=dict)
    quote: dict = field(default_factory=dict)
    statistics: dict = field(default_factory=dict)
    canvas_info: list = field(default_factory=list)


# ======================================================================
# 📊 محاسبه‌ی آمار حرفه‌ای
# ======================================================================
class StatsCalculator:

    @staticmethod
    def compute(candles: list[dict], indicators: dict,
                quote: dict, interval: str = "?") -> dict:
        if not candles:
            return {}

        last = candles[-1]
        first = candles[0]
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [c["volume"] for c in candles
                   if c.get("volume") is not None]

        stats: dict[str, Any] = {
            "candles": len(candles),
            "interval": interval,
            "time_from": first["time"],
            "time_to": last["time"],
            "time_from_iso": datetime.fromtimestamp(
                first["time"], tz=timezone.utc).isoformat(),
            "time_to_iso": datetime.fromtimestamp(
                last["time"], tz=timezone.utc).isoformat(),
            "duration_hours": round((last["time"] - first["time"]) / 3600, 2),

            "last": {
                "open": last["open"], "high": last["high"],
                "low": last["low"], "close": last["close"],
                "volume": last.get("volume"),
            },
            "high_max": max(highs),
            "low_min": min(lows),
            "close_max": max(closes),
            "close_min": min(closes),
            "average_close": sum(closes) / len(closes),
            "range": max(highs) - min(lows),
            "range_percent": (max(highs) - min(lows)) / first["close"] * 100
                if first["close"] else 0,
        }

        # نوسان‌پذیری (volatility)
        if len(closes) > 1:
            returns = [(closes[i] - closes[i - 1]) / closes[i - 1]
                       for i in range(1, len(closes)) if closes[i - 1]]
            if returns:
                mean_r = sum(returns) / len(returns)
                var = sum((r - mean_r) ** 2 for r in returns) / len(returns)
                stats["volatility_std"] = round((var ** 0.5) * 100, 6)

        if volumes:
            stats["volume_total"] = sum(volumes)
            stats["volume_avg"] = sum(volumes) / len(volumes)
            stats["volume_max"] = max(volumes)

        if first["close"]:
            change = last["close"] - first["close"]
            stats["change_absolute"] = round(change, 10)
            stats["change_percent"] = round(change / first["close"] * 100, 4)

        # آخرین مقادیر اندیکاتورها
        latest: dict = {}
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

        # quote
        if quote:
            for key in ("lp", "ch", "chp", "volume", "high_price", "low_price",
                        "open_price", "bid", "ask", "current_session"):
                if key in quote:
                    stats[f"quote_{key}"] = quote[key]

        return stats


# ======================================================================
# 🎯 خواننده‌ی اصلی
# ======================================================================
class TradingViewMultiChartReader:

    def __init__(self, panels: list[dict], shell_url: str, output_dir: str):
        self.panels = panels
        self.shell_url = shell_url
        self.output_dir = Path(output_dir)
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.parser = TVWebSocketParser()
        self.errors: list[dict] = []
        self.warnings: list[str] = []
        self.screenshots: list[str] = []

    # ------------------------------------------------------------------
    def _err(self, stage: str, err: Exception) -> None:
        self.errors.append({
            "stage": stage,
            "error": str(err),
            "type": type(err).__name__,
            "traceback": traceback.format_exc()[:1500],
        })

    # ------------------------------------------------------------------
    def _init_script(self) -> str:
        panels_json = json.dumps(self.panels)
        return f"""
        (() => {{
            try {{
                localStorage.setItem('tvp_layout', String({len(self.panels)}));
                localStorage.setItem('tvp_panels', JSON.stringify({panels_json}));
                localStorage.setItem('tvp_preset', '{PRESET}');
                localStorage.setItem('tvp_sync', '{str(SYNC).lower()}');
                localStorage.setItem('tvp_theme', '{THEME}');
                localStorage.setItem('tvp_interval', '{INTERVAL}');
            }} catch(e) {{ console.error('[EXT] init err', e); }}
        }})();
        """

    # ------------------------------------------------------------------
    async def _setup_ws(self, page: Page) -> None:
        def on_ws(ws):
            ws.on("framereceived",
                  lambda p: asyncio.create_task(self._on_ws(ws.url, p)))
            ws.on("framesent",
                  lambda p: asyncio.create_task(self._on_ws(ws.url, p)))
        page.on("websocket", on_ws)

    async def _on_ws(self, url: str, payload) -> None:
        try:
            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="ignore")
            else:
                text = str(payload)
            if len(text) > 2_000_000:
                return
            if "tradingview" in url:
                self.parser.parse_raw(text)
        except Exception as e:
            self._err("ws_parse", e)

    # ------------------------------------------------------------------
    async def _click_layout_button(self, page: Page, layout: str) -> bool:
        try:
            selector = f'.layout-btn[data-layout="{layout}"]'
            await page.wait_for_selector(selector, timeout=5000)
            await page.click(selector)
            await page.wait_for_timeout(3000)
            log(f"✅ کلیک روی layout={layout}", C.GRN, 2)
            return True
        except Exception as e:
            log(f"⚠️  fallback کلیک ناموفق: {e}", C.YEL, 2)
            return False

    # ------------------------------------------------------------------
    def _parse_iframe_url(self, src: str) -> dict:
        result: dict = {"raw_url": src[:300]}
        try:
            parsed = urlparse(src)
            result["host"] = parsed.netloc

            # fragment (hash) format
            if parsed.fragment:
                try:
                    hp = json.loads(unquote(parsed.fragment))
                    for k in ("symbol", "interval", "studies", "theme"):
                        if k in hp:
                            result[k if k != "studies" else "studies_raw"] = hp[k]
                except Exception:
                    pass

            # query string format
            if parsed.query:
                try:
                    qs = parse_qs(parsed.query)
                    for k in ("symbol", "interval"):
                        if k in qs:
                            result.setdefault(k, qs[k][0])
                except Exception:
                    pass
        except Exception:
            pass
        return result

    async def _find_iframes(self, page: Page) -> dict[str, dict]:
        result: dict[str, dict] = {}
        for tick in range(WAIT_FOR_IFRAMES):
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
                    log(f"✅ iframe: {sym} (interval={info.get('interval')})",
                        C.GRN, 2)
            if len(result) >= len(self.panels):
                break
        return result

    # ------------------------------------------------------------------
    async def _screenshot(self, page: Page, suffix: str = "page") -> str:
        try:
            path = self.output_dir / f"{suffix}_{self.ts}.png"
            await page.screenshot(path=str(path), full_page=False)
            self.screenshots.append(str(path))
            return str(path)
        except Exception as e:
            self._err("screenshot", e)
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
    async def run(self) -> list[ChartExtraction]:
        banner()
        log(f"🎯 URL:      {self.shell_url}", C.YEL)
        log(f"📁 خروجی:   {self.output_dir}", C.YEL)
        log(f"📊 چارت‌ها:  {len(self.panels)}", C.YEL)
        log(f"🕐 تایم‌فریم: {INTERVAL} دقیقه", C.MAG)
        log(f"🎨 Preset:  {PRESET} | Theme: {THEME} | Sync: {SYNC}", C.YEL)
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
                locale="en-US", timezone_id="Asia/Tehran",
                ignore_https_errors=True,
            )
            await ctx.add_init_script(self._init_script())
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
            """)

            page = await ctx.new_page()
            await self._setup_ws(page)

            section("۲. باز کردن صفحه اصلی")
            try:
                r = await page.goto(self.shell_url,
                                    wait_until="domcontentloaded",
                                    timeout=90000)
                log(f"✅ HTTP: {r.status if r else 'N/A'}", C.GRN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YEL, 2)
                self._err("goto", e)

            await page.wait_for_timeout(WAIT_AFTER_LOAD * 1000)

            section("۳. بررسی پنل‌ها")
            panel_count = await self._count_panels(page)
            log(f"📊 پنل‌ها: {panel_count} (هدف: {len(self.panels)})", C.CYN, 2)

            if panel_count < len(self.panels):
                layout_str = str(len(self.panels))
                if layout_str not in ("1", "2", "4", "6"):
                    if len(self.panels) <= 2: layout_str = "2"
                    elif len(self.panels) <= 4: layout_str = "4"
                    else: layout_str = "6"
                await self._click_layout_button(page, layout_str)
                await page.wait_for_timeout(5000)

            section("۴. پیدا کردن iframeها")
            iframes_map = await self._find_iframes(page)
            log(f"✅ {len(iframes_map)} iframe پیدا شد", C.GRN, 2)

            section(f"۵. جمع‌آوری داده WS (حداکثر {WAIT_FOR_DATA}s)")
            target_symbols = [p["symbol"] for p in self.panels]
            start = datetime.now(timezone.utc).timestamp()

            while True:
                await page.wait_for_timeout(2000)
                elapsed = datetime.now(timezone.utc).timestamp() - start

                parts = []
                for sym in target_symbols:
                    n = len(self.parser.candles.get(sym, {}))
                    parts.append(f"{sym.split(':')[-1]}:{n}")
                print(f"\r  ⏳ {int(elapsed):3d}s | " + " | ".join(parts) + "   ",
                      end="", flush=True)

                if elapsed >= WAIT_FOR_DATA:
                    break

                all_ready = all(
                    len(self.parser.candles.get(sym, {})) >= MIN_CANDLES_PER_SYMBOL
                    for sym in target_symbols
                )
                if all_ready and elapsed > 12:
                    now = datetime.now(timezone.utc).timestamp()
                    all_idle = all(
                        now - self.parser.last_update.get(sym, 0) > IDLE_THRESHOLD
                        for sym in target_symbols
                    )
                    if all_idle:
                        break
            print()
            log(f"✅ جمع‌آوری تمام شد", C.GRN, 2)

            section("۶. استخراج داده‌ها")
            results: list[ChartExtraction] = []

            for panel in self.panels:
                sym = panel["symbol"]
                ws_data = self.parser.get_symbol_data(sym)
                iframe_info = iframes_map.get(sym, {})

                canvas_info = []
                if iframe_info.get("frame"):
                    try:
                        canvas_info = await self._canvas_info(iframe_info["frame"])
                    except Exception as e:
                        self._err(f"canvas[{sym}]", e)

                actual_ws_symbol = ws_data["symbol_info"].get("pro_name", "")
                ws_interval = ws_data.get("interval") or ""

                # match strict
                match = False
                match_reason = "no_match"
                if actual_ws_symbol:
                    if actual_ws_symbol.upper() == sym.upper():
                        match = True
                        match_reason = "exact_pro_name"
                    elif sym.split(":")[-1].upper() in actual_ws_symbol.upper():
                        match = True
                        match_reason = "base_match"

                stats = StatsCalculator.compute(
                    ws_data["candles"],
                    ws_data["indicators"],
                    ws_data["quote"],
                    interval=ws_interval or INTERVAL,
                )

                results.append(ChartExtraction(
                    config=panel,
                    iframe_url=iframe_info.get("url", "")[:300],
                    iframe_interval=iframe_info.get("interval", ""),
                    iframe_studies_raw=iframe_info.get("studies_raw", ""),
                    actual_ws_symbol=actual_ws_symbol,
                    ws_interval=ws_interval,
                    match=match,
                    match_reason=match_reason,
                    candles_count=len(ws_data["candles"]),
                    indicators_count=len(ws_data["indicators"]),
                    candles=ws_data["candles"],
                    indicators=ws_data["indicators"],
                    symbol_info=ws_data["symbol_info"],
                    quote=ws_data["quote"],
                    statistics=stats,
                    canvas_info=canvas_info,
                ))

                status = "✅" if match else "❌"
                log(f"{status} {sym:22s} | "
                    f"کندل: {len(ws_data['candles']):5d} | "
                    f"اندیکاتور: {len(ws_data['indicators'])} | "
                    f"Interval: {ws_interval or '?':>3s} | "
                    f"WS: {actual_ws_symbol or 'N/A'}",
                    C.GRN if match else C.YEL, 2)

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
                "version": "13.0",
                "interval_requested": INTERVAL,
                "panels_requested": len(self.panels),
                "panels_extracted": len(results),
                "preset": PRESET,
                "theme": THEME,
                "screenshots": self.screenshots,
            },
            "websocket_summary": {
                "messages_count": self.parser.messages_count,
                "types_count": dict(self.parser.types_count),
                "sessions": {
                    sid: self.parser.sessions[sid]
                    for sid in self.parser.sessions
                },
                "known_symbols": self.parser.known_symbols(),
                "tickmark_requests": dict(self.parser.tickmark_requests),
                "parser_errors": self.parser.errors[:30],
            },
            "errors": self.errors,
            "warnings": self.warnings,
            "charts": [asdict(r) for r in results],
        }

        combined_path = self.output_dir / f"all_charts_{ts}.json"
        with open(combined_path, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2, default=str)
        log(f"✅ جامع: {combined_path.name}", C.GRN, 2)

        # جداگانه
        manifest: list[dict] = []
        for i, r in enumerate(results):
            sym_key = r.config["symbol"].replace(":", "_")
            base = self.output_dir / f"chart_{i:02d}_{sym_key}_{ts}"

            entry = {
                "index": i,
                "symbol": r.config["symbol"],
                "interval": r.ws_interval or r.config["interval"],
                "match": r.match,
                "files": [],
            }

            if not r.candles and not r.indicators:
                log(f"⚠️  {r.config['symbol']}: خالی، skip", C.YEL, 2)
                continue

            full_path = base.with_name(base.name + "_full.json")
            with open(full_path, "w", encoding="utf-8") as f:
                json.dump(asdict(r), f, ensure_ascii=False, indent=2, default=str)
            entry["files"].append(full_path.name)

            if r.candles:
                cp = base.with_name(base.name + "_candles.json")
                with open(cp, "w", encoding="utf-8") as f:
                    json.dump(r.candles, f, ensure_ascii=False, indent=2)
                entry["files"].append(cp.name)

            if r.indicators:
                ip = base.with_name(base.name + "_indicators.json")
                with open(ip, "w", encoding="utf-8") as f:
                    json.dump(r.indicators, f, ensure_ascii=False,
                              indent=2, default=str)
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
            log(f"✅ {r.config['symbol']:22s} "
                f"(candles={r.candles_count}, ind={r.indicators_count})",
                C.GRN, 2)

        # manifest
        manifest_path = self.output_dir / f"manifest_{ts}.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump({
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "version": "13.0",
                "interval": INTERVAL,
                "charts": manifest,
            }, f, ensure_ascii=False, indent=2)
        log(f"✅ Manifest: {manifest_path.name}", C.GRN, 2)

        self._save_summary(results)

    def _save_summary(self, results: list[ChartExtraction]) -> None:
        path = self.output_dir / f"summary_{self.ts}.txt"
        with open(path, "w", encoding="utf-8") as f:
            W = 78
            f.write("=" * W + "\n")
            f.write("📊 TradingView Multi-Chart Reader v13.0 - Summary\n")
            f.write("=" * W + "\n\n")
            f.write(f"Shell URL:    {self.shell_url}\n")
            f.write(f"Timestamp:    {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Interval:     {INTERVAL} minute\n")
            f.write(f"Panels:       {len(results)}/{len(self.panels)}\n")
            f.write(f"Preset:       {PRESET}\n")
            f.write(f"Theme:        {THEME}\n\n")

            f.write("─" * W + "\n[WebSocket Stats]\n" + "─" * W + "\n")
            f.write(f"Total messages:  {self.parser.messages_count}\n")
            f.write(f"Message types:\n")
            for k, v in sorted(self.parser.types_count.items(),
                               key=lambda x: -x[1]):
                f.write(f"   {k:30s} : {v}\n")
            f.write("\n")

            for i, r in enumerate(results):
                f.write("─" * W + "\n")
                f.write(f"[CHART {i+1}] {r.config['symbol']} "
                        f"(requested_interval={r.config['interval']})\n")
                f.write("─" * W + "\n")
                f.write(f"Match:             {'✅ YES' if r.match else '❌ NO'} "
                        f"({r.match_reason})\n")
                f.write(f"WS symbol:         {r.actual_ws_symbol}\n")
                f.write(f"WS interval:       {r.ws_interval}\n")
                f.write(f"Candles:           {r.candles_count}\n")
                f.write(f"Indicators:        {r.indicators_count}\n")
                f.write(f"Symbol name:       "
                        f"{r.symbol_info.get('description', 'N/A')}\n")
                f.write(f"Exchange:          "
                        f"{r.symbol_info.get('exchange', 'N/A')}\n")
                f.write(f"Timezone:          "
                        f"{r.symbol_info.get('timezone', 'N/A')}\n")
                f.write(f"Pricescale:        "
                        f"{r.symbol_info.get('pricescale', 'N/A')}\n\n")

                st = r.statistics
                if st:
                    f.write("  ── Statistics ──\n")
                    f.write(f"    last close:     {st.get('last', {}).get('close')}\n")
                    f.write(f"    high max:       {st.get('high_max')}\n")
                    f.write(f"    low min:        {st.get('low_min')}\n")
                    f.write(f"    range:          {st.get('range'):.4f} "
                            f"({st.get('range_percent'):.2f}%)\n")
                    f.write(f"    change:         {st.get('change_absolute')} "
                            f"({st.get('change_percent')}%)\n")
                    if "volatility_std" in st:
                        f.write(f"    volatility:     "
                                f"{st.get('volatility_std'):.4f}%\n")
                    if "volume_total" in st:
                        f.write(f"    volume total:   {st.get('volume_total')}\n")
                        f.write(f"    volume avg:     "
                                f"{st.get('volume_avg'):.4f}\n")
                    f.write(f"    duration:       "
                            f"{st.get('duration_hours')} hours\n\n")

                    li = st.get("latest_indicators", {})
                    if li:
                        f.write("  ── Latest Indicators ──\n")
                        for name, info in li.items():
                            f.write(f"    {name:12s}: {info.get('values')}\n")
                        f.write("\n")

                if r.quote:
                    f.write("  ── Quote Snapshot ──\n")
                    for k in ("lp", "bid", "ask", "ch", "chp", "volume",
                              "high_price", "low_price", "current_session"):
                        if k in r.quote:
                            f.write(f"    {k:18s}: {r.quote[k]}\n")
                    f.write("\n")

                if r.indicators:
                    f.write("  ── Indicators Detail ──\n")
                    for sid, ind in r.indicators.items():
                        if isinstance(ind, dict):
                            dedup_note = (f" [dedup of {ind['_dedup_of']}]"
                                          if ind.get("_dedup_of") else "")
                            f.write(f"    {sid:12s}: {ind.get('type'):20s} "
                                    f"({ind.get('points_count', 0):5d} pts)"
                                    f"{dedup_note}\n")
                f.write("\n")

            if self.errors:
                f.write("─" * W + "\n[ERRORS]\n" + "─" * W + "\n")
                for e in self.errors[:20]:
                    f.write(f"  - [{e.get('stage')}] {e.get('type')}: "
                            f"{e.get('error')}\n")

            if self.parser.errors:
                f.write("\n" + "─" * W + "\n[PARSER ERRORS]\n" + "─" * W + "\n")
                for e in self.parser.errors[:20]:
                    f.write(f"  - [{e.get('stage')}] {e.get('error')}\n")

        log(f"✅ خلاصه: {path.name}", C.GRN, 2)

    # ------------------------------------------------------------------
    def _print_summary(self, results: list[ChartExtraction]) -> None:
        print()
        print(f"{C.GRN}╔{'═' * 76}╗{C.RESET}")
        print(f"{C.GRN}║{C.BOLD}{C.WHT}          ✅  استخراج با موفقیت تمام شد          {C.RESET}{C.GRN}║{C.RESET}")
        print(f"{C.GRN}╚{'═' * 76}╝{C.RESET}")
        print()

        matched = sum(1 for r in results if r.match)
        for i, r in enumerate(results):
            status = "✅" if r.match else "❌"
            last_close = r.statistics.get("last", {}).get("close", "N/A")
            log(f"{status} [{i+1}] {r.config['symbol']:22s} | "
                f"Interval: {r.ws_interval or '?':>3s} | "
                f"کندل: {r.candles_count:5d} | "
                f"آخرین: {last_close}",
                C.WHT)

        print()
        log(f"📊 موفق: {matched}/{len(results)}", C.CYN)
        log(f"🔌 پیام WS: {self.parser.messages_count}", C.CYN)
        log(f"⚠️  خطاها: {len(self.errors) + len(self.parser.errors)}", C.YEL)
        log(f"📁 خروجی:  {self.output_dir.absolute()}", C.CYN)
        print()


# ======================================================================
# 🏁 ورودی تعاملی
# ======================================================================
async def interactive():
    banner()
    log("📝 Enter = پیش‌فرض\n", C.BOLD)

    log(f"🔗 URL پیش‌فرض: {SHELL_URL}", C.YEL)
    u = input(f"{C.CYN}URL: {C.RESET}").strip()
    url = u if u else SHELL_URL
    if not url.startswith("http"):
        url = "https://" + url

    log(f"\n📈 چارت‌ها ({len(PANELS)} مورد پیش‌فرض، همه روی 1m):", C.YEL)
    for i, c in enumerate(PANELS):
        log(f"   [{i+1}] {c['symbol']:22s} | {c['interval']}", C.WHT)
    log("\n   Enter = پیش‌فرض", C.DIM)
    log("   یا: SYMBOL,SYMBOL,... (فقط نمادها - interval خودکار 1m)", C.DIM)
    inp = input(f"{C.CYN}لیست: {C.RESET}").strip()

    panels = PANELS
    if inp:
        new_panels = []
        for part in inp.split(","):
            part = part.strip().upper()
            if not part:
                continue
            if ":" in part:
                new_panels.append({"symbol": part, "interval": INTERVAL})
            else:
                new_panels.append({
                    "symbol": f"BINANCE:{part}USDT",
                    "interval": INTERVAL,
                })
        if new_panels:
            panels = new_panels

    if len(panels) not in (1, 2, 4, 6):
        log(f"\n⚠️  سایت فقط 1، 2، 4 یا 6 چارت پشتیبانی می‌کند. "
            f"تعداد {len(panels)} تنظیم می‌شود.", C.YEL)
        if len(panels) <= 1: panels = panels[:1]
        elif len(panels) <= 2: panels = panels[:2]
        elif len(panels) <= 4: panels = panels[:4]
        else: panels = panels[:6]

    print()
    log("─" * 60, C.DIM)
    log(f"🔗 URL:      {url}", C.WHT)
    log(f"🕐 Interval: {INTERVAL}m", C.WHT)
    log(f"📊 چارت‌ها: {len(panels)}", C.WHT)
    for i, p in enumerate(panels):
        log(f"   [{i+1}] {p['symbol']:22s} | {p['interval']}", C.WHT)
    log("─" * 60, C.DIM)
    print()

    confirm = input(f"{C.CYN}ادامه؟ (Y/n): {C.RESET}").strip().lower()
    if confirm and confirm not in ("y", "yes", "بله", "ب"):
        log("❌ لغو شد.", C.RED)
        return

    reader = TradingViewMultiChartReader(panels, url, OUTPUT_DIR)
    await reader.run()


def main():
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

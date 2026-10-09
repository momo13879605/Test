"""
TradingView Multi-Chart Reader - Professional Edition v12.0
============================================================
- راه‌اندازی از طریق localStorage + fallback کلیک روی دکمه‌ها
- مسیریابی WebSocket با sessionId (چند چارت همزمان)
- استخراج کامل کندل‌ها، اندیکاتورها، symbol_info، quote
- گزارش آماری حرفه‌ای (آخرین قیمت، RSI، MACD، تغییرات)
- معماری تمیز و قابل توسعه

اجرا:
    python3 chart_reader_v12.py
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
from typing import Any, Optional
from urllib.parse import urlparse, unquote

from playwright.async_api import async_playwright, Page, BrowserContext, Frame


# ======================================================================
# ⚙️  تنظیمات
# ======================================================================
SHELL_URL = "https://source-donii.ir/tread/index.html"
OUTPUT_DIR = "chart_output_v12"

# چارت‌ها (۱، ۲، ۴ یا ۶ — سایت فقط این‌ها را پشتیبانی می‌کند)
PANELS = [
    {"symbol": "BINANCE:BTCUSDT", "interval": "60"},
    {"symbol": "BINANCE:ETHUSDT", "interval": "60"},
    {"symbol": "BINANCE:SOLUSDT", "interval": "60"},
    {"symbol": "TVC:GOLD",        "interval": "60"},
]

PRESET = "momentum"     # clean | basic | momentum | trend | volatility | pro | scalping | swing
THEME = "dark"
SYNC = False            # همگام‌سازی نماد بین چارت‌ها

# زمان‌بندی
WAIT_AFTER_LOAD = 6
WAIT_FOR_IFRAMES = 25
WAIT_FOR_DATA = 45
MIN_CANDLES_PER_SYMBOL = 50

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
    print(f"{C.CYN}║{C.BOLD}{C.WHT}   📊  TradingView Multi-Chart Reader  v12.0   {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}╚{'═' * 76}╝{C.RESET}")
    print()


# ======================================================================
# 🧩 پارسر WebSocket با مسیریابی sessionId
# ======================================================================
class TVWebSocketParser:
    """
    پارسر پیام‌های Socket.IO TradingView.
    هر chart_create_session یک sessionId دارد که به یک symbol متصل می‌شود.
    """

    def __init__(self):
        self.sessions: dict[str, dict] = {}                 # sessionId → {symbol}
        self.candles: dict[str, dict] = {}                  # symbol → {time: candle}
        self.indicators: dict[str, dict] = {}               # symbol → {studyId: {time: [v]}}
        self.study_defs: dict[str, dict] = {}               # symbol → {studyId: {type, inputs}}
        self.symbol_info: dict[str, dict] = {}              # symbol → info
        self.quote: dict[str, dict] = {}                    # symbol → quote
        self.last_update: dict[str, float] = {}             # symbol → timestamp
        self.types_count: dict[str, int] = defaultdict(int)
        self.messages_count = 0

    # ------------------------------------------------------------------
    def parse_raw(self, raw: str) -> None:
        for msg in self._split_frames(raw):
            try:
                self._handle(msg)
            except Exception:
                pass

    def _split_frames(self, raw: str) -> list[dict]:
        out = []
        i, n = 0, len(raw)
        while i < n:
            if raw.startswith("~h~", i):
                j = raw.find("~", i + 3)
                if j == -1: break
                i = j + 1
                continue
            if not raw.startswith("~m~", i):
                j = raw.find("~m~", i)
                if j == -1: break
                i = j
            j = raw.find("~m~", i + 3)
            if j == -1: break
            try:
                length = int(raw[i + 3:j])
            except ValueError:
                i = j + 3
                continue
            start = j + 3
            end = start + length
            if end > n: break
            try:
                out.append(json.loads(raw[start:end]))
            except json.JSONDecodeError:
                pass
            i = end
        return out

    # ------------------------------------------------------------------
    def _handle(self, msg: dict) -> None:
        if not isinstance(msg, dict): return
        self.messages_count += 1
        m = msg.get("m", "?")
        self.types_count[m] += 1
        p = msg.get("p", [])

        if m == "chart_create_session":
            self._on_create_session(p)
        elif m == "resolve_symbol":
            self._on_resolve_symbol(p)
        elif m == "symbol_resolved":
            self._on_symbol_resolved(p)
        elif m in ("du", "timescale_update"):
            self._on_data(p)
        elif m == "qsd":
            self._on_quote(p)
        elif m == "create_study":
            self._on_create_study(p)

    # ------------------------------------------------------------------
    @staticmethod
    def _parse_hash(raw: str) -> Optional[str]:
        """استخراج symbol از رشته‌ی '={"symbol":"..."}'"""
        if not isinstance(raw, str) or not raw.startswith("="):
            return None
        try:
            return json.loads(raw[1:]).get("symbol")
        except Exception:
            return None

    def _on_create_session(self, p: list) -> None:
        if p and isinstance(p[0], str):
            self.sessions[p[0]] = {"symbol": None}

    def _on_resolve_symbol(self, p: list) -> None:
        if len(p) < 3: return
        sid = p[0]
        symbol = self._parse_hash(p[2])
        if sid in self.sessions and symbol:
            self.sessions[sid]["symbol"] = symbol
            self.candles.setdefault(symbol, {})
            self.indicators.setdefault(symbol, {})
            self.study_defs.setdefault(symbol, {})

    def _on_symbol_resolved(self, p: list) -> None:
        if len(p) < 3 or not isinstance(p[2], dict): return
        sid = p[0]
        info = p[2]
        symbol = self.sessions.get(sid, {}).get("symbol") \
                 or info.get("pro_name") or info.get("full_name")
        if symbol:
            self.symbol_info[symbol] = info

    def _on_data(self, p: list) -> None:
        if len(p) < 2 or not isinstance(p[1], dict): return
        sid = p[0]
        symbol = self.sessions.get(sid, {}).get("symbol")
        if not symbol: return
        self.last_update[symbol] = datetime.now(timezone.utc).timestamp()

        candles = self.candles.setdefault(symbol, {})
        indicators = self.indicators.setdefault(symbol, {})

        for series_id, data in p[1].items():
            if not isinstance(data, dict): continue
            bars = data.get("s")
            if not isinstance(bars, list):
                bars = data.get("st")
            if not isinstance(bars, list): continue
            for bar in bars:
                if not isinstance(bar, dict): continue
                v = bar.get("v")
                if not isinstance(v, list) or len(v) < 2: continue
                try:
                    t = float(v[0])
                except (ValueError, TypeError):
                    continue

                if series_id == "sds_1" and len(v) >= 6:
                    candles[t] = {
                        "time": int(t),
                        "open": self._num(v[1]), "high": self._num(v[2]),
                        "low": self._num(v[3]), "close": self._num(v[4]),
                        "volume": self._num(v[5]),
                    }
                else:
                    indicators.setdefault(series_id, {})[t] = [
                        self._num(x) for x in v[1:]
                    ]

    def _on_quote(self, p: list) -> None:
        if len(p) < 2 or not isinstance(p[1], dict): return
        q = p[1]
        raw_n = q.get("n", "")
        symbol = self._parse_hash(raw_n) if isinstance(raw_n, str) else None
        if not symbol and isinstance(raw_n, str) and ":" in raw_n:
            symbol = raw_n
        v = q.get("v", {})
        if symbol and isinstance(v, dict):
            self.quote.setdefault(symbol, {}).update(v)

    def _on_create_study(self, p: list) -> None:
        if len(p) < 5: return
        sid = p[0]
        study_id = p[1]
        stype = str(p[4]).split("@")[0]
        inputs = p[5] if len(p) > 5 and isinstance(p[5], dict) else {}
        symbol = self.sessions.get(sid, {}).get("symbol")
        if symbol:
            self.study_defs.setdefault(symbol, {})[study_id] = {
                "type": stype, "inputs": inputs,
            }

    @staticmethod
    def _num(x):
        try:
            return x if isinstance(x, (int, float)) else float(x)
        except (ValueError, TypeError):
            return x

    # ------------------------------------------------------------------
    def get_symbol_data(self, symbol: str, max_points: int = 500) -> dict:
        """داده‌ی کامل برای یک symbol"""
        raw_candles = self.candles.get(symbol, {})
        raw_inds = self.indicators.get(symbol, {})
        defs = self.study_defs.get(symbol, {})

        ind_out = {}
        for sid, tv_map in raw_inds.items():
            meta = defs.get(sid, {})
            keys = sorted(tv_map.keys())
            pts = [{"time": int(t), "values": tv_map[t]} for t in keys]
            if len(pts) > max_points:
                pts = pts[-max_points:]
            ind_out[sid] = {
                "type": meta.get("type", "unknown"),
                "inputs": meta.get("inputs", {}),
                "points_count": len(pts),
                "points": pts,
            }

        return {
            "candles": [raw_candles[t] for t in sorted(raw_candles.keys())],
            "indicators": ind_out,
            "symbol_info": self.symbol_info.get(symbol, {}),
            "quote": self.quote.get(symbol, {}),
            "last_update_ts": self.last_update.get(symbol, 0),
        }

    def known_symbols(self) -> list[str]:
        return sorted(self.sessions[s]["symbol"] for s in self.sessions
                      if self.sessions[s].get("symbol"))


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
    match: bool = False
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
    """محاسبه‌ی آمار کلیدی از کندل‌ها و اندیکاتورها"""

    @staticmethod
    def compute(candles: list[dict], indicators: dict, quote: dict) -> dict:
        if not candles:
            return {}

        last = candles[-1]
        first = candles[0]
        closes = [c["close"] for c in candles]
        highs = [c["high"] for c in candles]
        lows = [c["low"] for c in candles]
        volumes = [c["volume"] for c in candles if c.get("volume") is not None]

        stats: dict[str, Any] = {
            "candles": len(candles),
            "time_from": first["time"],
            "time_to": last["time"],
            "time_from_iso": datetime.fromtimestamp(
                first["time"], tz=timezone.utc).isoformat(),
            "time_to_iso": datetime.fromtimestamp(
                last["time"], tz=timezone.utc).isoformat(),

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
        }
        if volumes:
            stats["volume_total"] = sum(volumes)
            stats["volume_avg"] = sum(volumes) / len(volumes)
            stats["volume_max"] = max(volumes)

        # تغییرات نسبت به کندل اول
        if first["close"]:
            change = last["close"] - first["close"]
            stats["change_absolute"] = round(change, 8)
            stats["change_percent"] = round(change / first["close"] * 100, 4)

        # آخرین مقادیر اندیکاتورها
        latest_indicators = {}
        for sid, ind in indicators.items():
            if isinstance(ind, dict) and ind.get("points"):
                last_pt = ind["points"][-1]
                latest_indicators[ind.get("type", sid)] = {
                    "time": last_pt["time"],
                    "values": last_pt["values"],
                }
        stats["latest_indicators"] = latest_indicators

        # Quote info
        if quote:
            stats["quote_lp"] = quote.get("lp")
            stats["quote_ch"] = quote.get("ch")
            stats["quote_chp"] = quote.get("chp")
            stats["quote_volume"] = quote.get("volume")
            stats["quote_high"] = quote.get("high_price")
            stats["quote_low"] = quote.get("low_price")

        return stats


# ======================================================================
# 🎯 خواننده‌ی اصلی
# ======================================================================
class TradingViewMultiChartReader:
    def __init__(self, panels: list[dict], shell_url: str, output_dir: str):
        self.panels = panels
        self.shell_url = shell_url
        self.output_dir = output_dir
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.parser = TVWebSocketParser()
        self.errors: list[dict] = []
        self.warnings: list[str] = []

    def _err(self, stage: str, err) -> None:
        self.errors.append({
            "stage": stage, "error": str(err),
            "traceback": traceback.format_exc()[:1000],
        })

    # ------------------------------------------------------------------
    # 🎨 init script قبل از لود صفحه
    # ------------------------------------------------------------------
    def _init_script(self) -> str:
        panels_json = json.dumps(self.panels)
        return f"""
            (() => {{
                try {{
                    const layout = String({len(self.panels)});
                    localStorage.setItem('tvp_layout', layout);
                    localStorage.setItem('tvp_panels', JSON.stringify({panels_json}));
                    localStorage.setItem('tvp_preset', '{PRESET}');
                    localStorage.setItem('tvp_sync', '{str(SYNC).lower()}');
                    localStorage.setItem('tvp_theme', '{THEME}');
                    console.log('[EXT] localStorage initialized: layout=' + layout);
                }} catch(e) {{ console.error('[EXT] init err', e); }}
            }})();
        """

    # ------------------------------------------------------------------
    # 📡 WebSocket listener
    # ------------------------------------------------------------------
    async def _setup_ws(self, page: Page) -> None:
        def on_ws(ws):
            try:
                ws.on("framereceived",
                      lambda p: asyncio.create_task(self._on_ws(ws.url, p)))
                ws.on("framesent",
                      lambda p: asyncio.create_task(self._on_ws(ws.url, p)))
            except Exception:
                pass
        page.on("websocket", on_ws)

    async def _on_ws(self, url: str, payload) -> None:
        try:
            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="ignore")
            else:
                text = str(payload)
            if len(text) > 1_000_000:
                return
            if "tradingview" in url:
                self.parser.parse_raw(text)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 🔧 fallback: کلیک روی دکمه‌ی layout
    # ------------------------------------------------------------------
    async def _click_layout_button(self, page: Page, layout: str) -> bool:
        """اگر localStorage کار نکرد، روی دکمه‌ی layout کلیک می‌کنیم"""
        try:
            selector = f'.layout-btn[data-layout="{layout}"]'
            await page.wait_for_selector(selector, timeout=5000)
            await page.click(selector)
            await page.wait_for_timeout(3000)
            log(f"✅ کلیک روی دکمه‌ی layout={layout}", C.GRN, 2)
            return True
        except Exception as e:
            log(f"⚠️  fallback کلیک ناموفق: {e}", C.YEL, 2)
            return False

    # ------------------------------------------------------------------
    # 🔎 پیدا کردن iframeها و نگاشت symbol
    # ------------------------------------------------------------------
    def _parse_iframe_url(self, src: str) -> dict:
        result: dict = {"raw_url": src[:250]}
        try:
            parsed = urlparse(src)
            result["host"] = parsed.netloc
            if parsed.fragment:
                try:
                    hp = json.loads(unquote(parsed.fragment))
                    result["symbol"] = hp.get("symbol")
                    result["interval"] = hp.get("interval")
                    result["studies_raw"] = hp.get("studies", "")
                    result["theme"] = hp.get("theme")
                except Exception:
                    pass
        except Exception:
            pass
        return result

    async def _find_iframes(self, page: Page) -> dict[str, dict]:
        """{symbol: {frame, url, interval, studies_raw}}"""
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
                    log(f"✅ iframe پیدا شد: {sym} (interval={info.get('interval')})",
                        C.GRN, 2)

            if len(result) >= len(self.panels):
                break

        return result

    # ------------------------------------------------------------------
    # 📸 اسکرین‌شات
    # ------------------------------------------------------------------
    async def _screenshot(self, page: Page, suffix: str = "page") -> str:
        try:
            path = os.path.join(self.output_dir, f"{suffix}_{self.ts}.png")
            await page.screenshot(path=path, full_page=False)
            return path
        except Exception as e:
            self._err("screenshot", e)
            return ""

    # ------------------------------------------------------------------
    # 🖼️ canvas info
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # 📊 بررسی پنل‌های DOM
    # ------------------------------------------------------------------
    async def _count_panels(self, page: Page) -> int:
        try:
            return await page.evaluate(
                "() => document.querySelectorAll('.chart-panel').length"
            )
        except Exception:
            return 0

    # ------------------------------------------------------------------
    # 🚀 اجرا
    # ------------------------------------------------------------------
    async def run(self) -> list[ChartExtraction]:
        banner()
        log(f"🎯 URL:      {self.shell_url}", C.YEL)
        log(f"📁 خروجی:   {self.output_dir}", C.YEL)
        log(f"📊 چارت‌ها:  {len(self.panels)}", C.YEL)
        log(f"🎨 Preset:  {PRESET} | Theme: {THEME} | Sync: {SYNC}", C.YEL)
        print()

        os.makedirs(self.output_dir, exist_ok=True)

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

            # 🔑 init script قبل از هر load
            await ctx.add_init_script(self._init_script())
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
            """)

            page = await ctx.new_page()
            await self._setup_ws(page)

            # ---------- ۲. باز کردن صفحه ----------
            section("۲. باز کردن صفحه اصلی")
            try:
                r = await page.goto(self.shell_url,
                                    wait_until="domcontentloaded",
                                    timeout=90000)
                log(f"✅ پاسخ HTTP: {r.status if r else 'N/A'}", C.GRN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YEL, 2)
                self._err("goto", e)

            log(f"⏳ انتظار {WAIT_AFTER_LOAD}s ...", C.CYN, 2)
            await page.wait_for_timeout(WAIT_AFTER_LOAD * 1000)

            # ---------- ۳. بررسی پنل‌ها + fallback ----------
            section("۳. بررسی پنل‌ها")
            panel_count = await self._count_panels(page)
            log(f"📊 پنل‌های ساخته‌شده: {panel_count} (هدف: {len(self.panels)})",
                C.CYN, 2)

            if panel_count < len(self.panels):
                log(f"⚠️  تعداد پنل‌ها کم است. تلاش برای کلیک روی دکمه‌ی layout...",
                    C.YEL, 2)
                layout_str = str(len(self.panels))
                if layout_str not in ("1", "2", "4", "6"):
                    # نزدیک‌ترین
                    if len(self.panels) <= 2: layout_str = "2"
                    elif len(self.panels) <= 4: layout_str = "4"
                    else: layout_str = "6"
                await self._click_layout_button(page, layout_str)
                await page.wait_for_timeout(5000)
                panel_count = await self._count_panels(page)
                log(f"📊 بعد از کلیک: {panel_count} پنل", C.CYN, 2)

            # ---------- ۴. پیدا کردن iframeها ----------
            section("۴. پیدا کردن iframeها")
            iframes_map = await self._find_iframes(page)
            log(f"✅ {len(iframes_map)} iframe پیدا شد", C.GRN, 2)

            # ---------- ۵. جمع‌آوری داده WS ----------
            section(f"۵. جمع‌آوری داده WebSocket (حداکثر {WAIT_FOR_DATA}s)")
            target_symbols = [p["symbol"] for p in self.panels]
            start = datetime.now(timezone.utc).timestamp()

            while True:
                await page.wait_for_timeout(2000)
                elapsed = datetime.now(timezone.utc).timestamp() - start

                # نوار پیشرفت
                parts = []
                for sym in target_symbols:
                    n = len(self.parser.candles.get(sym, {}))
                    parts.append(f"{sym.split(':')[-1]}:{n}")
                print(f"\r  ⏳ {int(elapsed):2d}s | " + " | ".join(parts) + "  ",
                      end="", flush=True)

                # شرایط خروج
                if elapsed >= WAIT_FOR_DATA:
                    break

                all_ready = all(
                    len(self.parser.candles.get(sym, {})) >= MIN_CANDLES_PER_SYMBOL
                    for sym in target_symbols
                )
                if all_ready and elapsed > 8:
                    now = datetime.now(timezone.utc).timestamp()
                    all_idle = all(
                        now - self.parser.last_update.get(sym, 0) > 5
                        for sym in target_symbols
                    )
                    if all_idle:
                        break

            print()
            log(f"✅ جمع‌آوری تمام شد", C.GRN, 2)

            # ---------- ۶. استخراج داده ----------
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
                    except Exception:
                        pass

                actual_ws_symbol = ws_data["symbol_info"].get("pro_name", "")
                match = bool(actual_ws_symbol) and (
                    sym.split(":")[-1] in actual_ws_symbol)

                stats = StatsCalculator.compute(
                    ws_data["candles"], ws_data["indicators"], ws_data["quote"])

                results.append(ChartExtraction(
                    config=panel,
                    iframe_url=iframe_info.get("url", "")[:200],
                    iframe_interval=iframe_info.get("interval", ""),
                    iframe_studies_raw=iframe_info.get("studies_raw", ""),
                    actual_ws_symbol=actual_ws_symbol,
                    match=match,
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
                    f"کندل: {len(ws_data['candles']):4d} | "
                    f"اندیکاتور: {len(ws_data['indicators'])} | "
                    f"WS: {actual_ws_symbol or 'N/A'}",
                    C.GRN if match else C.YEL, 2)

            # اسکرین‌شات نهایی
            await self._screenshot(page, suffix="full_page")

            # ---------- ۷. ذخیره ----------
            section("۷. ذخیره فایل‌ها")
            self._save_all(results)

            await ctx.close()
            await browser.close()

        self._print_summary(results)
        return results

    # ------------------------------------------------------------------
    # 💾 ذخیره‌سازی
    # ------------------------------------------------------------------
    def _save_all(self, results: list[ChartExtraction]) -> None:
        ts = self.ts

        combined = {
            "meta": {
                "shell_url": self.shell_url,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "version": "12.0",
                "panels_requested": len(self.panels),
                "panels_extracted": len(results),
                "preset": PRESET,
                "theme": THEME,
            },
            "websocket_summary": {
                "messages_count": self.parser.messages_count,
                "types_count": dict(self.parser.types_count),
                "sessions": {
                    sid: self.parser.sessions[sid].get("symbol")
                    for sid in self.parser.sessions
                },
                "known_symbols": self.parser.known_symbols(),
            },
            "errors": self.errors,
            "warnings": self.warnings,
            "charts": [asdict(r) for r in results],
        }
        path = os.path.join(self.output_dir, f"all_charts_{ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2, default=str)
        log(f"✅ جامع:    {path}", C.GRN, 2)

        # جداگانه برای هر symbol
        for i, r in enumerate(results):
            sym_key = r.config["symbol"].replace(":", "_")
            base = os.path.join(self.output_dir, f"chart_{i:02d}_{sym_key}_{ts}")

            with open(base + "_full.json", "w", encoding="utf-8") as f:
                json.dump(asdict(r), f, ensure_ascii=False, indent=2, default=str)

            if r.candles:
                with open(base + "_candles.json", "w", encoding="utf-8") as f:
                    json.dump(r.candles, f, ensure_ascii=False, indent=2)

            if r.indicators:
                with open(base + "_indicators.json", "w", encoding="utf-8") as f:
                    json.dump(r.indicators, f, ensure_ascii=False,
                              indent=2, default=str)

            log(f"✅ {r.config['symbol']:22s} "
                f"(candles={r.candles_count}, ind={r.indicators_count})",
                C.GRN, 2)

        # خلاصه‌ی خوانا
        self._save_summary(results)

    def _save_summary(self, results: list[ChartExtraction]) -> None:
        path = os.path.join(self.output_dir, f"summary_{self.ts}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write("=" * 76 + "\n")
            f.write("📊 TradingView Multi-Chart Reader v12.0 - Summary\n")
            f.write("=" * 76 + "\n\n")
            f.write(f"Shell URL:    {self.shell_url}\n")
            f.write(f"Timestamp:    {datetime.now(timezone.utc).isoformat()}\n")
            f.write(f"Panels:       {len(results)}/{len(self.panels)}\n")
            f.write(f"Preset:       {PRESET}\n")
            f.write(f"Theme:        {THEME}\n\n")

            f.write("─" * 76 + "\n[WebSocket Stats]\n" + "─" * 76 + "\n")
            f.write(f"Total messages:  {self.parser.messages_count}\n")
            f.write(f"Message types:   {json.dumps(dict(self.parser.types_count), indent=2)}\n\n")

            for i, r in enumerate(results):
                f.write("─" * 76 + "\n")
                f.write(f"[CHART {i+1}] {r.config['symbol']} "
                        f"(interval={r.config['interval']})\n")
                f.write("─" * 76 + "\n")
                f.write(f"Match (config↔WS): {'✅ YES' if r.match else '❌ NO'}\n")
                f.write(f"WS symbol:         {r.actual_ws_symbol}\n")
                f.write(f"Candles:           {r.candles_count}\n")
                f.write(f"Indicators:        {r.indicators_count}\n")
                f.write(f"Symbol name:       {r.symbol_info.get('description', 'N/A')}\n")
                f.write(f"Exchange:          {r.symbol_info.get('exchange', 'N/A')}\n\n")

                # آمار
                st = r.statistics
                if st:
                    f.write("  ── Statistics ──\n")
                    f.write(f"    last close:      {st.get('last', {}).get('close')}\n")
                    f.write(f"    high max:        {st.get('high_max')}\n")
                    f.write(f"    low min:         {st.get('low_min')}\n")
                    f.write(f"    change %:        {st.get('change_percent')}%\n")
                    f.write(f"    volume total:    {st.get('volume_total')}\n")
                    f.write(f"    volume avg:      {st.get('volume_avg')}\n\n")

                    # آخرین مقادیر اندیکاتورها
                    li = st.get("latest_indicators", {})
                    if li:
                        f.write("  ── Latest Indicators ──\n")
                        for name, info in li.items():
                            f.write(f"    {name}: {info.get('values')}\n")
                        f.write("\n")

                if r.indicators:
                    f.write("  ── Indicators Detail ──\n")
                    for sid, ind in r.indicators.items():
                        if isinstance(ind, dict):
                            f.write(f"    {sid}: {ind.get('type')} "
                                    f"({ind.get('points_count', 0)} points)\n")
                f.write("\n")

            if self.errors:
                f.write("─" * 76 + "\n[ERRORS]\n" + "─" * 76 + "\n")
                for e in self.errors[:15]:
                    f.write(f"  - [{e.get('stage')}] {e.get('error')}\n")

        log(f"✅ خلاصه:   {path}", C.GRN, 2)

    # ------------------------------------------------------------------
    # 📋 خلاصه‌ی ترمینال
    # ------------------------------------------------------------------
    def _print_summary(self, results: list[ChartExtraction]) -> None:
        print()
        print(f"{C.GRN}╔{'═' * 76}╗{C.RESET}")
        print(f"{C.GRN}║{C.BOLD}{C.WHT}          ✅  استخراج تمام شد          {C.RESET}{C.GRN}║{C.RESET}")
        print(f"{C.GRN}╚{'═' * 76}╝{C.RESET}")
        print()

        matched = 0
        for i, r in enumerate(results):
            if r.match:
                matched += 1
            status = "✅" if r.match else "❌"
            last_close = r.statistics.get("last", {}).get("close", "N/A")
            log(f"{status} [{i+1}] {r.config['symbol']:22s} | "
                f"WS: {r.actual_ws_symbol or 'N/A':22s} | "
                f"کندل: {r.candles_count:4d} | "
                f"آخرین قیمت: {last_close}",
                C.WHT)

        print()
        log(f"📊 چارت‌های موفق: {matched}/{len(results)}", C.CYN)
        log(f"🔌 پیام‌های WS:  {self.parser.messages_count}", C.CYN)
        log(f"📁 خروجی:         {os.path.abspath(self.output_dir)}", C.CYN)
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

    log(f"\n📈 چارت‌ها ({len(PANELS)} مورد پیش‌فرض):", C.YEL)
    for i, c in enumerate(PANELS):
        log(f"   [{i+1}] {c['symbol']:22s} | {c['interval']}", C.WHT)
    log("\n   Enter = پیش‌فرض", C.DIM)
    log("   یا: SYMBOL:INTERVAL,SYMBOL:INTERVAL,...", C.DIM)
    log("   مثال: BINANCE:BTCUSDT:60,BINANCE:ETHUSDT:240,TVC:GOLD:D", C.DIM)
    inp = input(f"{C.CYN}لیست: {C.RESET}").strip()

    panels = PANELS
    if inp:
        new_panels = []
        for part in inp.split(","):
            part = part.strip()
            if not part: continue
            m = re.match(r"^([A-Z0-9_.]+:[A-Z0-9_.]+):([0-9]+|[DWM])$",
                         part.upper())
            if m:
                new_panels.append({
                    "symbol": m.group(1),
                    "interval": m.group(2),
                })
            elif ":" in part:
                new_panels.append({"symbol": part.upper(), "interval": "60"})
            else:
                new_panels.append({
                    "symbol": f"BINANCE:{part.upper()}USDT",
                    "interval": "60",
                })
        if new_panels:
            panels = new_panels

    # محدودیت سایت: 1/2/4/6
    if len(panels) not in (1, 2, 4, 6):
        log(f"\n⚠️  سایت فقط 1, 2, 4 یا 6 چارت را پشتیبانی می‌کند. "
            f"تعداد {len(panels)} تنظیم می‌شود.", C.YEL)
        if len(panels) <= 1: panels = panels[:1]
        elif len(panels) <= 2: panels = panels[:2]
        elif len(panels) <= 4: panels = panels[:4]
        else: panels = panels[:6]

    print()
    log("─" * 60, C.DIM)
    log(f"🔗 URL:     {url}", C.WHT)
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

"""
TradingView Chart Extractor - Professional Edition v9.0 (FINAL)
================================================================
رویکرد نهایی:
  ✓ WebSocket parsing (کار می‌کند - 301 کندل + 5 اندیکاتور)
  ✓ Chart API detection (best-effort با تمام روش‌ها)
  ✓ Screenshot per chart
  ✓ Multi-chart concurrent
  ✓ Diagnostics کامل برای عیب‌یابی

اجرا:
    python3 chart_extractor_v9.py
"""

import asyncio
import json
import os
import re
import sys
import traceback
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, unquote

from playwright.async_api import (
    async_playwright, Page, Frame, Response, BrowserContext
)


# ======================================================================
# ⚙️ تنظیمات
# ======================================================================
SHELL_URL = "https://source-donii.ir/tread/index.html"

CHART_CONFIGS = [
    {"symbol": "BINANCE:BTCUSDT", "interval": "60"},
    {"symbol": "BINANCE:ETHUSDT", "interval": "60"},
    {"symbol": "FX:EURUSD", "interval": "60"},
    {"symbol": "TVC:GOLD", "interval": "60"},
]

MAX_CONCURRENT = 2
WAIT_PAGE_LOAD = 6
WAIT_CHART_LOAD = 10
WAIT_FOR_DATA = 35
HEADLESS = True
MAX_CANDLES = 3000
OUTPUT_DIR = "chart_output_v9"


# ======================================================================
# 🎨 رنگ‌ها
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
    print(f"{C.CYN}║{C.BOLD}{C.WHT}   📊  TradingView Chart Extractor - v9.0 FINAL   {C.RESET}{C.CYN}║{C.RESET}")
    print(f"{C.CYN}╚{'═' * 76}╝{C.RESET}")
    print()


# ======================================================================
# 🧩 پارسر WebSocket
# ======================================================================
class TVWebSocketParser:
    """پارسر Socket.IO TradingView"""

    def __init__(self):
        self.candles: dict[float, dict] = {}
        self.indicator_values: dict[str, dict[float, list]] = {}
        self.symbol_info: dict = {}
        self.quote: dict = {}
        self.study_defs: dict[str, dict] = {}
        self.types_count: dict[str, int] = {}
        self.messages_count = 0
        self.last_update_ts = 0.0

    def parse_raw(self, raw: str) -> None:
        for msg in self._split(raw):
            self._handle(msg)

    def _split(self, raw: str) -> list[dict]:
        out = []
        i, n = 0, len(raw)
        while i < n:
            if raw.startswith("~h~", i):
                j = raw.find("~", i + 3)
                if j == -1:
                    break
                i = j + 1
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
                out.append(json.loads(raw[start:end]))
            except json.JSONDecodeError:
                pass
            i = end
        return out

    def _handle(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return
        self.messages_count += 1
        m = msg.get("m", "?")
        self.types_count[m] = self.types_count.get(m, 0) + 1
        p = msg.get("p", [])
        try:
            if m == "symbol_resolved":
                if len(p) >= 3 and isinstance(p[2], dict):
                    self.symbol_info = p[2]
            elif m in ("du", "timescale_update"):
                self._on_data(p)
            elif m == "qsd":
                if len(p) >= 2 and isinstance(p[1], dict):
                    v = p[1].get("v", {})
                    if isinstance(v, dict):
                        self.quote.update(v)
            elif m == "create_study":
                if len(p) >= 5:
                    self.study_defs[p[1]] = {
                        "type": str(p[4]).split("@")[0],
                        "inputs": p[5] if len(p) > 5 and isinstance(p[5], dict) else {},
                    }
        except Exception:
            pass

    def _on_data(self, p: list) -> None:
        if len(p) < 2 or not isinstance(p[1], dict):
            return
        self.last_update_ts = datetime.now(timezone.utc).timestamp()

        for sid, data in p[1].items():
            if not isinstance(data, dict):
                continue
            bars = data.get("s")
            if not isinstance(bars, list):
                bars = data.get("st")
            if not isinstance(bars, list):
                continue
            for bar in bars:
                if not isinstance(bar, dict):
                    continue
                v = bar.get("v")
                if not isinstance(v, list) or len(v) < 2:
                    continue
                try:
                    t = float(v[0])
                except (ValueError, TypeError):
                    continue
                if sid == "sds_1" and len(v) >= 6:
                    self.candles[t] = {
                        "time": int(t),
                        "open": self._num(v[1]), "high": self._num(v[2]),
                        "low": self._num(v[3]), "close": self._num(v[4]),
                        "volume": self._num(v[5]),
                    }
                else:
                    self.indicator_values.setdefault(sid, {})[t] = [
                        self._num(x) for x in v[1:]
                    ]

    @staticmethod
    def _num(x):
        try:
            return x if isinstance(x, (int, float)) else float(x)
        except (ValueError, TypeError):
            return x

    def candles_sorted(self) -> list[dict]:
        return [self.candles[t] for t in sorted(self.candles.keys())]

    def indicators_sorted(self, max_pts: int = 500) -> dict[str, dict]:
        out = {}
        for sid, tv_map in self.indicator_values.items():
            meta = self.study_defs.get(sid, {})
            keys = sorted(tv_map.keys())
            pts = [{"time": int(t), "values": tv_map[t]} for t in keys]
            if len(pts) > max_pts:
                pts = pts[-max_pts:]
            out[sid] = {
                "type": meta.get("type", "unknown"),
                "inputs": meta.get("inputs", {}),
                "points_count": len(pts),
                "points": pts,
            }
        return out


# ======================================================================
# 📊 مدل نتیجه
# ======================================================================
@dataclass
class ChartResult:
    config: dict = field(default_factory=dict)
    iframe_info: dict = field(default_factory=dict)
    chart_api: dict = field(default_factory=dict)
    api_data: dict = field(default_factory=dict)
    candles_api: list = field(default_factory=list)
    candles_ws: list = field(default_factory=list)
    indicators: dict = field(default_factory=dict)
    symbol_info: dict = field(default_factory=dict)
    quote: dict = field(default_factory=dict)
    study_defs: dict = field(default_factory=dict)
    canvas_info: list = field(default_factory=list)
    ws_types: dict = field(default_factory=dict)
    ws_count: int = 0
    screenshot: str = ""
    errors: list = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    api_diag: dict = field(default_factory=dict)
    duration_sec: float = 0.0


# ======================================================================
# 🎯 خواننده تک چارت
# ======================================================================
class SingleChartReader:
    def __init__(self, config: dict, shell_url: str, idx: int = 0):
        self.config = config
        self.shell_url = shell_url
        self.idx = idx
        self.prefix = f"C{idx+1}"
        self.parser = TVWebSocketParser()
        self.errors: list[dict] = []
        self.warnings: list[str] = []

    def _err(self, stage: str, err) -> None:
        self.errors.append({
            "stage": stage, "error": str(err),
            "traceback": traceback.format_exc()[:1000],
        })

    async def _setup_ws(self, page: Page) -> None:
        def on_ws(ws):
            try:
                ws.on("framereceived", lambda p: asyncio.create_task(
                    self._on_ws(ws.url, p)))
                ws.on("framesent", lambda p: asyncio.create_task(
                    self._on_ws(ws.url, p)))
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

    async def _find_iframe(self, page: Page) -> Optional[Frame]:
        for attempt in range(5):
            try:
                await page.wait_for_selector("iframe", timeout=8000)
                break
            except Exception:
                if attempt == 4:
                    self.warnings.append("iframe یافت نشد")
                await page.wait_for_timeout(2000)
        await page.wait_for_timeout(1500)

        for f in page.frames:
            if f == page.main_frame:
                continue
            if f.url.startswith("http") and "tradingview" in f.url.lower():
                return f
        for f in page.frames:
            if f != page.main_frame and f.url.startswith("http"):
                return f
        return None

    # ------------------------------------------------------------------
    # Chart API — بهترین تلاش
    # ------------------------------------------------------------------
    async def _find_chart_api(self, frame: Frame) -> dict:
        """تلاش چندگانه برای پیدا کردن Chart API"""
        try:
            return await frame.evaluate(r"""
                () => {
                    const out = {
                        found: false,
                        source: null,
                        attempts: [],
                        diagnostics: {},
                    };

                    function tryGet(label, getter) {
                        try {
                            const v = getter();
                            if (v && typeof v.getSeries === 'function') {
                                out.attempts.push({path: label, ok: true});
                                return v;
                            }
                            out.attempts.push({
                                path: label, ok: false,
                                kind: v === null ? 'null' :
                                      v === undefined ? 'undef' : typeof v,
                            });
                            return null;
                        } catch (e) {
                            out.attempts.push({
                                path: label, ok: false,
                                err: String(e).slice(0, 100),
                            });
                            return null;
                        }
                    }

                    let chart = null, src = null;

                    // === روش 1: TradingViewApi (v4.0) ===
                    const tvapi = window.TradingViewApi;
                    if (tvapi) {
                        out.diagnostics.TradingViewApi = {
                            type: typeof tvapi,
                            keys: Object.getOwnPropertyNames(tvapi).slice(0, 40),
                            has_activeChart: typeof tvapi.activeChart === 'function',
                        };
                        if (typeof tvapi.activeChart === 'function') {
                            chart = tryGet('TradingViewApi.activeChart()',
                                () => tvapi.activeChart());
                            if (chart) src = 'TradingViewApi.activeChart()';
                        }
                        if (!chart && typeof tvapi.chart === 'function') {
                            chart = tryGet('TradingViewApi.chart()',
                                () => tvapi.chart());
                            if (chart) src = 'TradingViewApi.chart()';
                        }
                    }

                    // === روش 2: chartWidget ===
                    const cw = window.chartWidget;
                    if (!chart && cw) {
                        out.diagnostics.chartWidget = {
                            type: typeof cw,
                            keys: Object.getOwnPropertyNames(cw).slice(0, 40),
                            has_activeChart: typeof cw.activeChart === 'function',
                            has_chart: typeof cw.chart === 'function',
                            is_function: typeof cw === 'function',
                        };
                        if (typeof cw.activeChart === 'function') {
                            chart = tryGet('chartWidget.activeChart()',
                                () => cw.activeChart());
                            if (chart) src = 'chartWidget.activeChart()';
                        }
                        if (!chart && typeof cw.chart === 'function') {
                            chart = tryGet('chartWidget.chart()',
                                () => cw.chart());
                            if (chart) src = 'chartWidget.chart()';
                        }
                        // اگر خودش chart باشه
                        if (!chart && typeof cw.getSeries === 'function') {
                            chart = cw;
                            src = 'chartWidget (as chart)';
                        }
                    }

                    // === روش 3: chartWidgetCollection ===
                    const cwc = window.chartWidgetCollection;
                    if (!chart && cwc) {
                        out.diagnostics.chartWidgetCollection = {
                            keys: Object.getOwnPropertyNames(cwc).slice(0, 40),
                        };
                        // getAll()
                        if (typeof cwc.getAll === 'function') {
                            try {
                                const all = cwc.getAll();
                                out.diagnostics.cwc_getAll_len =
                                    Array.isArray(all) ? all.length : 'not-array';
                                if (Array.isArray(all)) {
                                    for (let i = 0; i < all.length; i++) {
                                        const w = all[i];
                                        if (!w) continue;
                                        if (typeof w.chart === 'function') {
                                            chart = tryGet(
                                                `cwc.getAll()[${i}].chart()`,
                                                () => w.chart());
                                        }
                                        if (!chart && typeof w.activeChart === 'function') {
                                            chart = tryGet(
                                                `cwc.getAll()[${i}].activeChart()`,
                                                () => w.activeChart());
                                        }
                                        if (!chart && typeof w.getSeries === 'function') {
                                            chart = w;
                                        }
                                        if (chart) { src = `cwc.getAll()[${i}]`; break; }
                                    }
                                }
                            } catch (e) {
                                out.diagnostics.cwc_getAll_err = String(e);
                            }
                        }
                        // داخلی‌ها
                        for (const p of ['_chartWidgetCollection', '_items',
                                          '_widgets', '_chartWidgets']) {
                            try {
                                const inner = cwc[p];
                                if (inner && typeof inner.getAll === 'function') {
                                    const all = inner.getAll();
                                    if (Array.isArray(all) && all.length) {
                                        for (let i = 0; i < all.length; i++) {
                                            const w = all[i];
                                            if (!w) continue;
                                            if (typeof w.chart === 'function') {
                                                chart = w.chart();
                                                if (chart && chart.getSeries) {
                                                    src = `cwc.${p}.getAll()[${i}].chart()`;
                                                    break;
                                                }
                                            }
                                            if (typeof w.getSeries === 'function') {
                                                chart = w;
                                                src = `cwc.${p}.getAll()[${i}]`;
                                                break;
                                            }
                                        }
                                        if (chart) break;
                                    }
                                }
                            } catch (e) {}
                        }
                    }

                    // === روش 4: tvWidget ===
                    if (!chart && window.tvWidget) {
                        const w = window.tvWidget;
                        if (typeof w.activeChart === 'function') {
                            chart = tryGet('tvWidget.activeChart()',
                                () => w.activeChart());
                            if (chart) src = 'tvWidget.activeChart()';
                        }
                        if (!chart && typeof w.chart === 'function') {
                            chart = tryGet('tvWidget.chart()',
                                () => w.chart());
                            if (chart) src = 'tvWidget.chart()';
                        }
                    }

                    // === روش 5: جستجوی سطحی window ===
                    if (!chart) {
                        const keys = Object.keys(window);
                        for (const k of keys) {
                            if (k.length > 50) continue;
                            try {
                                const o = window[k];
                                if (!o || typeof o !== 'object') continue;
                                if (typeof o.activeChart === 'function') {
                                    try {
                                        const c = o.activeChart();
                                        if (c && typeof c.getSeries === 'function') {
                                            chart = c;
                                            src = 'window.' + k + '.activeChart()';
                                            break;
                                        }
                                    } catch (e) {}
                                }
                                if (typeof o.chart === 'function') {
                                    try {
                                        const c = o.chart();
                                        if (c && typeof c.getSeries === 'function') {
                                            chart = c;
                                            src = 'window.' + k + '.chart()';
                                            break;
                                        }
                                    } catch (e) {}
                                }
                                if (typeof o.getSeries === 'function' &&
                                    typeof o.symbol === 'function') {
                                    chart = o;
                                    src = 'window.' + k;
                                    break;
                                }
                            } catch (e) {}
                        }
                    }

                    if (chart) {
                        out.found = true;
                        out.source = src;
                    }
                    return out;
                }
            """)
        except Exception as e:
            self._err("find_chart_api", e)
            return {"found": False, "error": str(e)}

    # ------------------------------------------------------------------
    async def _extract_from_chart(self, frame: Frame) -> dict:
        try:
            return await frame.evaluate(f"""
                () => {{
                    const out = {{
                        ok: false, symbol: null, resolution: null,
                        chart_type: null, last_price: null,
                        candles: [], candles_total: 0,
                        studies: [], drawings: [],
                    }};

                    let chart = null;
                    // تلاش یکسان با تابع قبلی
                    const trySources = [
                        () => window.TradingViewApi && window.TradingViewApi.activeChart && window.TradingViewApi.activeChart(),
                        () => window.TradingViewApi && window.TradingViewApi.chart && window.TradingViewApi.chart(),
                        () => window.chartWidget && window.chartWidget.activeChart && window.chartWidget.activeChart(),
                        () => window.chartWidget && window.chartWidget.chart && window.chartWidget.chart(),
                        () => window.tvWidget && window.tvWidget.activeChart && window.tvWidget.activeChart(),
                    ];
                    for (const f of trySources) {{
                        try {{
                            const c = f();
                            if (c && typeof c.getSeries === 'function') {{
                                chart = c; break;
                            }}
                        }} catch (e) {{}}
                    }}
                    if (!chart) return out;
                    out.ok = true;

                    try {{ out.symbol = chart.symbol(); }} catch(e) {{}}
                    try {{ out.resolution = chart.resolution(); }} catch(e) {{}}
                    try {{ out.chart_type = chart.chartType(); }} catch(e) {{}}

                    try {{
                        const s = chart.getSeries();
                        if (s) {{
                            const data = s.data();
                            if (data && Array.isArray(data)) {{
                                out.candles_total = data.length;
                                const lim = data.slice(-{MAX_CANDLES});
                                out.candles = lim.map(b => ({{
                                    time: b.time, open: b.open, high: b.high,
                                    low: b.low, close: b.close, volume: b.volume,
                                }}));
                                if (out.candles.length > 0) {{
                                    out.last_price = out.candles[out.candles.length - 1].close;
                                }}
                            }}
                        }}
                    }} catch(e) {{}}

                    try {{
                        out.studies = chart.getAllStudies();
                    }} catch(e) {{}}

                    try {{
                        const shapes = chart.getAllShapes();
                        out.drawings = shapes.map(s => ({{
                            id: s.id, name: s.name, points: s.points,
                        }}));
                    }} catch(e) {{}}

                    return out;
                }}
            """)
        except Exception as e:
            self._err("extract_chart", e)
            return {"ok": False, "error": str(e)}

    async def _canvas_info(self, frame: Frame) -> list[dict]:
        try:
            return await frame.evaluate("""
                () => {
                    const out = [];
                    document.querySelectorAll('canvas').forEach((c, i) => {
                        out.push({
                            index: i, width: c.width, height: c.height,
                            visible: c.offsetParent !== null,
                        });
                    });
                    return out;
                }
            """)
        except Exception:
            return []

    async def _screenshot(self, page: Page) -> str:
        try:
            path = os.path.join(OUTPUT_DIR, f"screenshot_{self.prefix}.png")
            await page.screenshot(path=path, full_page=False)
            return path
        except Exception as e:
            self._err("screenshot", e)
            return ""

    # ------------------------------------------------------------------
    async def read_single(self, page: Page) -> ChartResult:
        result = ChartResult(config=self.config)
        t0 = asyncio.get_event_loop().time()
        p = self.prefix

        try:
            await self._setup_ws(page)

            log("باز کردن URL...", C.DIM, p)
            try:
                await page.goto(self.shell_url, wait_until="domcontentloaded",
                                timeout=60000)
            except Exception as e:
                self._err("goto", e)
            await page.wait_for_timeout(WAIT_PAGE_LOAD * 1000)

            iframe = await self._find_iframe(page)
            if not iframe:
                self._err("find_iframe", "no iframe")
                result.errors = self.errors
                result.warnings = self.warnings
                return result

            result.iframe_info = self._parse_iframe(iframe.url)
            log(f"iframe: {iframe.url[:70]}...", C.GRN, p)
            await page.wait_for_timeout(WAIT_CHART_LOAD * 1000)

            # API detection
            log("جستجوی Chart API...", C.DIM, p)
            api = await self._find_chart_api(iframe)
            result.chart_api = api

            if api.get("found"):
                log(f"✅ API پیدا شد: {api.get('source')}", C.GRN, p)
                api_data = await self._extract_from_chart(iframe)
                result.api_data = api_data
                result.candles_api = api_data.get("candles", [])
                log(f"   کندل API: {len(result.candles_api)}", C.GRN, p)
            else:
                attempts = api.get("attempts", [])
                log(f"⚠️  API پیدا نشد ({len(attempts)} تلاش)", C.YEL, p)
                # چاپ دیاگ خلاصه
                diag = api.get("diagnostics", {})
                for key in ("TradingViewApi", "chartWidget", "chartWidgetCollection"):
                    if key in diag:
                        info = diag[key]
                        keys_str = ",".join((info.get("keys") or [])[:6])
                        log(f"   {key}: keys=[{keys_str}]", C.DIM, p)

            # Canvas info
            result.canvas_info = await self._canvas_info(iframe)
            log(f"canvas: {len(result.canvas_info)}", C.DIM, p)

            # Screenshot
            result.screenshot = await self._screenshot(page)

            # انتظار WS
            log("جمع‌آوری داده از WS...", C.DIM, p)
            n_c = 0
            for _ in range(WAIT_FOR_DATA):
                await page.wait_for_timeout(1000)
                n_c = len(self.parser.candles)
                if n_c > 0 and self.parser.last_update_ts > 0:
                    idle = datetime.now(timezone.utc).timestamp() - self.parser.last_update_ts
                    if idle > 5:
                        break

            result.candles_ws = self.parser.candles_sorted()
            result.indicators = self.parser.indicators_sorted()
            result.symbol_info = self.parser.symbol_info
            result.quote = self.parser.quote
            result.study_defs = self.parser.study_defs
            result.ws_types = dict(self.parser.types_count)
            result.ws_count = self.parser.messages_count

            n_total = len(result.candles_ws) or len(result.candles_api)
            log(f"📊 نتیجه: کندل={n_total} اندیکاتور={len(result.indicators)} "
                f"WS={result.ws_count}", C.GRN if n_total > 0 else C.YEL, p)

        except Exception as e:
            self._err("read_single", e)
        finally:
            result.errors = self.errors
            result.warnings = self.warnings
            result.duration_sec = round(
                asyncio.get_event_loop().time() - t0, 2)

        return result

    def _parse_iframe(self, src: str) -> dict:
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


# ======================================================================
# 🎯 مدیر
# ======================================================================
class MultiChartManager:
    def __init__(self, configs, shell_url, output_dir):
        self.configs = configs
        self.shell_url = shell_url
        self.output_dir = output_dir
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.results: list[ChartResult] = []

    async def _read_one(self, context, config, index):
        reader = SingleChartReader(config, self.shell_url, idx=index)
        page = await context.new_page()
        try:
            return await reader.read_single(page)
        finally:
            try:
                await page.close()
            except Exception:
                pass

    async def run(self):
        banner()
        log(f"🎯 URL: {self.shell_url}", C.YEL)
        log(f"📁 خروجی: {self.output_dir}", C.YEL)
        log(f"📊 چارت‌ها: {len(self.configs)}", C.YEL)
        print()

        os.makedirs(self.output_dir, exist_ok=True)

        async with async_playwright() as p:
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
                user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/124.0.0.0 Safari/537.36"),
                viewport={"width": 1920, "height": 1080},
                locale="en-US", timezone_id="Asia/Tehran",
                ignore_https_errors=True,
            )
            await ctx.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                window.chrome = { runtime: {}, loadTimes: () => {}, csi: () => {} };
            """)

            sem = asyncio.Semaphore(MAX_CONCURRENT)

            async def worker(cfg, i):
                async with sem:
                    log(f"▶ شروع {cfg['symbol']}", C.BOLD + C.CYN)
                    try:
                        return await self._read_one(ctx, cfg, i)
                    except Exception as e:
                        log(f"❌ {e}", C.RED)
                        empty = ChartResult(config=cfg)
                        empty.errors.append({"stage": "worker",
                                              "error": str(e)})
                        return empty

            self.results = await asyncio.gather(
                *[worker(c, i) for i, c in enumerate(self.configs)])

            await ctx.close()
            await browser.close()

        section("ذخیره")
        self._save_all()
        self._summary()
        return self.results

    def _save_all(self):
        combined = {
            "meta": {
                "shell_url": self.shell_url,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "version": "9.0",
                "charts_count": len(self.configs),
            },
            "charts": [asdict(r) for r in self.results],
        }
        path = os.path.join(self.output_dir, f"all_charts_{self.ts}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(combined, f, ensure_ascii=False, indent=2, default=str)
        log(f"✅ جامع: {path}", C.GRN)

        for i, r in enumerate(self.results):
            sym = (r.config.get("symbol") or f"chart{i}").replace(":", "_")
            base = os.path.join(self.output_dir, f"chart_{i:02d}_{sym}_{self.ts}")

            with open(base + "_full.json", "w", encoding="utf-8") as f:
                json.dump(asdict(r), f, ensure_ascii=False, indent=2, default=str)

            candles = r.candles_api or r.candles_ws
            if candles:
                with open(base + "_candles.json", "w", encoding="utf-8") as f:
                    json.dump(candles, f, ensure_ascii=False, indent=2)

            if r.indicators:
                with open(base + "_indicators.json", "w", encoding="utf-8") as f:
                    json.dump(r.indicators, f, ensure_ascii=False, indent=2, default=str)

            if r.api_diag:
                with open(base + "_api_diag.json", "w", encoding="utf-8") as f:
                    json.dump(r.api_diag, f, ensure_ascii=False, indent=2, default=str)

            log(f"✅ {sym}: candles={len(candles)}, ind={len(r.indicators)}", C.GRN)

        # summary
        sp = os.path.join(self.output_dir, f"summary_{self.ts}.txt")
        with open(sp, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("Chart Extractor v9.0 - Summary\n")
            f.write("=" * 70 + "\n\n")
            for i, r in enumerate(self.results):
                f.write(f"[{i+1}] {r.config.get('symbol')}\n")
                f.write(f"  duration:        {r.duration_sec}s\n")
                f.write(f"  api_found:       {r.chart_api.get('found')}\n")
                f.write(f"  api_source:      {r.chart_api.get('source')}\n")
                f.write(f"  candles_api:     {len(r.candles_api)}\n")
                f.write(f"  candles_ws:      {len(r.candles_ws)}\n")
                f.write(f"  indicators:      {len(r.indicators)}\n")
                f.write(f"  ws_messages:     {r.ws_count}\n")
                f.write(f"  ws_types:        {r.ws_types}\n")
                f.write(f"  last_price_ws:   {r.quote.get('lp')}\n")
                if r.symbol_info:
                    f.write(f"  symbol_name:     {r.symbol_info.get('description')}\n")
                    f.write(f"  exchange:        {r.symbol_info.get('exchange')}\n")
                # API attempts
                atts = r.chart_api.get("attempts", [])
                f.write(f"  api_attempts:    {len(atts)}\n")
                for a in atts[:5]:
                    f.write(f"    - {a.get('path')}: {'OK' if a.get('ok') else 'FAIL'}\n")
                # diag
                diag = r.chart_api.get("diagnostics", {})
                for k, v in diag.items():
                    if isinstance(v, dict):
                        keys = ",".join((v.get("keys") or [])[:8])
                        f.write(f"  diag.{k}: keys=[{keys}]\n")
                f.write("\n")
        log(f"✅ خلاصه: {sp}", C.GRN)

    def _summary(self):
        print()
        print(f"{C.GRN}╔{'═' * 76}╗{C.RESET}")
        print(f"{C.GRN}║{C.BOLD}{C.WHT}          ✅  تمام شد          {C.RESET}{C.GRN}║{C.RESET}")
        print(f"{C.GRN}╚{'═' * 76}╝{C.RESET}")
        print()
        api_n = 0
        for i, r in enumerate(self.results):
            api_ok = r.chart_api.get("found", False)
            if api_ok:
                api_n += 1
            n_api = len(r.candles_api)
            n_ws = len(r.candles_ws)
            n_ind = len(r.indicators)
            sym = r.config.get("symbol", f"chart{i}")
            log(f"{'✅' if (api_ok or n_ws > 10) else '⚠️ '} "
                f"[{i+1}] {sym:22s} | API:{'✅' if api_ok else '❌'} "
                f"| کندل API:{n_api:4d} | WS:{n_ws:4d} "
                f"| اندیکاتور:{n_ind} | {r.duration_sec}s", C.WHT)
        print()
        log(f"📊 API: {api_n}/{len(self.results)}", C.CYN)
        log(f"📁 {os.path.abspath(self.output_dir)}", C.CYN)


# ======================================================================
# 🏁 ورودی
# ======================================================================
async def interactive():
    banner()
    log("📝 Enter = پیش‌فرض\n", C.BOLD)

    log(f"🔗 URL: {SHELL_URL}", C.YEL)
    u = input(f"{C.CYN}URL: {C.RESET}").strip()
    url = u if u else SHELL_URL
    if not url.startswith("http"):
        url = "https://" + url

    log(f"\n📈 چارت‌ها: {len(CHART_CONFIGS)} مورد", C.YEL)
    for i, c in enumerate(CHART_CONFIGS):
        log(f"   {i+1}. {c['symbol']} | {c.get('interval','60')}", C.WHT)
    log("   Enter = پیش‌فرض", C.DIM)
    inp = input(f"{C.CYN}لیست (SYMBOL:INTERVAL,...): {C.RESET}").strip()
    cfgs = CHART_CONFIGS
    if inp:
        new_cfgs = []
        for part in inp.split(","):
            part = part.strip()
            if not part: continue
            if ":" in part:
                last = part.rfind(":")
                head, tail = part[:last], part[last+1:]
                if re.match(r"^\d+$|^[DWM]$", tail):
                    new_cfgs.append({"symbol": head, "interval": tail})
                else:
                    new_cfgs.append({"symbol": part, "interval": "60"})
            else:
                new_cfgs.append({"symbol": part, "interval": "60"})
        if new_cfgs:
            cfgs = new_cfgs

    mgr = MultiChartManager(cfgs, url, OUTPUT_DIR)
    await mgr.run()


def main():
    try:
        asyncio.run(interactive())
    except KeyboardInterrupt:
        log("\n⚠️ متوقف شد", C.YEL); sys.exit(0)
    except Exception as e:
        log(f"\n❌ {e}", C.RED)
        traceback.print_exc(); sys.exit(1)


if __name__ == "__main__":
    main()

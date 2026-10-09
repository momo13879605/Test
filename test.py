"""
TradingView Chart Reader - Professional Edition v3.1
====================================================
استخراج کامل داده‌های چارت TradingView از داخل iframe
با دسترسی به API داخلی widget برای اعمال ترسیمات

اجرا:
    python3 chart_reader.py
"""

import asyncio
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Optional
from urllib.parse import urlparse, parse_qs, unquote

from playwright.async_api import async_playwright, Page, Frame, Response


# ======================================================================
# ⚙️  تنظیمات اصلی
# ======================================================================

TARGET_URL = "https://source-donii.ir/tread/index.html"
OUTPUT_DIR = "chart_output"
WAIT_AFTER_LOAD = 12
WAIT_IFRAME_LOAD = 10
WAIT_MAX_IFRAME = 25           # حداکثر انتظار برای پیدا شدن iframe
HEADLESS = True
CAPTURE_WEBSOCKET = True
MAX_WS_MESSAGES = 500
MAX_API_CALLS = 200
MAX_CANDLES = 1000             # حداکثر تعداد کندل استخراجی


# ======================================================================
# 🎨 رنگ‌های ترمینال
# ======================================================================

class C:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"

    # نام‌های کوتاه برای سازگاری
    R = RESET
    B = BOLD
    D = DIM
    GRN = GREEN
    YEL = YELLOW
    CYN = CYAN
    WHT = WHITE
    MAG = MAGENTA


def log(msg: str = "", color: str = C.WHITE, indent: int = 0) -> None:
    """چاپ پیام با رنگ و تورفتگی"""
    print(" " * indent + f"{color}{msg}{C.RESET}")


def section(title: str) -> None:
    """چاپ یک خط جداکننده با عنوان"""
    print()
    log("━" * 68, C.DIM)
    log(f"  {title}", C.BOLD + C.CYAN)
    log("━" * 68, C.DIM)


def banner() -> None:
    print()
    print(f"{C.CYAN}╔{'═' * 68}╗{C.RESET}")
    print(f"{C.CYAN}║{C.BOLD}{C.WHITE}     📊  TradingView Chart Reader - Professional v3.1     {C.RESET}{C.CYAN}║{C.RESET}")
    print(f"{C.CYAN}╚{'═' * 68}╝{C.RESET}")
    print()


# ======================================================================
# 🎯 کلاس اصلی
# ======================================================================

class TradingViewChartReader:
    """خواننده حرفه‌ای چارت TradingView از داخل iframe"""

    def __init__(self, url: str, output_dir: str = "chart_output"):
        self.url = url
        self.output_dir = output_dir
        self.ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        self.data: dict[str, Any] = {
            "meta": {
                "source_url": url,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "timestamp_local": datetime.now().isoformat(),
                "script_version": "3.1",
            },
            "iframe_info": {},
            "widget_config": {},
            "chart_data": {},
            "candles": [],
            "indicators": {},
            "drawings": [],
            "draw_test": {},
            "symbol_info": {},
            "websocket_messages": [],
            "network_api_calls": [],
            "canvas_info": [],
            "page_dom": {},
            "storage": {},
            "errors": [],
            "warnings": [],
        }

        self._ws_messages: list[dict] = []
        self._api_calls: list[dict] = []

    # ------------------------------------------------------------------
    # 🔧 ابزارهای کمکی
    # ------------------------------------------------------------------
    def _warn(self, msg: str) -> None:
        self.data["warnings"].append(msg)

    def _err(self, stage: str, err: Any) -> None:
        self.data["errors"].append({
            "stage": stage,
            "error": str(err),
            "traceback": traceback.format_exc()[:2000],
        })

    # ------------------------------------------------------------------
    # 🔗 پارس URL iframe
    # ------------------------------------------------------------------
    def _parse_iframe_url(self, iframe_src: str) -> dict[str, Any]:
        result: dict[str, Any] = {"raw_url": iframe_src}
        try:
            parsed = urlparse(iframe_src)
            result["host"] = parsed.netloc
            result["path"] = parsed.path

            qs = parse_qs(parsed.query)
            result["query_params"] = {
                k: v[0] if len(v) == 1 else v for k, v in qs.items()
            }

            if parsed.fragment:
                try:
                    hash_params = json.loads(unquote(parsed.fragment))
                    result["hash_params"] = hash_params
                    result["symbol"] = hash_params.get("symbol")
                    result["interval"] = hash_params.get("interval")
                    result["studies_raw"] = hash_params.get("studies", "")
                    result["theme"] = hash_params.get("theme")
                    result["timezone"] = hash_params.get("timezone")
                    result["style"] = hash_params.get("style")
                except Exception as e:
                    result["hash_parse_error"] = str(e)
        except Exception as e:
            result["parse_error"] = str(e)
        return result

    # ------------------------------------------------------------------
    # 📡 رهگیری WebSocket
    # ------------------------------------------------------------------
    async def _setup_websocket_capture(self, page: Page) -> None:
        if not CAPTURE_WEBSOCKET:
            return

        def on_websocket(ws):
            try:
                log(f"🔌 WebSocket: {ws.url[:90]}", C.DIM, 2)
                ws.on("framereceived", lambda p: asyncio.create_task(
                    self._on_ws_message(ws.url, p, "received")
                ))
                ws.on("framesent", lambda p: asyncio.create_task(
                    self._on_ws_message(ws.url, p, "sent")
                ))
            except Exception as e:
                self._err("websocket_setup", e)

        page.on("websocket", on_websocket)

    async def _on_ws_message(self, ws_url: str, payload, direction: str) -> None:
        try:
            if len(self._ws_messages) >= MAX_WS_MESSAGES:
                return

            if isinstance(payload, bytes):
                text = payload.decode("utf-8", errors="ignore")
            else:
                text = str(payload)

            if len(text) > 100_000:
                return

            is_important = any(kw in text for kw in [
                "timescale", "series", "du", "quote", "price",
                "symbol", "resolution", "study", "create"
            ])
            if not is_important:
                return

            entry = {
                "ws_url": ws_url[:150],
                "direction": direction,
                "size": len(text),
                "preview": text[:2500],
            }

            try:
                entry["parsed"] = json.loads(text)
            except Exception:
                pass

            self._ws_messages.append(entry)
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 📡 رهگیری API
    # ------------------------------------------------------------------
    def _setup_api_capture(self, page: Page) -> None:
        async def on_response(response: Response):
            try:
                if len(self._api_calls) >= MAX_API_CALLS:
                    return

                url = response.url
                if not any(kw in url for kw in [
                    "scanner", "quote", "history", "symbol_info",
                    "pine-facade", "tvdatafeed", "indicators",
                    "/api/", "chart"
                ]):
                    return

                if response.status != 200:
                    return

                ct = response.headers.get("content-type", "")
                if "json" not in ct and "text" not in ct:
                    return

                entry = {"url": url[:300], "status": response.status}

                try:
                    if "json" in ct:
                        body = await response.json()
                        body_str = json.dumps(body, default=str)
                        if len(body_str) < 150_000:
                            entry["body"] = body
                        else:
                            entry["body_preview"] = body_str[:30000]
                except Exception:
                    pass

                self._api_calls.append(entry)
            except Exception:
                pass

        page.on("response", lambda r: asyncio.create_task(on_response(r)))

    # ------------------------------------------------------------------
    # 🖼️ پیدا کردن iframe چارت
    # ------------------------------------------------------------------
    async def _find_all_iframes(self, page: Page) -> list[Frame]:
        """پیدا کردن همه iframeهای مرتبط با TradingView"""
        try:
            await page.wait_for_selector("iframe", timeout=WAIT_MAX_IFRAME * 1000)
        except Exception:
            self._warn("iframe در زمان مقرر پیدا نشد")

        await page.wait_for_timeout(3000)

        frames = page.frames
        tv_frames = []
        other_frames = []

        for frame in frames:
            if frame == page.main_frame:
                continue
            url = frame.url
            if not url.startswith("http"):
                continue
            if any(kw in url for kw in ["tradingview", "widgetembed", "s.tradingview.com"]):
                tv_frames.append(frame)
            else:
                other_frames.append(frame)

        return tv_frames + other_frames

    # ------------------------------------------------------------------
    # 🔍 استخراج از DOM داخل iframe
    # ------------------------------------------------------------------
    async def _extract_chart_basic(self, frame: Frame) -> dict[str, Any]:
        try:
            return await frame.evaluate("""
                () => {
                    const out = {
                        url: window.location.href,
                        title: document.title,
                        ready: document.readyState,
                        canvas_count: 0,
                        widget_container: null,
                        tv_globals: [],
                        has_tv_widget: false,
                        has_tradingview: false,
                        chart_elements: [],
                    };

                    // canvasها
                    const canvases = document.querySelectorAll('canvas');
                    out.canvas_count = canvases.length;
                    canvases.forEach((c, i) => {
                        out.chart_elements.push({
                            type: 'canvas',
                            index: i,
                            width: c.width,
                            height: c.height,
                            className: c.className,
                            id: c.id,
                        });
                    });

                    // آبجکت‌های سراسری
                    try {
                        out.tv_globals = Object.keys(window).filter(k =>
                            /tv|trading|widget|chart|series|datafeed|pine/i.test(k)
                        ).slice(0, 80);
                    } catch (e) {}

                    // بررسی وجود آبجکت‌ها
                    try {
                        if (typeof TradingView !== 'undefined') {
                            out.has_tradingview = true;
                            out.tradingview_keys = Object.keys(TradingView).slice(0, 30);
                        }
                    } catch (e) {}

                    try {
                        if (window.tvWidget && typeof window.tvWidget === 'object') {
                            out.has_tv_widget = true;
                        }
                    } catch (e) {}

                    // کانتینر widget
                    const container = document.querySelector(
                        '.tradingview-widget-container__widget, ' +
                        '[class*="tradingview-widget"], ' +
                        '[id*="tradingview"]'
                    );
                    if (container) {
                        out.widget_container = {
                            id: container.id,
                            className: container.className,
                        };
                    }

                    return out;
                }
            """)
        except Exception as e:
            self._err("extract_chart_basic", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # 🔎 پیدا کردن آبجکت widget
    # ------------------------------------------------------------------
    async def _find_widget_object(self, frame: Frame) -> dict[str, Any]:
        """جستجوی گسترده برای پیدا کردن آبجکت widget"""
        try:
            return await frame.evaluate("""
                () => {
                    const out = {
                        found: false,
                        source: null,
                        has_activeChart: false,
                        methods: [],
                    };

                    const candidates = [
                        { name: 'window.tvWidget', obj: window.tvWidget },
                        { name: 'window.widget', obj: window.widget },
                        { name: 'window.chartWidget', obj: window.chartWidget },
                        { name: 'window._tvWidget', obj: window._tvWidget },
                        { name: 'window.TradingView.widget', obj: window.TradingView && window.TradingView.widget },
                    ];

                    // جستجوی گسترده در window
                    try {
                        for (const key of Object.keys(window)) {
                            try {
                                const obj = window[key];
                                if (obj && typeof obj === 'object' &&
                                    typeof obj.activeChart === 'function') {
                                    candidates.push({ name: 'window.' + key, obj });
                                }
                            } catch (e) {}
                        }
                    } catch (e) {}

                    for (const cand of candidates) {
                        if (cand.obj && typeof cand.obj === 'object') {
                            if (typeof cand.obj.activeChart === 'function') {
                                out.found = true;
                                out.source = cand.name;
                                out.has_activeChart = true;

                                // لیست متدها
                                for (const k in cand.obj) {
                                    try {
                                        if (typeof cand.obj[k] === 'function') {
                                            out.methods.push(k);
                                        }
                                    } catch (e) {}
                                }
                                return out;
                            }
                        }
                    }

                    return out;
                }
            """)
        except Exception as e:
            self._err("find_widget_object", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # 📊 استخراج داده‌ها از widget API
    # ------------------------------------------------------------------
    async def _extract_widget_data(self, frame: Frame) -> dict[str, Any]:
        try:
            return await frame.evaluate(f"""
                () => {{
                    const out = {{
                        widget_found: false,
                        chart_found: false,
                        symbol: null,
                        symbol_info: null,
                        resolution: null,
                        chart_type: null,
                        visible_range: null,
                        series_meta: null,
                        series_data: null,
                        studies: [],
                        study_values: {{}},
                        drawings: [],
                        available_chart_methods: [],
                    }};

                    // پیدا کردن widget
                    let widget = null;
                    const candidates = [
                        window.tvWidget, window.widget, window.chartWidget,
                        window._tvWidget,
                        window.TradingView && window.TradingView.widget
                    ];

                    for (const c of candidates) {{
                        if (c && typeof c === 'object' &&
                            typeof c.activeChart === 'function') {{
                            widget = c;
                            break;
                        }}
                    }}

                    // جستجوی گسترده
                    if (!widget) {{
                        for (const key of Object.keys(window)) {{
                            try {{
                                const obj = window[key];
                                if (obj && typeof obj === 'object' &&
                                    typeof obj.activeChart === 'function') {{
                                    widget = obj;
                                    break;
                                }}
                            }} catch (e) {{}}
                        }}
                    }}

                    if (!widget) {{
                        out.error = 'widget not found';
                        return out;
                    }}
                    out.widget_found = true;

                    // گرفتن chart
                    let chart;
                    try {{
                        chart = widget.activeChart();
                    }} catch (e) {{
                        out.chart_error = String(e);
                        return out;
                    }}

                    if (!chart) {{
                        out.error = 'activeChart returned null';
                        return out;
                    }}
                    out.chart_found = true;

                    // متدهای chart
                    try {{
                        for (const k in chart) {{
                            try {{
                                if (typeof chart[k] === 'function') {{
                                    out.available_chart_methods.push(k);
                                }}
                            }} catch (e) {{}}
                        }}
                    }} catch (e) {{}}

                    // نماد
                    try {{ out.symbol = chart.symbol(); }} catch (e) {{}}
                    try {{ out.symbol_info = chart.symbolExt(); }} catch (e) {{}}
                    try {{ out.resolution = chart.resolution(); }} catch (e) {{}}
                    try {{ out.chart_type = chart.chartType(); }} catch (e) {{}}
                    try {{ out.visible_range = chart.getVisibleRange(); }} catch (e) {{}}

                    // سری اصلی - کندل‌ها
                    try {{
                        const series = chart.getSeries ? chart.getSeries() : null;
                        if (series) {{
                            try {{
                                out.series_meta = series.meta ? series.meta() : null;
                            }} catch (e) {{}}

                            try {{
                                const data = series.data ? series.data() : null;
                                if (data && Array.isArray(data)) {{
                                    const max = {MAX_CANDLES};
                                    const limited = data.slice(-max);
                                    out.series_data = {{
                                        total: data.length,
                                        returned: limited.length,
                                        bars: limited.map(bar => ({{
                                            time: bar.time,
                                            open: bar.open,
                                            high: bar.high,
                                            low: bar.low,
                                            close: bar.close,
                                            volume: bar.volume,
                                        }}))
                                    }};
                                }}
                            }} catch (e) {{
                                out.series_data_error = String(e);
                            }}
                        }}
                    }} catch (e) {{
                        out.series_error = String(e);
                    }}

                    // اندیکاتورها (studies)
                    try {{
                        if (typeof chart.getAllStudies === 'function') {{
                            const studies = chart.getAllStudies();
                            out.studies = studies;
                            for (const st of studies) {{
                                try {{
                                    const s = chart.getStudyById(st.id);
                                    if (!s) continue;

                                    const sd = {{
                                        id: st.id,
                                        name: st.name,
                                        title: st.title,
                                    }};

                                    try {{ sd.values = s.getValues(); }} catch (e) {{}}
                                    try {{ sd.inputs = s.getInputs(); }} catch (e) {{}}
                                    try {{ sd.outputs = s.getOutputs(); }} catch (e) {{}}
                                    try {{
                                        const srs = s.getSeries ? s.getSeries() : [];
                                        sd.series_count = srs.length;
                                    }} catch (e) {{}}

                                    out.study_values[st.name] = sd;
                                }} catch (e) {{
                                    out.study_values[st.name] = {{ error: String(e) }};
                                }}
                            }}
                        }}
                    }} catch (e) {{
                        out.studies_error = String(e);
                    }}

                    // ترسیمات
                    try {{
                        if (typeof chart.getAllShapes === 'function') {{
                            const shapes = chart.getAllShapes();
                            out.drawings = shapes.map(s => ({{
                                id: s.id,
                                name: s.name,
                                points: s.points,
                                properties: s.properties,
                            }}));
                        }}
                    }} catch (e) {{
                        out.drawings_error = String(e);
                    }}

                    return out;
                }}
            """)
        except Exception as e:
            self._err("extract_widget_data", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # 🎨 بررسی امکان ترسیم
    # ------------------------------------------------------------------
    async def _check_drawing_api(self, frame: Frame) -> dict[str, Any]:
        try:
            return await frame.evaluate("""
                () => {
                    const out = {
                        can_create_shape: false,
                        can_create_multipoint: false,
                        can_create_execution: false,
                        available_methods: [],
                        last_price: null,
                    };

                    let widget = null;
                    const candidates = [
                        window.tvWidget, window.widget, window.chartWidget,
                        window._tvWidget,
                    ];
                    for (const c of candidates) {
                        if (c && typeof c === 'object' &&
                            typeof c.activeChart === 'function') {
                            widget = c;
                            break;
                        }
                    }
                    if (!widget) {
                        for (const key of Object.keys(window)) {
                            try {
                                const obj = window[key];
                                if (obj && typeof obj === 'object' &&
                                    typeof obj.activeChart === 'function') {
                                    widget = obj;
                                    break;
                                }
                            } catch (e) {}
                        }
                    }
                    if (!widget) return out;

                    const chart = widget.activeChart();
                    if (!chart) return out;

                    if (typeof chart.createShape === 'function')
                        out.can_create_shape = true;
                    if (typeof chart.createMultipointShape === 'function')
                        out.can_create_multipoint = true;
                    if (typeof chart.createExecutionShape === 'function')
                        out.can_create_execution = true;

                    // گرفتن آخرین قیمت
                    try {
                        const series = chart.getSeries();
                        if (series) {
                            const data = series.data();
                            if (data && data.length > 0) {
                                out.last_price = data[data.length - 1].close;
                            }
                        }
                    } catch (e) {}

                    return out;
                }
            """)
        except Exception as e:
            self._err("check_drawing_api", e)
            return {"error": str(e)}

    # ------------------------------------------------------------------
    # 🖼️ اطلاعات canvasها
    # ------------------------------------------------------------------
    async def _extract_canvas_info(self, frame: Frame) -> list[dict]:
        try:
            return await frame.evaluate("""
                () => {
                    const out = [];
                    document.querySelectorAll('canvas').forEach((c, i) => {
                        out.push({
                            index: i,
                            id: c.id,
                            className: c.className,
                            width: c.width,
                            height: c.height,
                            clientWidth: c.clientWidth,
                            clientHeight: c.clientHeight,
                            offsetX: c.offsetLeft,
                            offsetY: c.offsetTop,
                            visible: c.offsetParent !== null,
                            parentClass: c.parentElement ? c.parentElement.className : null,
                        });
                    });
                    return out;
                }
            """)
        except Exception as e:
            self._err("extract_canvas_info", e)
            return []

    # ------------------------------------------------------------------
    # 💾 استخراج Storage از صفحه اصلی
    # ------------------------------------------------------------------
    async def _extract_storage(self, page: Page) -> None:
        try:
            local = await page.evaluate("""
                () => {
                    const out = {};
                    try {
                        for (let i = 0; i < localStorage.length; i++) {
                            const k = localStorage.key(i);
                            out[k] = localStorage.getItem(k);
                        }
                    } catch (e) {}
                    return out;
                }
            """)
            self.data["storage"]["local"] = local
        except Exception:
            pass

        try:
            session = await page.evaluate("""
                () => {
                    const out = {};
                    try {
                        for (let i = 0; i < sessionStorage.length; i++) {
                            const k = sessionStorage.key(i);
                            out[k] = sessionStorage.getItem(k);
                        }
                    } catch (e) {}
                    return out;
                }
            """)
            self.data["storage"]["session"] = session
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 📸 اسکرین‌شات
    # ------------------------------------------------------------------
    async def _screenshot(self, frame: Frame) -> Optional[str]:
        try:
            el = await frame.query_selector("body")
            if not el:
                return None
            path = os.path.join(self.output_dir, f"chart_{self.ts}.png")
            await el.screenshot(path=path)
            log(f"✅ اسکرین‌شات: {path}", C.GREEN, 2)
            return path
        except Exception as e:
            self._err("screenshot", e)
            return None

    # ------------------------------------------------------------------
    # 💾 ذخیره فایل‌ها
    # ------------------------------------------------------------------
    def _save_files(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)

        # JSON اصلی
        json_path = os.path.join(self.output_dir, f"chart_data_{self.ts}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2, default=str)
        log(f"✅ JSON اصلی:   {json_path}", C.GREEN, 2)

        # کندل‌ها
        if self.data["candles"]:
            p = os.path.join(self.output_dir, f"candles_{self.ts}.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(self.data["candles"], f, ensure_ascii=False, indent=2)
            log(f"✅ کندل‌ها:      {p}", C.GREEN, 2)

        # WebSocket
        if self._ws_messages:
            p = os.path.join(self.output_dir, f"websocket_{self.ts}.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(self._ws_messages, f, ensure_ascii=False, indent=2)
            log(f"✅ WebSocket:    {p}", C.GREEN, 2)

        # API calls
        if self._api_calls:
            p = os.path.join(self.output_dir, f"api_calls_{self.ts}.json")
            with open(p, "w", encoding="utf-8") as f:
                json.dump(self._api_calls, f, ensure_ascii=False, indent=2, default=str)
            log(f"✅ API calls:    {p}", C.GREEN, 2)

        # خلاصه
        self._save_summary()

    def _save_summary(self) -> None:
        p = os.path.join(self.output_dir, f"summary_{self.ts}.txt")
        cd = self.data.get("chart_data", {})
        ifr = self.data.get("iframe_info", {})

        with open(p, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("📊 TradingView Chart Reader v3.1 - Summary\n")
            f.write("=" * 70 + "\n\n")

            f.write(f"Source URL:    {self.url}\n")
            f.write(f"Timestamp:     {self.data['meta']['timestamp_utc']}\n\n")

            f.write("─" * 70 + "\n[IFRAME INFO]\n" + "─" * 70 + "\n")
            f.write(f"Symbol:        {ifr.get('symbol', 'N/A')}\n")
            f.write(f"Interval:      {ifr.get('interval', 'N/A')}\n")
            f.write(f"Studies:       {ifr.get('studies_raw', 'N/A')}\n")
            f.write(f"Theme:         {ifr.get('theme', 'N/A')}\n")
            f.write(f"Timezone:      {ifr.get('timezone', 'N/A')}\n\n")

            f.write("─" * 70 + "\n[CHART DATA]\n" + "─" * 70 + "\n")
            f.write(f"Widget found:  {cd.get('widget_found', False)}\n")
            f.write(f"Chart found:   {cd.get('chart_found', False)}\n")
            f.write(f"Symbol:        {cd.get('symbol', 'N/A')}\n")
            f.write(f"Resolution:    {cd.get('resolution', 'N/A')}\n")
            f.write(f"Chart type:    {cd.get('chart_type', 'N/A')}\n")
            f.write(f"Studies:       {len(cd.get('studies', []))}\n")
            f.write(f"Drawings:      {len(cd.get('drawings', []))}\n")

            if cd.get("series_data"):
                f.write(f"Candles total: {cd['series_data'].get('total', 0)}\n")
                f.write(f"Candles saved: {cd['series_data'].get('returned', 0)}\n")

            f.write("\n" + "─" * 70 + "\n[DRAWING API]\n" + "─" * 70 + "\n")
            dt = self.data.get("draw_test", {})
            f.write(f"createShape():           {dt.get('can_create_shape', False)}\n")
            f.write(f"createMultipointShape(): {dt.get('can_create_multipoint', False)}\n")
            f.write(f"createExecutionShape():  {dt.get('can_create_execution', False)}\n")
            f.write(f"Last price:              {dt.get('last_price', 'N/A')}\n")

            f.write("\n" + "─" * 70 + "\n[STATISTICS]\n" + "─" * 70 + "\n")
            f.write(f"WebSocket messages:  {len(self._ws_messages)}\n")
            f.write(f"API calls:           {len(self._api_calls)}\n")
            f.write(f"Candles:             {len(self.data.get('candles', []))}\n")
            f.write(f"Warnings:            {len(self.data.get('warnings', []))}\n")
            f.write(f"Errors:              {len(self.data.get('errors', []))}\n")

            if self.data.get("errors"):
                f.write("\n" + "─" * 70 + "\n[ERRORS]\n" + "─" * 70 + "\n")
                for e in self.data["errors"][:20]:
                    f.write(f"  - [{e.get('stage')}] {e.get('error')}\n")

            if self.data.get("warnings"):
                f.write("\n" + "─" * 70 + "\n[WARNINGS]\n" + "─" * 70 + "\n")
                for w in self.data["warnings"][:20]:
                    f.write(f"  - {w}\n")

        log(f"✅ خلاصه:       {p}", C.GREEN, 2)

    # ------------------------------------------------------------------
    # 🚀 اجرای اصلی
    # ------------------------------------------------------------------
    async def run(self) -> dict[str, Any]:
        banner()

        log(f"🎯 URL هدف:      {self.url}", C.YELLOW)
        log(f"📁 پوشه خروجی:  {self.output_dir}", C.YELLOW)
        log(f"⏱️  زمان انتظار:  {WAIT_AFTER_LOAD}s + {WAIT_IFRAME_LOAD}s", C.YELLOW)
        log(f"🖥️  حالت:        {'مخفی' if HEADLESS else 'نمایشی'}", C.YELLOW)

        os.makedirs(self.output_dir, exist_ok=True)

        async with async_playwright() as p:
            section("راه‌اندازی مرورگر")

            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--mute-audio",
                ],
            )

            context = await browser.new_context(
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

            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {} };
            """)

            page = await context.new_page()

            # نصب رهگیرها
            try:
                await self._setup_websocket_capture(page)
            except Exception as e:
                self._err("setup_ws", e)

            try:
                self._setup_api_capture(page)
            except Exception as e:
                self._err("setup_api", e)

            # --- باز کردن صفحه ---
            section("باز کردن صفحه اصلی")
            try:
                resp = await page.goto(self.url, wait_until="domcontentloaded", timeout=90000)
                log(f"✅ پاسخ: {resp.status if resp else 'N/A'}", C.GREEN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)
                self._err("goto", e)

            log(f"⏳ انتظار {WAIT_AFTER_LOAD}s برای بارگذاری اولیه...", C.CYAN, 2)
            await page.wait_for_timeout(WAIT_AFTER_LOAD * 1000)

            # --- پیدا کردن iframe ---
            section("جستجوی iframe چارت")
            frames = await self._find_all_iframes(page)

            if not frames:
                log("❌ هیچ iframe‌ای پیدا نشد!", C.RED, 2)
                self._err("find_iframe", "no iframe found")
                await browser.close()
                return self.data

            log(f"✅ {len(frames)} iframe پیدا شد", C.GREEN, 2)
            for i, fr in enumerate(frames):
                log(f"   [{i}] {fr.url[:100]}", C.DIM, 2)

            # انتخاب اولین iframe TradingView
            chart_frame = frames[0]
            for fr in frames:
                if "tradingview" in fr.url.lower():
                    chart_frame = fr
                    break

            log(f"🎯 iframe انتخابی: {chart_frame.url[:100]}", C.CYAN, 2)

            # --- پارس اطلاعات iframe ---
            iframe_info = self._parse_iframe_url(chart_frame.url)
            self.data["iframe_info"] = iframe_info
            self.data["widget_config"] = iframe_info.get("hash_params", {})

            if iframe_info.get("symbol"):
                log(f"   نماد:      {iframe_info['symbol']}", C.WHITE, 2)
            if iframe_info.get("interval"):
                log(f"   تایم‌فریم: {iframe_info['interval']}", C.WHITE, 2)
            if iframe_info.get("studies_raw"):
                log(f"   اندیکاتورها: {iframe_info['studies_raw'][:80]}...", C.WHITE, 2)

            # --- انتظار برای بارگذاری چارت ---
            log(f"⏳ انتظار {WAIT_IFRAME_LOAD}s برای بارگذاری چارت داخل iframe...", C.CYAN, 2)
            await page.wait_for_timeout(WAIT_IFRAME_LOAD * 1000)

            # --- استخراج داده‌های پایه ---
            section("استخراج داده‌های چارت")

            try:
                basic = await self._extract_chart_basic(chart_frame)
                self.data["chart_data"]["basic"] = basic
                log(f"✅ اطلاعات پایه: {basic.get('canvas_count', 0)} canvas پیدا شد", C.GREEN, 2)
                if basic.get("has_tv_widget"):
                    log("✅ window.tvWidget موجود است", C.GREEN, 2)
                if basic.get("has_tradingview"):
                    log("✅ window.TradingView موجود است", C.GREEN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- پیدا کردن widget ---
            try:
                widget_info = await self._find_widget_object(chart_frame)
                self.data["chart_data"]["widget_object"] = widget_info
                if widget_info.get("found"):
                    log(f"✅ آبجکت widget پیدا شد از: {widget_info.get('source')}", C.GREEN, 2)
                else:
                    log("⚠️  آبجکت widget پیدا نشد", C.YELLOW, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- استخراج از widget API ---
            try:
                widget_data = await self._extract_widget_data(chart_frame)
                self.data["chart_data"].update(widget_data)

                if widget_data.get("chart_found"):
                    log(f"✅ chart API در دسترس", C.GREEN, 2)
                    log(f"   نماد:        {widget_data.get('symbol')}", C.WHITE, 2)
                    log(f"   رزولوشن:     {widget_data.get('resolution')}", C.WHITE, 2)
                    log(f"   نوع چارت:    {widget_data.get('chart_type')}", C.WHITE, 2)
                    log(f"   اندیکاتورها: {len(widget_data.get('studies', []))}", C.WHITE, 2)
                    log(f"   ترسیمات:     {len(widget_data.get('drawings', []))}", C.WHITE, 2)
                    if widget_data.get("series_data"):
                        sd = widget_data["series_data"]
                        log(f"   کندل‌ها:      {sd.get('returned', 0)} از {sd.get('total', 0)}", C.WHITE, 2)
                else:
                    log(f"⚠️  chart API در دسترس نیست: {widget_data.get('error', 'unknown')}", C.YELLOW, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- ذخیره کندل‌ها در فیلد جدا ---
            sd = self.data.get("chart_data", {}).get("series_data")
            if sd and sd.get("bars"):
                self.data["candles"] = sd["bars"]

            # --- بررسی API ترسیم ---
            section("بررسی امکان ترسیم")
            try:
                draw_info = await self._check_drawing_api(chart_frame)
                self.data["draw_test"] = draw_info

                if draw_info.get("can_create_shape"):
                    log("✅ createShape() در دسترس", C.GREEN, 2)
                if draw_info.get("can_create_multipoint"):
                    log("✅ createMultipointShape() در دسترس", C.GREEN, 2)
                if draw_info.get("can_create_execution"):
                    log("✅ createExecutionShape() در دسترس", C.GREEN, 2)
                if draw_info.get("last_price"):
                    log(f"💰 آخرین قیمت: {draw_info['last_price']}", C.WHITE, 2)

                if not any([
                    draw_info.get("can_create_shape"),
                    draw_info.get("can_create_multipoint"),
                    draw_info.get("can_create_execution"),
                ]):
                    log("⚠️  هیچ API ترسیمی در دسترس نیست", C.YELLOW, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- اطلاعات canvasها ---
            try:
                canvas_info = await self._extract_canvas_info(chart_frame)
                self.data["canvas_info"] = canvas_info
                log(f"✅ {len(canvas_info)} canvas ذخیره شد", C.GREEN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- Storage ---
            try:
                await self._extract_storage(page)
                log(f"✅ Storage استخراج شد", C.GREEN, 2)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- WebSocket و API ---
            self.data["websocket_messages"] = self._ws_messages
            self.data["network_api_calls"] = self._api_calls
            log(f"✅ {len(self._ws_messages)} پیام WebSocket", C.GREEN, 2)
            log(f"✅ {len(self._api_calls)} درخواست API", C.GREEN, 2)

            # --- اسکرین‌شات ---
            try:
                await self._screenshot(chart_frame)
            except Exception as e:
                log(f"⚠️  {e}", C.YELLOW, 2)

            # --- ذخیره فایل‌ها ---
            section("ذخیره فایل‌های خروجی")
            try:
                self._save_files()
            except Exception as e:
                log(f"❌ خطا در ذخیره: {e}", C.RED, 2)
                self._err("save_files", e)

            await browser.close()

        self._print_summary()
        return self.data

    def _print_summary(self) -> None:
        cd = self.data.get("chart_data", {})
        ifr = self.data.get("iframe_info", {})
        dt = self.data.get("draw_test", {})

        print()
        print(f"{C.GREEN}╔{'═' * 68}╗{C.RESET}")
        print(f"{C.GREEN}║{C.BOLD}{C.WHITE}              ✅  استخراج با موفقیت انجام شد              {C.RESET}{C.GREEN}║{C.RESET}")
        print(f"{C.GREEN}╚{'═' * 68}╝{C.RESET}")
        print()

        log(f"📌 نماد:            {cd.get('symbol', ifr.get('symbol', 'N/A'))}", C.WHITE)
        log(f"⏱️  تایم‌فریم:      {cd.get('resolution', ifr.get('interval', 'N/A'))}", C.WHITE)
        log(f"📊 تعداد کندل:      {len(self.data.get('candles', []))}", C.WHITE)
        log(f"📉 اندیکاتورها:     {len(cd.get('studies', []))}", C.WHITE)
        log(f"✏️  ترسیمات موجود:  {len(cd.get('drawings', []))}", C.WHITE)
        log(f"🖼️  canvasها:      {len(self.data.get('canvas_info', []))}", C.WHITE)
        log(f"🔌 پیام‌های WS:     {len(self.data.get('websocket_messages', []))}", C.WHITE)
        log(f"🌐 API calls:       {len(self.data.get('network_api_calls', []))}", C.WHITE)
        log(f"⚠️  هشدارها:        {len(self.data.get('warnings', []))}", C.WHITE)
        log(f"❌ خطاها:           {len(self.data.get('errors', []))}", C.WHITE)
        print()

        # توانایی‌ها
        log("─" * 68, C.DIM)
        log("🎯 توانایی‌های در دسترس:", C.BOLD + C.CYAN)
        log(f"   Widget API:      {'✅' if cd.get('widget_found') else '❌'}", C.WHITE)
        log(f"   Chart API:       {'✅' if cd.get('chart_found') else '❌'}", C.WHITE)
        log(f"   createShape:     {'✅' if dt.get('can_create_shape') else '❌'}", C.WHITE)
        log(f"   Multipoint:      {'✅' if dt.get('can_create_multipoint') else '❌'}", C.WHITE)
        log(f"   Execution:       {'✅' if dt.get('can_create_execution') else '❌'}", C.WHITE)
        log("─" * 68, C.DIM)

        if cd.get("chart_found"):
            log("✨ API داخلی TradingView آماده اعمال ترسیمات!", C.GREEN + C.BOLD)
        elif cd.get("widget_found"):
            log("⚠️  Widget هست ولی Chart API در دسترس نیست", C.YELLOW)
        else:
            log("⚠️  API داخلی در دسترس نیست - از داده‌های WebSocket استفاده کنید", C.YELLOW)

        print()
        log(f"📁 پوشه خروجی: {os.path.abspath(self.output_dir)}", C.CYAN)
        print()


# ======================================================================
# 🏁 ورودی برنامه
# ======================================================================

async def interactive():
    banner()

    log("📝 وارد کردن تنظیمات (Enter = پیش‌فرض):\n", C.BOLD)

    log(f"🔗 URL پیش‌فرض: {TARGET_URL}", C.YELLOW)
    url_input = input(f"{C.CYAN}   URL جدید: {C.RESET}").strip()
    url = url_input if url_input else TARGET_URL
    if not url.startswith("http"):
        url = "https://" + url

    log(f"\n🖥️  حالت پیش‌فرض: {'مخفی' if HEADLESS else 'نمایشی'}", C.YELLOW)
    log("   1 = مخفی   2 = نمایشی", C.DIM)
    mode = input(f"{C.CYAN}   انتخاب (Enter = پیش‌فرض): {C.RESET}").strip()
    headless = HEADLESS if mode not in ("1", "2") else (mode == "1")

    print()
    log("─" * 60, C.DIM)
    log(f"🔗 URL:    {url}", C.WHITE)
    log(f"🖥️  حالت:  {'مخفی' if headless else 'نمایشی'}", C.WHITE)
    log("─" * 60, C.DIM)
    print()

    confirm = input(f"{C.CYAN}ادامه؟ (Y/n): {C.RESET}").strip().lower()
    if confirm and confirm not in ("y", "yes", "بله", "ب"):
        log("❌ لغو شد.", C.RED)
        return

    globals()["HEADLESS"] = headless
    reader = TradingViewChartReader(url=url, output_dir=OUTPUT_DIR)

    try:
        await reader.run()
    except Exception as e:
        log(f"\n❌ خطای غیرمنتظره: {e}", C.RED)
        traceback.print_exc()


def main():
    try:
        asyncio.run(interactive())
    except KeyboardInterrupt:
        log("\n\n⚠️  متوقف شد.", C.YELLOW)
        sys.exit(0)
    except Exception as e:
        log(f"\n\n❌ خطا: {e}", C.RED)
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
TradingView Data Extractor - Professional Edition
==================================================
استخراج کامل داده‌های چارت TradingView به همراه ذخیره‌سازی در JSON

نحوه اجرا:
    python3 main.py
"""

import asyncio
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

from playwright.async_api import async_playwright, Page, Response, BrowserContext

# ======================================================================
# ⚙️  تنظیمات اصلی (این بخش را ویرایش کنید)
# ======================================================================

# 🔗 آدرس سایت TradingView خود را اینجا وارد کنید
TARGET_URL = "https://source-donii.ir/tread/index.html"

# 📁 پوشه خروجی برای ذخیره فایل‌ها
OUTPUT_DIR = "tv_output"

# ⏱️ مدت زمان انتظار برای بارگذاری کامل چارت (ثانیه)
CHART_LOAD_WAIT = 10

# 🖥️ حالت نمایش مرورگر (True = مخفی، False = نمایش)
HEADLESS = True

# 🌐 زبان صفحه
LOCALE = "en-US"

# 🎭 User-Agent مرورگر
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# ======================================================================
# 🎨 رنگ‌های ترمینال
# ======================================================================

class Colors:
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


def c(text: str, color: str) -> str:
    """رنگ‌آمیزی متن"""
    return f"{color}{text}{Colors.RESET}"


def banner() -> None:
    """نمایش بنر شروع"""
    print()
    print(c("╔" + "═" * 68 + "╗", Colors.CYAN))
    print(c("║" + " " * 18 + "📊  TradingView Data Extractor  📊" + " " * 17 + "║", Colors.CYAN + Colors.BOLD))
    print(c("║" + " " * 22 + "Professional Edition  v2.0" + " " * 21 + "║", Colors.CYAN))
    print(c("╚" + "═" * 68 + "╝", Colors.CYAN))
    print()


# ======================================================================
# 🎯 کلاس اصلی استخراج‌کننده
# ======================================================================

class TradingViewExtractor:
    """استخراج‌کننده جامع داده‌های TradingView"""

    def __init__(self, url: str, output_dir: str = "tv_output"):
        self.url = url
        self.output_dir = output_dir
        self.timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

        # ذخیره‌سازی داده‌های استخراج‌شده
        self.data: dict[str, Any] = {
            "meta": {
                "url": url,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                "timestamp_local": datetime.now().isoformat(),
                "script_version": "2.0",
            },
            "page_info": {},
            "dom_elements": {},
            "network_responses": [],
            "api_responses": {},
            "console_logs": [],
            "cookies": [],
            "local_storage": {},
            "session_storage": {},
            "scripts_content": [],
            "stylesheets": [],
            "images": [],
            "links": [],
            "raw_html": "",
            "errors": [],
        }

        # شمارنده‌های داخلی
        self._response_count = 0
        self._max_responses = 500  # حداکثر تعداد پاسخ‌های ذخیره‌شده

    # ------------------------------------------------------------------
    # 📡 رهگیری پاسخ‌های شبکه
    # ------------------------------------------------------------------
    async def _on_response(self, response: Response) -> None:
        """ذخیره پاسخ‌های شبکه"""
        try:
            if self._response_count >= self._max_responses:
                return

            url = response.url
            status = response.status
            content_type = response.headers.get("content-type", "")

            # فقط پاسخ‌های موفق و مهم
            if status != 200:
                return

            # فقط پاسخ‌های JSON و APIهای مهم
            is_interesting = (
                "json" in content_type
                or "tradingview.com" in url
                or "/api/" in url
                or "scanner" in url
                or "quote" in url
                or "history" in url
                or "symbol" in url
                or "indicator" in url
            )

            if not is_interesting:
                return

            entry = {
                "url": url,
                "status": status,
                "content_type": content_type,
            }

            # تلاش برای خواندن بدنه
            try:
                if "json" in content_type:
                    body = await response.json()
                    # محدود کردن حجم داده
                    body_str = json.dumps(body, default=str)
                    if len(body_str) > 100_000:
                        entry["body_truncated"] = True
                        entry["body_preview"] = body_str[:50_000]
                    else:
                        entry["body"] = body
                elif "text" in content_type or "html" in content_type:
                    text = await response.text()
                    entry["body_preview"] = text[:10_000]
                    entry["body_size"] = len(text)
            except Exception:
                entry["body_error"] = "unable to read body"

            self.data["network_responses"].append(entry)
            self._response_count += 1

        except Exception:
            pass

    # ------------------------------------------------------------------
    # 🖥️ ثبت لاگ‌های کنسول مرورگر
    # ------------------------------------------------------------------
    def _on_console(self, msg) -> None:
        """ذخیره پیام‌های کنسول"""
        try:
            self.data["console_logs"].append({
                "type": msg.type,
                "text": msg.text[:500],
            })
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 🚨 ثبت خطاهای صفحه
    # ------------------------------------------------------------------
    def _on_page_error(self, error) -> None:
        self.data["errors"].append({"type": "page_error", "message": str(error)})

    # ------------------------------------------------------------------
    # 🔍 استخراج جامع از DOM
    # ------------------------------------------------------------------
    async def _extract_dom(self, page: Page) -> dict[str, Any]:
        """استخراج همه‌چیز از DOM"""
        print(c("  🔍 استخراج داده‌های DOM...", Colors.CYAN))

        result = await page.evaluate("""
            () => {
                const output = {};

                // --- اطلاعات پایه صفحه ---
                output.title = document.title;
                output.url = window.location.href;
                output.referrer = document.referrer;
                output.readyState = document.readyState;
                output.charset = document.characterSet;
                output.lang = document.documentElement.lang;

                // --- Meta tags ---
                output.meta_tags = {};
                document.querySelectorAll('meta').forEach(m => {
                    const key = m.name || m.getAttribute('property') || m.getAttribute('http-equiv');
                    if (key) output.meta_tags[key] = m.content;
                });

                // --- همه لینک‌ها ---
                output.links = Array.from(document.querySelectorAll('a[href]')).map(a => ({
                    href: a.href,
                    text: a.textContent.trim().substring(0, 100),
                    target: a.target
                })).slice(0, 200);

                // --- همه تصاویر ---
                output.images = Array.from(document.querySelectorAll('img')).map(img => ({
                    src: img.src,
                    alt: img.alt,
                    width: img.naturalWidth,
                    height: img.naturalHeight
                })).slice(0, 100);

                // --- همه استایل‌شیت‌ها ---
                output.stylesheets = Array.from(document.querySelectorAll('link[rel="stylesheet"]'))
                    .map(l => l.href);

                // --- همه اسکریپت‌های خارجی ---
                output.external_scripts = Array.from(document.querySelectorAll('script[src]'))
                    .map(s => ({ src: s.src, async: s.async, defer: s.defer }));

                // --- اسکریپت‌های inline ---
                output.inline_scripts = Array.from(document.querySelectorAll('script:not([src])'))
                    .map(s => s.textContent.substring(0, 2000))
                    .filter(t => t.trim().length > 0)
                    .slice(0, 50);

                // --- استایل‌های inline ---
                output.inline_styles = Array.from(document.querySelectorAll('style'))
                    .map(s => s.textContent.substring(0, 3000))
                    .slice(0, 30);

                // --- داده‌های چارت TradingView ---
                output.tv_data = {};

                // عنوان نماد
                const symbolTitle = document.querySelector('.title-l31H9iuA, [data-name="legend-source-title"]');
                if (symbolTitle) output.tv_data.symbol_title = symbolTitle.textContent.trim();

                // قیمت فعلی
                const priceEls = document.querySelectorAll('.lastPrice-3pL2A3w0, .price-l31H9iuA, [class*="lastPrice"], [class*="price-"]');
                output.tv_data.prices = Array.from(priceEls).map(el => ({
                    text: el.textContent.trim(),
                    className: el.className
                })).slice(0, 10);

                // تغییرات
                const changeEls = document.querySelectorAll('[class*="change-"], [class*="Change-"]');
                output.tv_data.changes = Array.from(changeEls).map(el => ({
                    text: el.textContent.trim(),
                    className: el.className
                })).slice(0, 10);

                // اندیکاتورهای legend
                const legendItems = document.querySelectorAll('[data-name="legend-source-item"], [class*="legend"]');
                output.tv_data.legend_items = Array.from(legendItems).map(el => ({
                    text: el.textContent.trim().substring(0, 200),
                    className: el.className
                })).slice(0, 30);

                // همه متن‌های قابل مشاهده در پنل کناری
                const sidebar = document.querySelector('[class*="sidebar"], [class*="right-toolbar"], aside');
                if (sidebar) {
                    output.tv_data.sidebar_text = sidebar.innerText.substring(0, 5000);
                }

                // همه دکمه‌ها
                output.buttons = Array.from(document.querySelectorAll('button')).map(b => ({
                    text: b.textContent.trim().substring(0, 100),
                    className: b.className,
                    id: b.id,
                    ariaLabel: b.getAttribute('aria-label'),
                    title: b.getAttribute('title'),
                    disabled: b.disabled
                })).slice(0, 100);

                // همه inputها
                output.inputs = Array.from(document.querySelectorAll('input, select, textarea')).map(i => ({
                    tag: i.tagName.toLowerCase(),
                    type: i.type,
                    name: i.name,
                    id: i.id,
                    placeholder: i.placeholder,
                    value: (i.type === 'password' ? '***' : i.value)
                })).slice(0, 100);

                // کلاس‌های اصلی TradingView
                const tvClasses = new Set();
                document.querySelectorAll('[class*="chart"], [class*="tv-"], [class*="price"], [class*="symbol"]').forEach(el => {
                    el.classList.forEach(cls => {
                        if (cls.length > 3 && cls.length < 60) tvClasses.add(cls);
                    });
                });
                output.tv_classes = Array.from(tvClasses).slice(0, 200);

                // اندازه صفحه و وضعیت چارت
                output.viewport = {
                    width: window.innerWidth,
                    height: window.innerHeight,
                    scrollX: window.scrollX,
                    scrollY: window.scrollY
                };

                // canvas‌های موجود (چارت اصلی معمولاً canvas است)
                output.canvases = Array.from(document.querySelectorAll('canvas')).map(c => ({
                    width: c.width,
                    height: c.height,
                    className: c.className,
                    id: c.id
                }));

                return output;
            }
        """)

        return result

    # ------------------------------------------------------------------
    # 💾 استخراج Storage
    # ------------------------------------------------------------------
    async def _extract_storage(self, context: BrowserContext, page: Page) -> None:
        """استخراج Cookies و Storage"""
        print(c("  💾 استخراج Cookies و Storage...", Colors.CYAN))

        try:
            cookies = await context.cookies()
            self.data["cookies"] = cookies
        except Exception as e:
            self.data["errors"].append({"type": "cookie_error", "message": str(e)})

        try:
            local_storage = await page.evaluate("""
                () => {
                    const ls = {};
                    for (let i = 0; i < localStorage.length; i++) {
                        const key = localStorage.key(i);
                        const value = localStorage.getItem(key);
                        ls[key] = value ? value.substring(0, 2000) : value;
                    }
                    return ls;
                }
            """)
            self.data["local_storage"] = local_storage
        except Exception:
            pass

        try:
            session_storage = await page.evaluate("""
                () => {
                    const ss = {};
                    for (let i = 0; i < sessionStorage.length; i++) {
                        const key = sessionStorage.key(i);
                        const value = sessionStorage.getItem(key);
                        ss[key] = value ? value.substring(0, 2000) : value;
                    }
                    return ss;
                }
            """)
            self.data["session_storage"] = session_storage
        except Exception:
            pass

    # ------------------------------------------------------------------
    # 📸 گرفتن اسکرین‌شات
    # ------------------------------------------------------------------
    async def _take_screenshot(self, page: Page) -> None:
        """ذخیره اسکرین‌شات کامل صفحه"""
        print(c("  📸 گرفتن اسکرین‌شات...", Colors.CYAN))
        try:
            path = os.path.join(self.output_dir, f"screenshot_{self.timestamp}.png")
            await page.screenshot(path=path, full_page=True)
            self.data["meta"]["screenshot"] = path
            print(c(f"  ✅ اسکرین‌شات ذخیره شد: {path}", Colors.GREEN))
        except Exception as e:
            self.data["errors"].append({"type": "screenshot_error", "message": str(e)})

    # ------------------------------------------------------------------
    # 💾 ذخیره در فایل
    # ------------------------------------------------------------------
    def _save_files(self) -> None:
        """ذخیره همه داده‌ها در فایل‌های خروجی"""
        os.makedirs(self.output_dir, exist_ok=True)

        # --- JSON اصلی ---
        json_path = os.path.join(self.output_dir, f"data_{self.timestamp}.json")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2, default=str)
        print(c(f"  ✅ JSON اصلی ذخیره شد: {json_path}", Colors.GREEN))

        # --- HTML خام ---
        html_path = os.path.join(self.output_dir, f"page_{self.timestamp}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(self.data.get("raw_html", ""))
        print(c(f"  ✅ HTML ذخیره شد: {html_path}", Colors.GREEN))

        # --- خلاصه متنی ---
        summary_path = os.path.join(self.output_dir, f"summary_{self.timestamp}.txt")
        with open(summary_path, "w", encoding="utf-8") as f:
            f.write("=" * 70 + "\n")
            f.write("📊 TradingView Extraction Summary\n")
            f.write("=" * 70 + "\n\n")
            f.write(f"URL: {self.data['meta']['url']}\n")
            f.write(f"Timestamp: {self.data['meta']['timestamp_utc']}\n\n")

            f.write(f"عنوان صفحه: {self.data['dom_elements'].get('title', 'N/A')}\n\n")

            f.write(f"تعداد پاسخ‌های شبکه: {len(self.data['network_responses'])}\n")
            f.write(f"تعداد لینک‌ها: {len(self.data['dom_elements'].get('links', []))}\n")
            f.write(f"تعداد تصاویر: {len(self.data['dom_elements'].get('images', []))}\n")
            f.write(f"تعداد دکمه‌ها: {len(self.data['dom_elements'].get('buttons', []))}\n")
            f.write(f"تعداد inputها: {len(self.data['dom_elements'].get('inputs', []))}\n")
            f.write(f"تعداد اسکریپت‌های خارجی: {len(self.data['dom_elements'].get('external_scripts', []))}\n")
            f.write(f"تعداد استایل‌شیت‌ها: {len(self.data['dom_elements'].get('stylesheets', []))}\n")
            f.write(f"تعداد canvasها: {len(self.data['dom_elements'].get('canvases', []))}\n")
            f.write(f"تعداد Cookies: {len(self.data['cookies'])}\n")
            f.write(f"تعداد خطاها: {len(self.data['errors'])}\n\n")

            f.write("-" * 70 + "\n")
            f.write("📈 داده‌های چارت TradingView:\n")
            f.write("-" * 70 + "\n")
            tv_data = self.data["dom_elements"].get("tv_data", {})
            for key, value in tv_data.items():
                f.write(f"\n[{key}]:\n")
                f.write(json.dumps(value, ensure_ascii=False, indent=2, default=str)[:3000] + "\n")

        print(c(f"  ✅ خلاصه متنی ذخیره شد: {summary_path}", Colors.GREEN))

    # ------------------------------------------------------------------
    # 🚀 اجرای اصلی
    # ------------------------------------------------------------------
    async def run(self) -> None:
        banner()

        print(c(f"🎯 آدرس هدف: {self.url}", Colors.YELLOW))
        print(c(f"📁 پوشه خروجی: {self.output_dir}", Colors.YELLOW))
        print(c(f"⏱️  مدت انتظار: {CHART_LOAD_WAIT} ثانیه", Colors.YELLOW))
        print(c(f"🖥️  حالت مخفی: {HEADLESS}", Colors.YELLOW))
        print()

        os.makedirs(self.output_dir, exist_ok=True)

        async with async_playwright() as p:
            print(c("🚀 راه‌اندازی مرورگر Chromium...", Colors.CYAN))

            browser = await p.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-blink-features=AutomationControlled",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-accelerated-2d-canvas",
                    "--disable-background-timer-throttling",
                ],
            )

            context = await browser.new_context(
                user_agent=USER_AGENT,
                viewport={"width": 1920, "height": 1080},
                locale=LOCALE,
                timezone_id="UTC",
                ignore_https_errors=True,
            )

            # پاک‌سازی ردپای خودکارسازی
            await context.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
                Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
                window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {} };
            """)

            page = await context.new_page()

            # --- اتصال event handlerها ---
            page.on("response", lambda r: asyncio.create_task(self._on_response(r)))
            page.on("console", self._on_console)
            page.on("pageerror", self._on_page_error)

            # --- باز کردن صفحه ---
            print(c(f"📡 در حال باز کردن صفحه...", Colors.CYAN))
            try:
                await page.goto(self.url, wait_until="domcontentloaded", timeout=90000)
                print(c("  ✅ صفحه باز شد", Colors.GREEN))
            except Exception as e:
                print(c(f"  ⚠️  هشدار: {e}", Colors.YELLOW))

            # --- انتظار برای بارگذاری چارت ---
            print(c(f"⏳ انتظار {CHART_LOAD_WAIT} ثانیه برای بارگذاری کامل چارت...", Colors.CYAN))
            await page.wait_for_timeout(CHART_LOAD_WAIT * 1000)

            # --- تلاش برای پیدا کردن canvas چارت ---
            print(c("🔎 بررسی وجود چارت...", Colors.CYAN))
            try:
                has_canvas = await page.evaluate(
                    "() => document.querySelectorAll('canvas').length > 0"
                )
                if has_canvas:
                    canvas_count = await page.evaluate(
                        "() => document.querySelectorAll('canvas').length"
                    )
                    print(c(f"  ✅ {canvas_count} canvas پیدا شد (چارت در حال رندر است)", Colors.GREEN))
                else:
                    print(c("  ⚠️  canvas پیدا نشد، احتمالاً چارت هنوز بارگذاری نشده", Colors.YELLOW))
            except Exception:
                pass

            # --- استخراج DOM ---
            print()
            print(c("📦 شروع استخراج داده‌ها...", Colors.BOLD + Colors.MAGENTA))
            dom_data = await self._extract_dom(page)
            self.data["dom_elements"] = dom_data

            # --- ذخیره HTML خام ---
            try:
                self.data["raw_html"] = await page.content()
                print(c(f"  ✅ HTML خام استخراج شد ({len(self.data['raw_html'])} کاراکتر)", Colors.GREEN))
            except Exception as e:
                self.data["errors"].append({"type": "html_error", "message": str(e)})

            # --- Storage ---
            await self._extract_storage(context, page)

            # --- اسکرین‌شات ---
            await self._take_screenshot(page)

            # --- ذخیره فایل‌ها ---
            print()
            print(c("💾 ذخیره فایل‌های خروجی...", Colors.BOLD + Colors.MAGENTA))
            self._save_files()

            # --- بستن مرورگر ---
            await browser.close()

            # --- نمایش خلاصه نهایی ---
            print()
            print(c("╔" + "═" * 68 + "╗", Colors.GREEN))
            print(c("║" + " " * 22 + "✅  استخراج با موفقیت انجام شد" + " " * 22 + "║", Colors.GREEN + Colors.BOLD))
            print(c("╚" + "═" * 68 + "╝", Colors.GREEN))
            print()
            print(c(f"📁 پوشه خروجی: {os.path.abspath(self.output_dir)}", Colors.CYAN))
            print(c(f"📄 فایل‌های ذخیره‌شده:", Colors.CYAN))
            print(c(f"   • JSON اصلی (تمام داده‌ها)", Colors.WHITE))
            print(c(f"   • HTML خام صفحه", Colors.WHITE))
            print(c(f"   • خلاصه متنی", Colors.WHITE))
            print(c(f"   • اسکرین‌شات PNG", Colors.WHITE))
            print()

    # ------------------------------------------------------------------
    # 📊 نمایش آمار
    # ------------------------------------------------------------------
    def print_stats(self) -> None:
        """نمایش آمار نهایی"""
        d = self.data
        print(c("─" * 70, Colors.DIM))
        print(c("📊 آمار استخراج:", Colors.BOLD))
        print(c(f"  • پاسخ‌های شبکه:      {len(d['network_responses'])}", Colors.WHITE))
        print(c(f"  • لینک‌ها:              {len(d['dom_elements'].get('links', []))}", Colors.WHITE))
        print(c(f"  • تصاویر:              {len(d['dom_elements'].get('images', []))}", Colors.WHITE))
        print(c(f"  • دکمه‌ها:              {len(d['dom_elements'].get('buttons', []))}", Colors.WHITE))
        print(c(f"  • inputها:             {len(d['dom_elements'].get('inputs', []))}", Colors.WHITE))
        print(c(f"  • اسکریپت‌های خارجی:  {len(d['dom_elements'].get('external_scripts', []))}", Colors.WHITE))
        print(c(f"  • canvasها:            {len(d['dom_elements'].get('canvases', []))}", Colors.WHITE))
        print(c(f"  • Cookies:             {len(d['cookies'])}", Colors.WHITE))
        print(c(f"  • خطاها:               {len(d['errors'])}", Colors.WHITE))
        print(c("─" * 70, Colors.DIM))


# ======================================================================
# 🎯 تعامل با کاربر (Interactive Mode)
# ======================================================================

async def interactive_mode() -> None:
    """حالت تعاملی: از کاربر اطلاعات می‌گیرد"""
    banner()

    print(c("📝 وارد کردن تنظیمات (برای مقادیر پیش‌فرض Enter بزنید):\n", Colors.BOLD))

    # --- آدرس سایت ---
    print(c(f"🔗 آدرس پیش‌فرض: {TARGET_URL}", Colors.YELLOW))
    url_input = input(c("   آدرس جدید (Enter = پیش‌فرض): ", Colors.CYAN)).strip()
    url = url_input if url_input else TARGET_URL
    if not url.startswith("http"):
        url = "https://" + url

    # --- مدت انتظار ---
    print(c(f"\n⏱️  مدت انتظار پیش‌فرض: {CHART_LOAD_WAIT} ثانیه", Colors.YELLOW))
    wait_input = input(c("   مدت انتظار جدید (Enter = پیش‌فرض): ", Colors.CYAN)).strip()
    try:
        wait_time = int(wait_input) if wait_input else CHART_LOAD_WAIT
    except ValueError:
        wait_time = CHART_LOAD_WAIT

    # --- حالت مرورگر ---
    print(c(f"\n🖥️  حالت پیش‌فرض: {'مخفی (Headless)' if HEADLESS else 'نمایشی (Headed)'}", Colors.YELLOW))
    print(c("   1 = مخفی", Colors.DIM))
    print(c("   2 = نمایشی", Colors.DIM))
    mode_input = input(c("   انتخاب (Enter = پیش‌فرض): ", Colors.CYAN)).strip()
    if mode_input == "1":
        headless = True
    elif mode_input == "2":
        headless = False
    else:
        headless = HEADLESS

    # --- تأیید ---
    print()
    print(c("─" * 70, Colors.DIM))
    print(c("📋 تنظیمات نهایی:", Colors.BOLD))
    print(c(f"   🔗 URL:        {url}", Colors.WHITE))
    print(c(f"   ⏱️  انتظار:    {wait_time} ثانیه", Colors.WHITE))
    print(c(f"   🖥️  حالت:      {'مخفی' if headless else 'نمایشی'}", Colors.WHITE))
    print(c("─" * 70, Colors.DIM))
    print()

    confirm = input(c("ادامه؟ (Y/n): ", Colors.CYAN)).strip().lower()
    if confirm and confirm not in ("y", "yes", "بله", "ب"):
        print(c("❌ لغو شد.", Colors.RED))
        return

    print()
    extractor = TradingViewExtractor(url=url, output_dir=OUTPUT_DIR)
    # override settings
    globals()["HEADLESS"] = headless
    globals()["CHART_LOAD_WAIT"] = wait_time

    await extractor.run()
    extractor.print_stats()


# ======================================================================
# 🏁 ورودی برنامه
# ======================================================================

def main() -> None:
    """نقطه ورود برنامه"""

    # اجرا در حالت تعاملی
    try:
        asyncio.run(interactive_mode())
    except KeyboardInterrupt:
        print(c("\n\n⚠️  توسط کاربر متوقف شد.", Colors.YELLOW))
        sys.exit(0)
    except Exception as e:
        print(c(f"\n\n❌ خطای غیرمنتظره: {e}", Colors.RED))
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

import argparse
import re
from pathlib import Path
from typing import Set
from urllib.parse import quote

import scrapy
from scrapy.crawler import CrawlerProcess


class MatwebSpider(scrapy.Spider):
    name = "matweb"
    custom_settings = {}

    def __init__(
        self,
        query: str,
        out_dir: str,
        count: int,
        resume: bool,
        use_playwright: bool,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.out_dir = Path(out_dir)
        self.count = count
        self.resume = resume
        self.use_playwright = use_playwright
        self.seen: Set[str] = set()
        self.visited_pages: Set[str] = set()
        self.saved = 0

    def start_requests(self):
        # 入口：搜索结果页
        url = f"https://www.matweb.com/search/QuickText.aspx?SearchText={quote(self.query)}"
        meta = {"playwright": True} if self.use_playwright else {}
        yield scrapy.Request(url=url, callback=self.parse_search, meta=meta)

    def parse_search(self, response):
        # 搜索页解析：提取 datasheet 链接 + 翻页
        self.visited_pages.add(response.url)
        links = response.xpath("//a[contains(@href,'DataSheet.aspx')]/@href").getall()
        if not links:
            links = response.css("a[href*='DataSheet.aspx']::attr(href)").getall()
        if not links:
            matches = re.findall(r'href=[\"\\\']([^\"\\\']*DataSheet\\.aspx[^\"\\\']*)', response.text)
            links = list(dict.fromkeys(matches))
        if not links:
            # 保存调试页，便于分析站点返回结构
            self.out_dir.mkdir(parents=True, exist_ok=True)
            debug_path = self.out_dir / "matweb_search_debug.html"
            debug_path.write_bytes(response.body)
            self.logger.warning("No datasheet links found, saved %s", debug_path)
            return
        self.logger.info("Found %d datasheet links", len(links))
        for href in links:
            if href in self.seen:
                continue
            self.seen.add(href)
            if self.saved >= self.count:
                return
            url = response.urljoin(href)
            file_key = self._url_to_filename(url)
            if self.resume and self._exists(file_key):
                continue
            # 可选启用 Playwright 处理 JS 动态页面
            meta = {"file_key": file_key}
            if self.use_playwright:
                meta["playwright"] = True
            yield scrapy.Request(url=url, callback=self.parse_datasheet, meta=meta)

        if self.saved >= self.count:
            return

        next_link = self._find_next_page_link(response)
        if next_link and next_link not in self.visited_pages:
            meta = {"playwright": True} if self.use_playwright else {}
            yield scrapy.Request(url=next_link, callback=self.parse_search, meta=meta)

    def parse_datasheet(self, response):
        # 详情页保存：按 URL 生成稳定文件名
        file_key = response.meta.get("file_key", "unknown")
        if self.resume and self._exists(file_key):
            return
        self.out_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.out_dir / f"{file_key}.html"
        out_path.write_bytes(response.body)
        self.saved += 1

    def _exists(self, file_key: str) -> bool:
        return (self.out_dir / f"{file_key}.html").exists()

    def _url_to_filename(self, url: str) -> str:
        safe = quote(url, safe="")
        return safe[:200]

    def _find_next_page_link(self, response) -> str | None:
        # 优先使用 “Next” 链接，否则从 Page= 参数中找更大的页码
        next_href = response.xpath("//a[contains(text(),'Next')]/@href").get()
        if next_href:
            return response.urljoin(next_href)
        page_links = response.xpath("//a[contains(@href,'Page=')]/@href").getall()
        if not page_links:
            return None
        current_page = 1
        match = re.search(r"Page=(\\d+)", response.url)
        if match:
            current_page = int(match.group(1))
        candidate = None
        candidate_page = current_page
        for href in page_links:
            match = re.search(r"Page=(\\d+)", href)
            if not match:
                continue
            page_num = int(match.group(1))
            if page_num > candidate_page:
                candidate_page = page_num
                candidate = href
        return response.urljoin(candidate) if candidate else None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrapy MatWeb crawler")
    parser.add_argument("--query", default="Polyethylene")
    parser.add_argument("--out-dir", default="data/html_pages")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--use-playwright", action="store_true")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--download-delay", type=float, default=0.5)
    parser.add_argument("--proxy-pool", default="")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = {
        "CONCURRENT_REQUESTS": args.concurrency,
        "DOWNLOAD_DELAY": args.download_delay,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2.0,
        "AUTOTHROTTLE_MAX_DELAY": 15.0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 4,
        "ROBOTSTXT_OBEY": True,
        "DOWNLOADER_MIDDLEWARES": {
            "middlewares.RandomUserAgentMiddleware": 400,
            "middlewares.ProxyPoolMiddleware": 450,
            "middlewares.Retry403Middleware": 550,
        },
        "PROXY_POOL": args.proxy_pool,
        "LOG_LEVEL": "INFO",
    }
    if args.use_playwright:
        settings.update(
            {
                "DOWNLOAD_HANDLERS": {
                    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                },
                "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
                "PLAYWRIGHT_BROWSER_TYPE": "chromium",
                "PLAYWRIGHT_LAUNCH_OPTIONS": {"headless": True},
            }
        )
    process = CrawlerProcess(settings=settings)
    process.crawl(
        MatwebSpider,
        query=args.query,
        out_dir=args.out_dir,
        count=args.count,
        resume=not args.no_resume,
        use_playwright=args.use_playwright,
    )
    process.start()


if __name__ == "__main__":
    main()

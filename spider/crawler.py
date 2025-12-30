import argparse
from pathlib import Path
from typing import Set
from urllib.parse import quote

import scrapy
from scrapy.crawler import CrawlerProcess


class MatwebSpider(scrapy.Spider):
    name = "matweb"
    custom_settings = {
        "CONCURRENT_REQUESTS": 2,
        "DOWNLOAD_DELAY": 2.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 2.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 3,
        "ROBOTSTXT_OBEY": True,
        "DOWNLOADER_MIDDLEWARES": {
            "middlewares.RandomUserAgentMiddleware": 400,
            "middlewares.Retry403Middleware": 550,
        },
        "LOG_LEVEL": "INFO",
    }

    def __init__(self, query: str, out_dir: str, count: int, resume: bool, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.query = query
        self.out_dir = Path(out_dir)
        self.count = count
        self.resume = resume
        self.seen: Set[str] = set()
        self.saved = 0

    def start_requests(self):
        url = f"https://www.matweb.com/search/QuickText.aspx?SearchText={quote(self.query)}"
        yield scrapy.Request(url=url, callback=self.parse_search)

    def parse_search(self, response):
        links = response.css("a[href*='DataSheet.aspx']::attr(href)").getall()
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
            yield scrapy.Request(url=url, callback=self.parse_datasheet, meta={"file_key": file_key})

    def parse_datasheet(self, response):
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrapy MatWeb crawler")
    parser.add_argument("--query", default="Polyethylene")
    parser.add_argument("--out-dir", default="data/html_pages")
    parser.add_argument("--count", type=int, default=50)
    parser.add_argument("--no-resume", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    process = CrawlerProcess()
    process.crawl(
        MatwebSpider,
        query=args.query,
        out_dir=args.out_dir,
        count=args.count,
        resume=not args.no_resume,
    )
    process.start()


if __name__ == "__main__":
    main()

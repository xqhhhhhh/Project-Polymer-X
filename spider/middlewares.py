import random

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
]


class RandomUserAgentMiddleware:
    def process_request(self, request, spider):
        request.headers.setdefault(b"User-Agent", random.choice(USER_AGENTS).encode("utf-8"))


class Retry403Middleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        if response.status in {403, 429}:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        return response

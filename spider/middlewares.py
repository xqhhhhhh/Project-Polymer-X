import os
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
        # 简单 UA 轮换，降低被封风险
        request.headers.setdefault(b"User-Agent", random.choice(USER_AGENTS).encode("utf-8"))


class Retry403Middleware(RetryMiddleware):
    def process_response(self, request, response, spider):
        if response.status in {403, 429}:
            # 遇到封禁/限流，清除当前代理并触发重试
            reason = response_status_message(response.status)
            if request.meta.get("proxy"):
                request.meta.pop("proxy", None)
            return self._retry(request, reason, spider) or response
        return response


class ProxyPoolMiddleware:
    def __init__(self, proxy_list):
        self.proxy_list = proxy_list

    @classmethod
    def from_crawler(cls, crawler):
        # 支持 settings 或环境变量注入代理池
        raw = crawler.settings.get("PROXY_POOL", "")
        if not raw:
            raw = os.environ.get("PROXY_POOL", "")
        proxy_list = [p.strip() for p in raw.split(",") if p.strip()]
        return cls(proxy_list)

    def process_request(self, request, spider):
        # 请求前随机选一个代理
        if not self.proxy_list:
            return
        request.meta["proxy"] = random.choice(self.proxy_list)

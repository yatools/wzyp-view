"""从公开页面及描述提取可追溯链接，不执行页面脚本。"""
import html
import re
from urllib.parse import urljoin, urlsplit

from bs4 import BeautifulSoup

from shop_http import normalize_url

_URL = re.compile(r'https?://[^\s<>"\x27`\\]+', re.I)
_ASSETS = re.compile(r"\.(?:png|jpe?g|gif|svg|webp|ico|css|js|woff2?|mp4|mp3|zip|pdf)$", re.I)


def extract_links(text, base_url):
    if not text:
        return []
    text = html.unescape(str(text))
    soup = BeautifulSoup(text, "html.parser")
    raw = [a.get("href", "") for a in soup.select("a[href]")]
    raw.extend(match.group(0).rstrip("。，；、）)]}") for match in _URL.finditer(text))
    links = set()
    for value in raw:
        if not value or value.startswith(("#", "mailto:", "tel:", "javascript:", "data:")):
            continue
        try:
            url = normalize_url(urljoin(base_url, value))
        except ValueError:
            continue
        if not _ASSETS.search(urlsplit(url).path):
            links.add(url)
    return sorted(links)

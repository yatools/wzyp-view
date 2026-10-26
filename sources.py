"""平台标识与显示信息；保持旧 wzyp 别名兼容。"""
from typing import Literal

DirectSource = Literal["wzyp", "wzyp-live", "16688", "dujiao-next", "dujiaoka", "catfk"]
Source = Literal["wzyp", "wzyp-live", "16688", "dujiao-next", "dujiaoka", "catfk", "yike"]
DIRECT_SOURCES = {"wzyp-live": "链动小铺", "16688": "16688 交易平台",
                  "dujiao-next": "Dujiao-Next", "dujiaoka": "独角数卡", "catfk": "云猫寄售"}
SOURCES = {**DIRECT_SOURCES, "yike": "一可比价网"}
INDEPENDENT = {"dujiao-next", "dujiaoka"}


def canonical_source(source):
    source = "wzyp-live" if source == "wzyp" else source
    if source not in SOURCES:
        raise ValueError("未知数据源")
    return source


def adapter_for(source):
    source = canonical_source(source)
    if source == "yike":
        raise ValueError("一可比价网使用关键词采集，不支持整店发现或同步")
    if source == "wzyp-live":
        import platformwzyp
        return platformwzyp
    if source == "16688":
        import platform16688
        return platform16688
    if source == "catfk":
        return catfk
    from platform_independent import dujiao_next, dujiaoka
    return dujiao_next if source == "dujiao-next" else dujiaoka


from hosted_shop import HostedShopPlatform
catfk = HostedShopPlatform("catfk", "catfk", "https://catfk.com", frozenset({"catfk.com"}), "云猫寄售")


def native_source(url):
    """Use each adapter's validated aliases, including legacy ldxp links."""
    for source in ("wzyp-live", "16688", "catfk"):
        try:
            adapter_for(source).normalize_target(url)
            return source
        except ValueError:
            continue
    return None

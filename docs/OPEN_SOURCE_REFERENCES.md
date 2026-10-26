# 开源设计参考与第三方资源

本次优化保留项目的 MIT 许可。业务实现按本项目的数据格式和兼容要求独立编写，以下项目用于核对公开协议、交互和架构设计。

| 项目 | 本项目采用的设计 |
|---|---|
| [AI Price Memory](https://github.com/BeterXie/ai_price_radar) | 多来源适配、原始规格与来源、完整快照发布，避免把不同 SKU 合并为一条报价 |
| [PriceBuddy](https://github.com/jez500/pricebuddy) | 商品对比与决策信息展示。本项目未加入其价格历史或通知能力 |
| [Scrapy](https://github.com/scrapy/scrapy/blob/master/docs/topics/jobs.rst) | 持久化队列、请求去重、独立任务恢复的设计思路；未引入 Scrapy 执行引擎 |
| [Dujiao-Next](https://github.com/dujiao-next/dujiao-next) | 从公开配置、商品路由与 SKU 响应契约编写读取适配器 |
| [经典独角数卡模板](https://github.com/iLay1678/dujiaoka_mod/tree/master/resources/views) | 核对 AmazeUI、LayUI 页面关键字段及 Choice 公开只读查询路径 |
| [经典 2.x 历史模板](https://github.com/hiouttime/dujiaoka/tree/a81dcbd782f096354734cfd822fd6896f38498c3/resources/views) | 核对 Hyper、Luna 商品详情字段，以及缺货时不公开购买链接的目录条目 |
| [一可比价网](https://vs.yiketools.com/) | 核对公开前端正常匿名会话使用的关键词列表、同源组分页和商品详情读取契约；作为独立聚合快照来源，保留原商品链接和来源更新时间 |

## 运行依赖与静态资源

- Beautiful Soup 4.15.0：HTML 解析，Python 包安装时包含其许可。
- [DOMPurify 3.4.17](https://github.com/cure53/DOMPurify)：本地静态资源 `static/vendor/purify.min.js`，保留原版权声明；完整上游许可随附在同目录的 `DOMPurify-LICENSE.txt`。
- 原有 marked 静态资源继续保留；其 HTML 输出先由 DOMPurify 清理再显示。

原站发现流程中的公开目录与第三方索引仅用于发现链接，商品字段读取原始货铺公开接口或页面。一可比价网通过独立 `yike` 来源保存其公开聚合报价，标明为关键词采集子集，不覆盖原站快照；来源更新时间和本地采集时间分别保存。不提供同款自动判定、汇率换算、价格历史、定时采集或通知。

## 集中平台自动恢复参考

- [curl_cffi](https://github.com/lexiforest/curl_cffi)（MIT）：使用浏览器 TLS/HTTP 协议兼容能力和内存会话；不会执行 JavaScript，不能保证通过验证。
- [Playwright](https://github.com/microsoft/playwright-python)（Apache-2.0）：使用独立浏览器线程、匿名上下文、请求拦截和同页面公开查询；服务停止或取消时释放上下文，不保存验证凭据。
- [AI Price Memory 链动采集器](https://github.com/BeterXie/ai_price_radar/tree/main/crawler/ldxp)（MIT）：参考真实页面会话与公开商品分页的思路。本项目独立实现自动处理预算和请求边界；不采用其人工验证等待流程。
- [FlareSolverr](https://github.com/FlareSolverr/FlareSolverr)：已调研，主要面向 Cloudflare/DDoS-GUARD，本轮未作为运行依赖。
- [阿里云 ESA 状态码说明](https://help.aliyun.com/zh/edge-security-acceleration/esa/support/http-status-code-description)：520 为源站异常响应，不直接判断为确定风控。
- [云猫公开店铺](https://catfk.com/shop/work)：核实店铺信息、分类、四类商品列表及详情契约，使用独立来源 `catfk`；不访问支付、订单或商家后台。

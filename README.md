# wzyp-view — 多平台货铺发现、采集与商品比价

面向个人本地使用的公开货铺采集、统一搜索与 LangGraph AI 分析工具。支持 [链动小铺](https://wzyp.cn)、[16688](https://www.16688.com.cn)、[云猫寄售](https://catfk.com)、Dujiao-Next、传统独角数卡的原站采集，以及[一可比价网](https://vs.yiketools.com/)的关键词聚合快照；各类来源独立保存 SQLite 数据。

## 两阶段店铺发现与自动商品采集

新建的跨平台和单平台发现任务先搜索店铺，再自动采集商品：

1. **搜索店铺目录**：读取店铺首页、公开分类和首屏商品列表，保存店铺名称、商品名称与链接、分类、原站公布的数量及库存状态。不会为了补齐预览读取商品详情或后续商品分页；商品入口仅在定位店铺时读取必要的归属接口。每店最多保存 100 条预览，明确标注目录是否完整，未公开的数量保留为空。
2. **自动采集整店商品**：本轮索引、目录核验及可执行的店铺搜索全部结束后，才对保留的店铺读取完整商品。库存未知的店铺先保留，第二阶段再核实；完整目录已确认空店或全部缺货时跳过。商品数量不代表库存数量。

两个阶段分别保存断点。取消第一阶段不会启动商品采集；恢复第二阶段时不重复搜索已保存的店铺。部分入口失败或达到目录／核验预算，不阻止已保留店铺进入第二阶段；失败入口和预算断点仍保留。第二阶段商品说明里发现的新链接留待下一轮搜索。仅完整采集成功才替换正式快照，目录预览不会计入总览、搜索或 AI 的正式商品数量。

任务中心和各平台卡片显示当前阶段、发现店铺数、待采数及库存待核实数，可点「查看店铺目录」查看预览。单店同步、刷新已收录店铺仍直接采集；一可比价网仍按关键词采集。升级前的任务保持原流程恢复，新任务使用 `workflow_version: 2`。

新增只读接口：`GET /api/discovery/shops?job_id=任务编号&source=catfk&page=1&pageSize=20`（`source` 可省略，`pageSize` 最大 100），以及用于任务中心轻量计数的 `GET /api/sources/summary`。任务的 `phase` 分别为 `shop_discovery`、`goods_collection`；`discovered_shops`、`pending_shops`、`unverified_stock_shops` 与正式的 `success_shops`、`collected_goods` 分开统计。

任务运行时每 3 秒刷新，空闲时每 15 秒刷新，页面隐藏或离开任务页后停止该页轮询。首页与发送 AI 消息不再额外调用 LLM 测试，连接状态由显式测试和实际对话更新。

发现历史分别显示已保存的店铺目录、待采店铺和正式商品结果，并直接显示失败原因。大量入口导入分批提交；本机各线程协调 SQLite 写入，短暂的数据库忙会自动重试。工作线程仍在运行或退出时，不会因为心跳延迟而误判中断或重复续跑，已保存的目录和断点保持可恢复。

离线回归：`python -m unittest discover -s tests -v`。浏览器回归：先运行 `python tests/browser_server.py`，再运行 `python tests/browser_checks.py`；只使用合成数据，不访问真实采集库或 LLM。

## 云猫接入与跨卡网并发

新增 `catfk`（云猫寄售），数据库默认 `data/catfk.sqlite3`，可用 `WZYP_CATFK_DB` 指定。支持 `/shop/{token}`、`/item/{goods_key}`，读取公开店铺、四类商品分页、分类与详情；成功完整读取后才替换整店快照。云猫与链动共用不可变的协议适配器，两边的地址、匿名会话和商品身份独立，相同商品编号不会覆盖。

云猫「发现采集」使用已有候选链接及公共索引；也支持手动链接、刷新、单店同步、取消、续采、统一搜索、对比和 AI 查询。无入口或索引失效时如实报告，不枚举店铺编号，不声称覆盖全站。天瑞、新发卡等其他集中平台本轮未接入。

跨平台发现默认同时运行 **4 个平台子任务**，可在「扩展范围」中选择 1–8。两个独立线程核验未知站点；索引并行读取，手动链接和已收录入口无需等待索引完成。各平台按轮转方式获得空位；已有平台任务占用时仅该平台等待，取消发现任务只取消它创建的子任务。旧任务续采保留原来的来源范围，新任务默认包含云猫。

普通请求最多尝试三次，遵循 `Retry-After`（最多 60 秒）。全局最多每秒 4 次请求，同源站最多每秒 1 次，链动及其域名别名共用每两秒 1 次的额度；退避只影响对应源站，不暂停其他卡网。

登记的集中平台在出现兼容性或验证页面特征时，可自动升级到 `curl_cffi`，再尝试隔离的 Playwright 浏览器会话。普通 HTTP 最多三次、兼容请求一次、浏览器恢复一次；首次浏览器恢复最多 45 秒，后续同会话读取最多 25 秒，全进程最多两个匿名上下文。仅开放已核实的商品查询路径，固定公网地址，屏蔽其他域名、内网、写入接口、WebSocket、下载和 Service Worker，不保存浏览器登录态或验证凭据。

需要人工验证或恢复失败时，本轮跳过该源站，保留旧快照与断点，其他平台继续。520 表示源站返回异常响应，不能仅凭状态码认定为风控；ESA/Cloudflare 类型、状态码、公开请求编号、处理阶段和尝试次数以 `failure` 返回并存入任务历史，不含 Cookie 或令牌。源站不可用或明确拒绝时，自动恢复不保证成功。

接口扩展：

- `GET /api/sources` 返回云猫及来源能力，前端统一从这里生成来源选项。
- `/api/sources/catfk/crawl*`、`/sync`、`/jobs` 复用其他原站来源的接口；商品详情为 `/api/catalog/goods/catfk/{goods_key}`。
- `POST /api/discovery/jobs` 新增 `parallel_sources`，默认 4、范围 1–8；状态新增 `source_progress`，分别展示运行、等待、跳过和失败原因。候选的 `ready` 表示等待采集，`blocked` 表示本轮跳过，续采可以重试。
- 数据库仅增补字段和索引，保留既有来源路径、快照、任务编号及旧 `wzyp` 别名。

Windows 首次安装或更新运行 `powershell -ExecutionPolicy Bypass -File .\setup.ps1`，自动补齐依赖与 Chromium，再运行 `start.bat`。Linux 使用 `python -m playwright install --with-deps chromium`；Docker 镜像构建自动安装。浏览器组件暂不可用时仍能浏览本地数据，无法恢复的源站按上述规则跳过。

## 一可比价网：按关键词采集

在「采集任务」的一可比价网卡片输入一个关键词，点击「按关键词采集」。系统通过正常匿名会话读取公开列表，完整展开同源组分页，逐条读取各店商品详情后保存实际报价；同源组的最低价和价格区间不作为单店商品入库。

- 一可数据仅代表手动关键词采集的商品子集，不代表全站或整店完整数据。搜索、详情、对比清单、单平台查询、总览及 AI 分析均标注其聚合来源；搜索只读取本地快照。
- 默认混合搜索优先使用当前选中原站来源中已有的同商品记录。一可记录独立保留，仅选择「一可比价网」即可查看；仅按已核实的原站商品标识及相同规格匹配，不按标题猜测同款，不混合拼接两份报价的字段。
- 保存原商品、原店铺及一可详情三种链接，分别展示来源更新时间原文和本地采集时间。未知价格、库存数量保持为空，不从「卡密」推断自动发货。
- 支持进度、失败原因、取消、重启恢复、断点续采和历史。恢复时从第一页重新核验关键词结果，按 GUID 去重；同任务内已成功且列表信息未变化的商品会跳过。分页异常报告部分完成或失败，明确拒绝访问时中断。
- 每条完整商品及详情用事务增量更新，不调用原站整店替换逻辑；新关键词、空结果和读取失败不会删除已采集商品。来源明确下架的商品不参与当前报价列表，保留详情以便对比清单提示下架。

新增来源标识为 `yike`，默认数据库 `data/yike.sqlite3`，可用 `WZYP_YIKE_DB` 指定路径。匿名 Cookie 和页面查询令牌仅在任务内存会话中使用，不保存至配置或任务历史。一可不加入原站来源的整店发现范围。

| 接口 | 用途 |
|---|---|
| `POST /api/sources/yike/crawl` | `{"keyword":"GPT Plus"}`；去除首尾空白后须为 1–200 字符，同一数据库只运行一个任务 |
| `GET /api/sources/yike/crawl`、`GET /api/sources/yike/crawl/{id}` | 最近任务及指定任务状态 |
| `POST /api/sources/yike/crawl/{id}/cancel`、`resume` | 取消或续采 |
| `GET /api/sources/yike/jobs` | 采集历史及关键词、分页、成功／失败统计 |
| `POST /api/catalog/search`、`GET /api/catalog/goods/yike/{guid}` | 搜索支持 `sources:["yike"]`；详情保留来源追溯 |

`GET /api/sources` 增加来源种类、是否支持整店发现和是否支持关键词采集的信息。目录返回新增 `source_url`、`source_updated_at`、`origin_source`、`origin_goods_key`；现有来源无需提供这些字段。

## 采集任务中心与统一商品目录

首页默认进入 **采集任务中心**，原有平台采集、总览、店铺、单平台商品查询、AI 分析和设置均保留。

1. 在「添加发现入口」混合粘贴店铺、商品或公开目录页链接，每行一个；也可使用已有公开索引、种子和已收录入口。
2. 选择允许收录的平台，点击「发现并采集」。系统识别平台、核验货铺，再完整采集该店的公开商品。公开索引只提供链接，不导入第三方价格或库存。
3. 「链接与来源追溯」显示每个候选的来源页面、深度、核验状态和具体失败原因。发现、核验与整店采集分别计数，发现链接不等于已收录货铺。
4. 默认跨站扩展两层，最多核验 200 个不同的未知源站、读取 200 个普通目录页；两种预算分别保存。达到核验额度后，已识别货铺仍继续完整采集。续采复用源站判断；「继续未完成任务」不增加额度，「追加预算」可分别增加 0–2000 个核验源站或目录页。
5. 「平台采集」优先核验已有店铺目录，索引在后台读取；本轮搜索结束后再采集商品，索引失败不会阻止已保留店铺进入第二阶段。跨平台默认同时采集 4 个平台，各平台独立排队、共用请求限速。取消只停止自己创建的子任务，工作线程退出后才能续采。
6. 续采默认只重试网络、限流、服务和快照变化等暂时故障；明确不存在、封禁、打烊、无效链接和不支持模板默认跳过。「重试全部失败」可主动重试业务失败。候选的「仅重试此入口」会创建独立目标任务并保留原失败历史，平台忙时提示等待；未知入口只预填重新发现表单。
7. 打开「商品搜索」统一查询全部已支持来源，支持商品名全部关键词交集、货铺、平台、价格、币种、库存和发货方式筛选；默认人民币、价格升序。未知库存不按缺货处理，未知价格不填零。
8. 加入最多八件商品到「对比清单」，逐项查看标题、规格、价格、库存、原始说明、来源和同步时间。清单保存在浏览器本地，刷新时核对当前快照；不自动认定同款，不对不同币种换汇。

本轮**不包含价格历史、定时采集或通知提醒**。采集结果为最近成功读取的公开快照；目录页或描述中的链接由公开 HTTPS 请求核验，不访问登录后数据、不提交订单；仅登记的集中平台可在隔离浏览器中自动执行公开页面脚本。

### 新增来源

- **Dujiao-Next**：读取 `/api/v1/public/config`、`categories`、分页 `products` 和按 slug 的详情。每个有效公开 SKU 分别保存，保留规格与原币种，会员价、批发条件价不冒充普通标价；隐藏库存不推算精确数量。
- **传统独角数卡**：支持可核验的 AmazeUI、LayUI、2.x Hyper/Luna、保留独角数卡署名及商品编号的旧 Bootstrap 变体，以及 Choice 公开商品查询契约。Choice 只调用已核实的只读查询接口，跳过密码保护的分类和商品；匿名会话按源站隔离，用后清理。原站目录中没有商品直达链接的缺货卡片标明为目录条目，不猜测购买编号。自定义或不完整模板显示原因并保留旧快照。
- 独立货铺以源站和商品／SKU 共同确定身份，不同站点的相同编号不会覆盖。完整快照成功后才替换该店数据；失败、缺页或解析异常均保留旧数据。
- 独立站仅接受公网 HTTPS 443，逐跳校验目标、固定已验证 DNS 地址，限制响应大小和耗时。普通表单的验证码插件不会被误判为整页验证；实际访问阻断会停止相应源站。
- 同一扩展深度优先核验尚未访问的域名，避免一个未知模板的多页链接耗尽目录预算。独立站默认采集时限为 30 分钟；Dujiao-Next 的公开商品总数核验成功后，按目录大小分配时间，最多 6 小时。单次响应 4 MiB、单站 128 MiB、请求限速和有限重试继续生效；超出预算保留旧快照与任务断点。

新增存储默认位于 `data/dujiao-next.sqlite3`、`data/dujiaoka.sqlite3`、`data/discovery.sqlite3`，可分别通过 `WZYP_DUJIAO_NEXT_DB`、`WZYP_DUJIAOKA_DB`、`WZYP_DISCOVERY_DB` 指定。两个原有库、商品标识、`wzyp` 别名、聊天命名空间及旧 API 默认行为保持兼容。独立货铺的旧版总览／店铺价格统计只统计人民币商品，其余币种通过统一商品目录查看。

新增接口：

| 接口 | 用途 |
|---|---|
| `POST /api/discovery/jobs` | `urls`、`sources`、`include_indexes`、`max_depth`、`page_limit`、`probe_limit`、`parallel_sources` 创建发现任务 |
| `GET /api/discovery/jobs`、`GET /api/discovery/jobs/{id}` | 历史与状态 |
| `GET /api/discovery/jobs/{id}/candidates` | `page`、`pageSize`、`state` 分页查看候选及父链接 |
| `POST /api/discovery/jobs/{id}/cancel`、`resume` | 取消或续跑；续跑可传 `extra_pages:0`、`extra_probes:0`、`max_depth`、`retry_mode:"transient"`（或 `"all"`） |
| `POST /api/discovery/jobs/{id}/candidates/{candidate_id}/retry` | 已识别平台创建新 `targets` 任务（忙时 409）；未知入口返回 `rediscover` 预填内容；原失败记录保持不变 |
| `POST /api/catalog/search` | 统一筛选、全局排序与分页；`currency:null` 按币种分别排序 |
| `GET /api/catalog/goods/{source}/{goods_key}` | 按来源读取当前商品详情，不存在时返回 404 |
| `/api/sources/{source}/crawl*` | 六类来源的采集、状态、取消和续跑；直接来源支持整店，一可按关键词；续跑支持 `retry_mode`，省略请求体兼容旧行为入口 |
| `POST /api/sources/{source}/sync`、`GET /api/sources/{source}/jobs` | 单店同步与平台历史 |

后端新模块为平台适配、HTTPS 传输、发现协调、只读目录与路由；前端拆分为原有功能脚本、原生模块和独立样式，无需 Node 构建。Markdown 使用本地 DOMPurify 清理，商品说明继续放在受限 iframe 中。设计参考及第三方资源许可见 [开源借鉴说明](docs/OPEN_SOURCE_REFERENCES.md)。

离线回归：`python -m unittest discover -s tests -v`。浏览器隔离回归：运行 `python tests/browser_server.py` 后访问 `http://127.0.0.1:18871`，检查 Markdown 清理、清单上限／恢复、下架状态、关键词、乱序请求、重置和 iframe 隔离；此服务只使用合成数据，不读取真实采集库或 LLM。

> 🎨 **[在线宣传页](https://samge0.github.io/wzyp-view/)** — 功能总览 · AI 架构 · 快速开始

- 📊 可视化总览：统计卡 / 商品类型分布 / 价格区间 / 热词云 / 销量 Top10，**全部支持点击下钻**到对应筛选结果
- 🏪 店铺 / 🛒 商品多维查询：字段筛选构建器（≥/≤/like…）、表头+下拉双排序、抓取状态筛选、排除 0 元、商品详情抽屉
- 🔎 商品多关键词：输入 `gpt`、`plus` 分别按 Enter 或点「添加关键词」，只显示商品名同时包含全部词的商品；可继续加词或删除标签，每行保留商品名、店铺名和价格，默认价格降序，也可切换升序
- 💬 AI 分析：LangGraph Agent 流式对话（markdown 渲染、自动追问建议、多会话管理），支持「AI 解读」把当前查询结果一键带入聊天
- ⚙️ 页面化配置：LLM 接口可在网页设置页配置、测试连接和导入/导出，保存后热生效；数据存储自动管理
- 🔗 **链动小铺采集**：从历史公开快照的 250 家店铺入口及公开链接发现店铺，逐类读取原站商品与详情；历史商品和价格不会导入采集库
- 🔗 **16688 平台采集**：自动发现公开店铺与商品链接，按原站店铺编号去重后批量采集；支持进度查看、取消、重启恢复与断点续跑，采集结果统一参与总览、筛选、详情和 AI 分析。

## 采集 16688 平台公开商品

两个平台均无需安装数据库服务或导入 SQL 快照。按下方步骤安装 Python 依赖、运行 `python run.py`，打开 **http://127.0.0.1:8770** 即可。

1. 顶栏选择 **16688 交易平台**（新浏览器默认选中）。
2. 点击 **搜索店铺并自动采集**。程序读取公开索引的全部商品分页，从中提取 16688 店铺／商品链接，再向 16688 原站采集各店的公开商品；商品描述中的 16688 链接也会加入发现队列。
3. 面板每两秒更新发现链接、核验店铺、成功／失败数量、商品数和待处理数量。**取消采集**会停止后续请求并保留已完成快照；**继续采集**跳过已完成店铺，重试失败或未完成的部分。
4. 以后点击 **刷新已收录店铺**，可更新已经收录的店铺，无需重新读取公开索引。
5. 在总览查看统计、在店铺与商品页搜索筛选、点击商品名称查看详情。配置 LLM 后可使用 AI 分析。

展开 **补充链接 / 单店同步** 可每行添加一个 16688 链接，加入下一次批量任务，也可仅同步明确指定的一个店铺。默认不绑定任何特定商品或店铺。

也可以从命令行采集（16688 与链动小铺 CLI 参数一致）：

```bash
# 16688 交易平台：自动发现并批量采集公开商品
python platform16688.py crawl
python platform16688.py refresh
python platform16688.py crawl --resume JOB_ID
python platform16688.py crawl --url https://www.16688.com.cn/shop/XIAOQING
python platform16688.py https://www.16688.com.cn/goods/G74884541

# 链动小铺（保存至 data/wzyp.sqlite3，source=wzyp-live）：
python platformwzyp.py crawl
python platformwzyp.py refresh
python platformwzyp.py crawl --resume JOB_ID
python platformwzyp.py crawl --url https://wzyp.cn/shop/9B5624X
python platformwzyp.py https://wzyp.cn/shop/9B5624X
```

CLI 每次进度变化会输出任务编号及统计；`Ctrl+C` 取消当前 CLI 创建的任务。网页和 CLI 共用同一数据库时，只允许一个平台任务运行，重复启动会返回已有任务。服务重启后，未完成任务会标记为中断，点击继续或使用 `--resume` 恢复；不会自动定时执行。

每个平台默认两个店铺工作线程，请求共同遵守全局与源站限速。超时、连接失败、HTTP 429 和临时服务器错误每个请求最多尝试三次，并遵循 `Retry-After`（最长等待 60 秒）。失效店铺、商品和读取失败会记录具体入口，不会阻断其他店铺。

数据及任务检查点默认保存在 `data/16688.sqlite3`，可通过 `WZYP_16688_DB` 指定文件。旧版 SQLite 会自动增加任务表，已有快照保持不变。每个店铺完整读取成功后才更新该店快照，并移除已不在公开列表中的商品；其他店铺保持不变。网络失败、平台返回异常或不完整列表时保留之前的数据。Docker 的 `./data` 挂载同时保存快照与采集任务。

**发现来源与覆盖范围**：首个发现源为 [PriceMap AI 公开索引](https://pricemapai.com/data/latest.json)，使用版本化清单校验全部商品分页；版本变化或分页失效时重新读取一次，仍不完整则保留旧数据并报告。第三方索引仅用于发现链接，价格、库存、描述和分类均从 16688 原站采集。只跟随已公开的 16688 商品／店铺链接，不枚举编号、不使用商家登录态。公开索引有收录范围，结果应称为“已发现公开店铺的商品快照”，不代表全站或全部类目完整数据。

**数据口径**：价格为公开标价，未含平台手续费；未公开的市场价、实付价和开店日期保留为空。“1.3万+付款”以原文保存，排名与汇总使用 13,000 作为公开下限，不代表精确销量。`auto` / `manual` / `unknown` 分别表示自动发货、手动发货和未公开发货方式；手动发货返回的库存 `-1` 按未公开数量处理。平台商品分类单独保存。HTML 描述在受限 iframe 中显示。

## 采集链动小铺公开商品

顶栏只有一个 **链动小铺** 入口，内部标识沿用 `wzyp-live`。点击 **搜索店铺并自动采集**，程序从原项目公开历史快照中提取店铺入口，再从原站逐类读取列表、价格和详情。仓库附带 250 家店铺的种子链接，在线发现不可用时会使用这份清单。仅成功读取的店铺会写入 `data/wzyp.sqlite3`；历史备份不用于填充商品、价格、销量或详情。

链动小铺同样支持补充链接、单店同步、刷新、取消和断点续跑。遇到 403、人机验证或连续的明确阻断响应时，会中断任务并保留断点；普通单店失效或读取失败会记录原因，其他店铺继续处理。已入库快照在失败时保留，成功刷新后整店替换。

旧浏览器保存的 `wzyp` 选择会自动迁移到此入口。旧 API 的 `source=wzyp` 及未指定数据源的请求也读取同一采集库。现有 `wzyp-live` 聊天命名空间、数据库路径和任务检查点沿用原值，16688 仍独立保存。

原 MySQL 查询和配置功能已停用，`GET/POST /api/db/settings` 和 `GET /api/db/test` 返回 HTTP 410。配置导入仅读取 LLM 设置，旧 JSON 中的 `db` 段被忽略。原有文件不需要删除。

![16688 平台采集进度与数据总览](docs/screenshot-16688.png)

新增接口：

| 接口 | 说明 |
|------|------|
| `GET /api/sources` | 可用数据源 |
| `POST /api/sources/16688/crawl` | `{"mode":"discover","urls":[]}`；mode 为 discover（默认）或 refresh |
| `GET /api/sources/16688/crawl` | 最近的任务，优先返回运行中的任务：`{"job":任务或null}` |
| `GET /api/sources/16688/crawl/{job_id}` | 指定任务的状态、进度与最近失败记录 |
| `POST /api/sources/16688/crawl/{job_id}/cancel` | 取消任务，返回任务编号及状态 |
| `POST /api/sources/16688/crawl/{job_id}/resume` | 续跑失败、部分完成、中断或取消的任务 |
| `POST /api/sources/16688/sync` | 请求体 `{"url":"https://www.16688.com.cn/shop/XIAOQING"}` |
| `POST /api/sources/wzyp/crawl`、`GET /api/sources/wzyp/crawl*`、`POST /api/sources/wzyp/sync` | 链动小铺批量采集、状态、取消、续跑与单店同步 |
| `/api/stats`、`/api/shops`、`/api/goods`、`/api/goods/{goods_key}` | 默认读取链动小铺；`source=wzyp` 与 `source=wzyp-live` 读取同一采集库 |
| `/api/stats?source=16688` | 本地快照统计 |
| `/api/shops?source=16688`、`/api/goods?source=16688` | 复用原查询请求体 |
| `/api/goods/{goods_key}?source=16688` | 商品详情 |
| `/api/chat?source=16688` | 当前数据源的 AI 分析 |
| `GET /api/health` | 服务健康检查，不依赖外部数据库或 LLM |

`POST /api/goods` 支持 `keywords` 数组，例如 `{"keywords":["gpt","plus"],"sort":"price","order":"desc"}`。关键词只匹配商品名，英文字母忽略大小写，`%`、`_` 按普通字符处理；可与店铺、类型、价格和高级筛选组合使用。旧 `kw` 参数保持原有行为，与 `keywords` 同时传入时共同生效。「AI 解读」使用与列表相同的查询条件，包括多关键词和排除 0 元。

任务响应包含 `job_id`、`mode`、`status`、`phase`、`discovered_links`、`resolved_shops`、`success_shops`、`failure_count`、`collected_goods`、`pending_links`、`updated_at` 和失败记录。状态包括 `running`、`cancelling`、`cancelled`、`interrupted`、`completed`、`partial`、`failed`。任务表与队列表不在 AI 查询权限内。

离线回归测试（使用本地 HTTP 模拟服务及 curl，无需实际商店网络或 LLM 服务）：

```bash
python -m unittest discover -s tests -v
```

## 界面预览

以下截图包含旧版界面和历史示例数据；当前版本以实际采集结果为准。

**📊 数据总览** — 查看已采集数据，所有卡片与图表可点击下钻：

![数据总览](docs/screenshot-dashboard.png)

**💬 AI 分析** — 自然语言查询，流式 markdown 回复 + 自动追问建议（回答引用真实库内链接）：

![AI 分析](docs/screenshot-chat.png)

**🛒 商品查询 + 详情抽屉** — 多维筛选，点击商品名查看 HTML 描述全文：

![商品查询](docs/screenshot-goods.png)

**🏪 店铺列表** — 关键词搜索 + 字段筛选构建器 + 双排序，含抓取状态徽章：

![店铺列表](docs/screenshot-shops.png)

**⚙️ 设置页** — 当前版本只需配置 LLM，支持连通测试和配置导入/导出（下图为旧版示意）：

![设置页](docs/screenshot-settings.png)

## 快速开始

### Windows 一键启停

安装下方 Python 依赖后，双击项目内的 `start.bat` 启动服务并打开浏览器，双击 `stop.bat` 停止服务。当前工作区上一级目录也提供了同名入口。服务在后台运行，关闭启动窗口或网页不会停止服务。

脚本优先使用项目的 `.venv\Scripts\python.exe`，没有虚拟环境时尝试系统 Python（≥ 3.11）。重复启动会提示已有服务；端口被其他程序占用时会报错。停止前核对记录的 PID、进程创建时间与可执行文件，只结束脚本管理的进程树。

在项目目录的 PowerShell 中也可执行（无需激活虚拟环境）：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\service.ps1 start
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\service.ps1 stop
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\service.ps1 restart
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\service.ps1 status

# 更换端口；监听地址可通过 WZYP_HOST 设置
$env:WZYP_PORT = '9000'
.\start.bat
```

默认访问 `http://127.0.0.1:8770`，确认 `/api/health` 正常后才报告启动成功。启动等待默认 30 秒，可通过 `start.bat -StartupTimeout 60` 延长。`status` 在服务正常时返回 0，未运行、健康检查失败或操作失败时返回 1。

运行记录保存在 `data/service.json`；日志保存在 `data/service.stdout.log` 和 `data/service.stderr.log`，每次启动覆盖上次日志。`stop` 会结束服务进程树，正在采集时建议先在网页取消并等待任务停止；未完成任务可在下次启动后继续。停止和重启不会删除数据库、配置或采集检查点。

### 方式一：本地运行（uv）

#### 1. 创建虚拟环境（[uv](https://docs.astral.sh/uv/)）

```bash
cd wzyp-view

# 创建虚拟环境（默认 .venv，Python ≥ 3.11）
uv venv --python 3.13

# 安装依赖（国内可加 --index-url https://pypi.tuna.tsinghua.edu.cn/simple）
uv pip install -r requirements.txt
```

#### 2. 启动

```bash
# 激活虚拟环境
#   Windows (cmd):
.venv\Scripts\activate
#   Windows (PowerShell):
.venv\Scripts\Activate.ps1
#   Linux / macOS:
source .venv/bin/activate

# 启动服务
# 首次安装或更新依赖后，安装浏览器组件
python -m playwright install chromium
python run.py
```

> 不激活环境也可以直接跑：`.venv\Scripts\python.exe run.py`（Windows）或 `.venv/bin/python run.py`（Linux/macOS）

#### 3. 采集并使用

浏览器打开 **http://127.0.0.1:8770**：

1. 选择 **链动小铺** 或 **16688 交易平台**，启动采集或继续已有任务；已有成功快照可直接浏览。
2. 如需 AI 分析，进入 **⚙️ 设置**，填入 OpenAI 兼容接口的 Base URL、Model、API Key，测试并保存。

配置持久化在 `data/settings.json`，重启不丢。也可复制 `data/settings.example.json` 手动编辑，或使用环境变量（见下）。

### 方式二：Docker

```bash
git clone https://github.com/Samge0/wzyp-view.git
cd wzyp-view
docker compose up -d --build
# 访问 http://localhost:8770
```

- 配置同样走页面设置（持久化在 `./data` volume）
- 容器内访问宿主机 LLM 服务用 `http://host.docker.internal:16869/v1`
- 也可用环境变量注入配置（优先级高于页面）：

```yaml
environment:
  WZYP_LLM_BASEURL: http://host.docker.internal:16869/v1
  WZYP_LLM_MODEL: qwen38
  WZYP_LLM_APIKEY: ""
```

## 数据说明

| 表 | 说明 |
|----|------|
| `wzyp_shops` | 完整采集成功的公开店铺、平台统计、同步时间与抓取状态 |
| `wzyp_goods` | 对应店铺公开商品列表、价格、类型和真实链接 |
| `wzyp_goods_detail` | 商品详情、HTML 描述、图片及公开价格字段 |

行数随采集任务变化，以页面统计和查询结果为准；已发现链接不等于已成功采集的店铺。两个平台分别保存到 `data/wzyp.sqlite3` 和 `data/16688.sqlite3`，可通过 `WZYP_LIVE_DB` 和 `WZYP_16688_DB` 指定路径。链动小铺仅将旧快照作为发现入口；价格和销量是各店最近成功采集时的快照，不保证随原站实时更新。

## AI 架构（LangGraph）

```
planner(LLM+bind_tools) ⇄ tools(只读SQL+schema) → END
```

**防幻觉设计**（本地小模型实测打磨）：

- schema 动态行数 + few-shot 示例 SQL + 「仅有 3 张表」显式声明，防表名幻觉
- SQL 报错自动回灌纠正提示（Unknown table/column → 引导核对 schema 重写）
- 工具结果渲染为竖线表格「直喂」，防 JSON 转录失真
- 系统提示每轮重注入 + data_schema 后强提示「无数据行」
- 追问轮防复读提示

**记忆体系**：JSON 长期记忆（跨请求会话持久化，`data/chat_memory.sqlite.json`）+ 24 条滑动窗口 + 历史摘要压缩。

## 项目结构

```
wzyp-view/
├── server.py          # FastAPI（静态 + 数据API + SSE + 设置/导入导出API）
├── agent.py           # LangGraph Agent（工具/记忆/流式/追问建议）
├── db.py              # 两个平台的 SQLite 只读查询及旧 source 参数兼容
├── local_store.py     # SQLite 快照与只读查询
├── platformwzyp.py    # 链动小铺种子发现、公开接口采集与 CLI
├── platform16688.py   # 16688 公开接口采集 / 命令行同步
├── crawler16688.py    # 公开索引发现、后台批量采集与 CLI
├── crawl_store.py     # 持久化任务、队列、店铺别名与检查点
├── public_http.py     # 共享限速、有限重试、可中断请求
├── tests/             # 单店/平台采集、恢复、迁移、API、AI 隔离测试
├── config.py          # LLM 配置加载（settings.json + env）
├── static/index.html  # 前端 SPA（原生 JS 单文件）
├── data/              # SQLite 数据库、配置与聊天记忆（运行时生成，已 gitignore）
│   ├── wzyp_seeds.json # 链动小铺公开店铺入口
│   └── settings.example.json
├── requirements.txt
└── Dockerfile / docker-compose.yml / .dockerignore
```

## 常见问题

- **端口占用**：`WZYP_PORT=9000 python run.py` 换端口
- **采集全部连接失败**：确认运行服务的进程具备访问源站的网络权限；受限运行环境或错误代理可能使整批请求失败。先通过单店同步验证连通，再继续批量任务
- **LLM 401**：vLLM 本地服务也要填 API Key；Ollama 留空即可
- **容器内 LLM 连接失败**：Base URL 用 `http://host.docker.internal:<port>/v1` 而非 `127.0.0.1`

## License

MIT

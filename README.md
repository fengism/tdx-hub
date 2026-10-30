# tdxhub

[![Python Version](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/tdxhub-sdk.svg)](https://pypi.org/project/tdxhub-sdk/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

`tdxhub` 是面向 Python 3.11+ 的通达信市场数据工具包。它将本地 VIPDOC 文件、标准/扩展在线行情、官方配置与财务文件、复权与换手率、增量拉取和可选 HTTP 服务统一到一套现代化 Python API 中。

> **提示**：本项目专注于金融市场数据的高效读取、协议解析与本地存储，不包含任何交易与下单功能。

---

## 0.4.0 版本说明

当前源码版本为 **0.4.0**，主要更新：

- 将 TDX 协议实现内置到 `tdxhub.tdx`，移除外部 tdxpy/Cython 依赖（0.3.0 起）。
- 内置 round2 新协议客户端（握手/心跳/K 线/实时快照/服务端复权），主站停用旧 K 线与旧盘口命令后自动切换；K 线默认优先新协议，可用 `TDXHUB_KLINE_PROTOCOL=legacy` 恢复旧行为。
- 在线行情全链路支持 HTTP CONNECT / SOCKS5 代理（构造参数 `proxy=` 或 `TDXHUB_PROXY` 环境变量）。
- 主站优选名单按实测可用性收敛（标准行情 43 台 / 扩展行情 18 台），淘汰不可达与误判节点。
- 加强按市场/接口的主站能力探测、请求超时预算与心跳故障隔离，支持快照分批、去重与缺失诊断。
- 加强 K 线/分笔分页完整性检查，资金流累计窗口与板块汇总不再静默返回残缺结果。
- 修正财务摘要字段与单位、指数盘口语义、基金复权和历史股本换算，提供明确的数据质量状态。
- 加强财务文件下载长度、MD5 与路径校验，完善协议回归和仓库外 wheel 安装验证。

现有高层 API 入口保持不变，但财务字段口径和不完整数据的错误行为有变化，升级前请阅读
[数据口径与准确性边界](#数据口径与准确性边界)与[数据完整性与异常处理](#数据完整性与异常处理)。
底层 `tdxhub.tdx` 不承诺跨版本稳定。源码版本更新不代表已发布到 PyPI；以下说明以当前源码为准。
版本号唯一源为 `tdxhub/__init__.py` 的 `__version__`（`pyproject.toml` 通过 `dynamic = ["version"]` 读取），`python -m tdxhub --version` 直接反映该值。

## 目录

- [功能概览](#功能概览)
- [安装指南](#安装指南)
- [快速开始](#快速开始)
  - [1. 读取本地 VIPDOC 文件](#1-读取本地-vipdoc-文件)
  - [2. 在线行情与指数获取](#2-在线行情与指数获取)
  - [3. 个股与指数区分说明](#3-个股与指数区分说明)
  - [4. 资金流向与主力监控](#4-资金流向与主力监控-capital-flow)
- [数据完整性与异常处理](#数据完整性与异常处理)
- [复权、GBBQ 与历史换手率](#复权gbbq-与历史换手率)
- [官方配置与财务数据](#官方配置与财务数据)
- [增量同步与 SQLite 持久化](#增量同步与-sqlite-持久化)
- [HTTP API 服务](#http-api-服务)
- [命令行工具 (CLI)](#命令行工具-cli)
- [测试与本地开发](#测试与本地开发)
- [TDX 模块维护](#tdx-模块维护)
- [文档索引](#文档索引)
- [License](#license)

---

## 功能概览

| 功能模块 | 核心入口 | 核心能力说明 |
|---|---|---|
| **本地行情** | `tdxhub.reader.Reader` | 读取日线、1/5分钟线、扩展市场、退市股缓存（`ds_cache`）、板块与官方配置文件 |
| **在线行情** | `tdxhub.quotes.Quotes` | 实时切片、全量/分页 K 线、大盘指数、分时、逐笔分笔、集合竞价、财务与扩展行情 |
| **资金流向** | `capital_flow*` / `sector_capital_flow` | 逐笔切片（超大/大/中/小单）、主力5日/20日净流入、行业板块资金流汇总及成分股穿透 |
| **品种分类** | `tdxhub.security` | 自动识别区分 A/B 股、指数、ETF、普通基金、债券等，支持多市场统一过滤 |
| **复权与换手率** | `tdxhub.gbbq` | GBBQ/XDXR 规范化、高精度前复权/后复权、历史流通股本快照与日 K 换手率计算 |
| **官方配置** | `tdxhub.official` | 解析板块 (`spblock`)、行业 (`tdxhy`/`incon`)、北交所目录、估值统计与新股申购配置 |
| **专业财务** | `tdxhub.affair.Affair` | 财务数据包清单获取、自动下载、MD5 校验与结构化解析 |
| **增量同步** | `tdxhub.pull` | 自动规划缺口区间、分页拉取、逐笔分钟线补齐、SQLite 事务与幂等持久化 |
| **HTTP 服务** | `tdxhub.http` | 基于 FastAPI 提供轻量 RESTful API，涵盖行情、K 线、逐笔、资金流与财务路由 |
| **CLI 命令行** | `tdxhub` | 提供离线读取、行情查询、优选测速节点、财务管理等终端命令 |

---

## 安装指南

PyPI 发行包名为 **`tdxhub-sdk`**；安装后直接使用 `import tdxhub`，CLI 命令为 `tdxhub`。

```bash
# 基础安装
python -m pip install -U tdxhub-sdk
```

### 内置协议实现

自 0.3.0 起，项目已将 `tdxpy 0.2.7` 的纯 Python 协议与文件读取实现迁入
`tdxhub/tdx/`，不再依赖外部 `tdxpy` / `pytdx`，也不需要 Cython 编译。
`Quotes`、`Reader`、财务、CLI 与 HTTP 的现有入口保持不变；`tdxhub.tdx` 是底层实现模块，不承诺跨版本 API 稳定性，不提供顶层 `tdx` / `tdxpy` 兼容别名。
直接捕获 `tdxpy.exceptions` 的外部调用方需调整异常导入，因为内置异常与外部包不再是同一类型。

### 可选能力扩展

CLI 已包含在基础安装中，无需额外依赖；`[cli]` 仅保留为兼容空扩展。根据业务需求安装其他扩展：

```bash
# 1. 支持节假日日历解析
python -m pip install -U "tdxhub-sdk[holiday]"

# 2. 支持 FastAPI HTTP 服务端
python -m pip install -U "tdxhub-sdk[server]"

# 3. 安装全部可选依赖
python -m pip install -U "tdxhub-sdk[all]"
```

### 源码开发

```bash
git clone https://github.com/RandomLiu2025/tdx-hub.git
cd tdx-hub
uv sync --frozen --all-extras
uv run --frozen tdxhub --version
```

---

## 快速开始

在线示例依赖可访问的通达信公共行情节点，历史数据覆盖和节点可用性可能变化；本地读取仅需已有文件，
不要求安装或运行通达信客户端。示例代码、日期及行业名称应按实际查询对象调整。

### 1. 读取本地 VIPDOC 文件

`tdxdir` 支持通达信客户端安装根目录，或直接指向 `vipdoc` 目录。代码支持纯数字或带前缀形式（如 `600000`、`SH600000`、`SH.600000`），自动判定沪、深、北三大市场。

```python
from tdxhub.reader import Reader

# 标准市场本地读取
reader = Reader.factory("std", tdxdir=r"D:\new_tdx")

# 日线、1分钟、5分钟 K 线
daily = reader.daily("SH.600000")
minute = reader.minute("600000")
five_minute = reader.fzline("600000")
print(daily.tail())

# 退市证券缓存（ds_cache/*.~~~day）
retired = reader.delisted_daily("sz000003")

# 本地板块与官方配置
concepts = reader.block("block_gn.dat")
beijing = reader.beijing_stocks()
industries = reader.stock_industries()
```

扩展市场本地文件（期货、期权等）使用独立 factory：

```python
ext_reader = Reader.factory("ext", tdxdir=r"D:\new_tdx")
future_daily = ext_reader.daily("4#CF7D0LAO")
```

---

### 2. 在线行情与指数获取

`Quotes` 是统一的行情工厂，推荐使用 `with` 上下文管理器管理连接生命周期：

```python
from tdxhub.quotes import Quotes

with Quotes.factory("std", timeout=5) as client:
    # 1. 实时切片快照（支持个股与指数）
    quotes = client.quotes(["sh000001", "399001", "sh600000", "sz000001"])

    # 2. 个股 K 线（bars）与全量历史回溯（bars_all）
    daily_bars = client.bars("sh600000", frequency=9, offset=100)
    all_bars = client.bars_all("sh600000", frequency=9, since="20250101")

    # 3. 指数专用 K 线（index / index_bars / index_all）
    # 自动识别沪、深、北交易所核心指数及行业板块指数，支持带前缀代码
    sh_idx = client.index("000001", frequency=9, offset=100)      # 上证指数 (沪市)
    sz_idx = client.index("399001", frequency=9, offset=100)      # 深证成指 (深市)
    bj_idx = client.index("899050", frequency=9, offset=100)      # 北证50 (北交所)
    bk_idx = client.index("880491", frequency=9, offset=100)      # 通达信板块指数
    idx_all = client.index_all("sh000001", frequency=9, since="20250101")

    # 4. 盘中分时与逐笔成交
    minute = client.minute("sz300394")
    minute_241 = client.minute_241("sh600000")                     # 241 根规范化分钟线
    history_trades = client.transactions_all("sh600000", date="20250910")
    auction = client.call_auction("sh600000")                     # 早盘集合竞价

    # 5. 资金流向与主力监控
    flow = client.capital_flow("sz002594")                        # 当日实时资金流分档与主力净流入
    hist_flow = client.capital_flow("sh600519", date="20260914")    # 指定历史日资金流

    # 6. 证券分类与综合聚合
    etfs = client.stocks(market=1, security_type="etf")            # 筛选沪市所有 ETF
    summary = client.stock_info(["sh600519", "sz000001"])          # 37 列规范化聚合详情
```

标准行情 `client.quotes(symbols, batch_size=80, diagnostics=True)` 自动分批、稳定去重，
可通过 `df.attrs["diagnostics"]` 区分缺失行情和请求失败。
主站按市场/接口缓存近期能力，详见 [标准行情 API](docs/api/quote1.md)。

#### 数据口径与准确性边界

- 分笔价格按品种精度解码（ETF、可转债不套用 A 股的两位小数）；分笔数量保留协议单位。
- `get_k_data(code, start_date, end_date)` 按实际交易日期分页，区间为 **左闭右开** `[start_date, end_date)`，不按自然日推算记录偏移。
- 指数快照没有可交易五档盘口，`bid/ask` 及其数量设为空；沪深指数提供 `up_count/down_count`，北交这两个字段尚未确认，保留为空。股票/基金盘口不变。
- `stock_count(2)` 与 `stocks(2)` 使用相同的北交报告目录口径；SDK 可用 `stock_count(2, raw=True)` 查看不同口径的协议计数，不能把两者之差解释为漏股。
- `minute_241()` 只用有效的 09:25～09:30 前成交推导竞价量额，新增 `auction_status`。缺失/不自洽时价格为空、量额为 0，且不扣减 09:31；它们不是零价成交。成功扣减时，09:31 的 OHLC 仍是原始分钟柱，不能反推为剔除竞价后的 OHLC。
- 资金流是分笔估算，**不是交易所官方资金流，也不保证与其他软件算法一致**。当前仅股票/基金使用已确认的每手 100 股/份；未校准单位/币种的品种（如债券、B 股、指数）拒绝估算，但仍可读取原始分笔。特殊类型与无效成交被排除，SDK `attrs` 提供数量诊断；HTTP 保持原有行列表结构，不输出 `attrs`。
- `finance()` 已修复金额被放大 10 倍和部分槽位误命名：金额按千元转元、股本按万股转股，每股指标不缩放。`finance_schema="tdx_finance_v2"` 标识新契约；错误旧名（如 `zhigonggu`、`farengu`、`changqifuzhai`）返回空值，请迁移到 `meigushouyi`、`shangniantongqiyingyeshouru`、`shaoshugudongquanyi` 等正确字段，完整清单见 [财务 API](docs/api/quote1.md#13-读取财务信息)。`raw_fields` 保留未换算槽位，`data_quality` 提供状态/未知项；`normalized` 仅表示已按字段表归一化，不是财报审计认证。不要对新结果再次除以 10。

更多字段、状态与单位说明见 [标准行情 API](docs/api/quote1.md) 和 [财务字段映射表](docs/api/fields.md)。

#### 常用 K 线频率说明

| 参数值 | 频率 | 参数值 | 频率 |
| :---: | :---: | :---: | :---: |
| `8` / `7` | 1 分钟 | `9` / `4` | 日线 |
| `0` | 5 分钟 | `5` | 周线 |
| `1` | 15 分钟 | `6` | 月线 |
| `2` | 30 分钟 | `10` | 季线 |
| `3` | 60 分钟 / 1 小时 | `11` | 年线 |

#### 容灾与节点热备机制
标准与扩展行情客户端默认内置**有界自动故障转移**（Failover）：
* 内部维护健康节点池，发生断线时自动切换可用节点（连接故障默认冷却 60 秒），标准快照/K 线协议错误按接口能力暂避 300 秒；
* 支持通过 `server=(ip, port)` 锁定特定服务器，或使用 `client.server_status()` 检查节点状态。
* 可用 `probe_symbols=["sh600000", "sz000001", "bj920002"]` 和 `probe_frequencies=[9, 8]`
  按目标市场分别探测 quotes/日 K/分钟 K，合法空响应记为能力未知；请使用当前有效证券作为样本。
* `request_timeout`（默认等于 `timeout`）为每次底层请求提供跨连接、握手、分片和切换的总预算；
  重试统一由节点池管理，`auto_retry` 仅保留参数兼容，不再叠加重试。
* 开启 `heartbeat=True` 后，心跳故障隔离并关闭旧连接，下次请求切换；不保证公共主站 SLA 或数据新鲜度。

默认启动探测仍为上海日 K（扩展行情为合约数量）。初始化探测、批量分页和连接池借用等待的预算边界
见 [标准行情 API](docs/api/quote1.md#超时心跳与边界)。分页与聚合会发起多次底层请求，不能把单次请求预算当作整个高层调用的总时限。

#### round2 新协议与自动降级

自 2025 年起，通达信主站逐步停用旧版 K 线命令（服务器返回 2003 错误码）与旧版实时五档命令
（返回合法但 `count=0` 的空快照）。SDK 内置了对应的新协议实现（`tdxhub.tdx.round2`，含首帧
握手 `0x000d`、心跳 `0x0004`、K 线 `0x052d`、实时快照 `0x054c`，支持服务端复权）：

* **默认直接走 round2**：`bars()` / `bars_all()` / `get_k_data()` 首次调用即使用 round2 新协议，
  数据与旧命令口径一致（成交量/成交额单位不变），换手率与 GBBQ 复权链路无缝衔接；
* **旧协议逃生口**：设置环境变量 `TDXHUB_KLINE_PROTOCOL=legacy` 可恢复"先试旧命令、失败再降级"的行为；
* **手动使用 Round2Client**（也可独立调用快照与指数）：

```python
from tdxhub.tdx.round2 import Round2Client

client = Round2Client(proxy='http://user:pass@host:2260')  # 代理可选
client.connect('119.29.19.242', 7709, time_out=12)

daily = client.get_bars('600000', 'day', 100, adjust='qfq')     # 日线+前复权
idx = client.get_bars('sh000001', 'day', 1, index=True)          # 指数（含涨跌家数）
snap = client.get_security_quotes(['600000', 'sz000001'])        # 实时快照（新命令）
client.close()
```

#### 网络代理

在线行情全链路支持经 HTTP(S) CONNECT 或 SOCKS5 代理访问主站，代理凭据仅在内存中使用：

* 优先级最高：构造参数 `StdQuotes(..., proxy='http://user:pass@host:port')`；
* 免改代码：环境变量 `TDXHUB_PROXY`（或 `TDXHUB_PROXY_URL`）；
* 代理对标准行情、扩展行情、round2 降级连接统一生效；`python -m tdxhub bestip` 测速仅读环境变量。

```python
from tdxhub.quotes import Quotes

client = Quotes.factory("std", server=('119.29.19.242', 7709),
                        proxy='socks5://user:pass@host:port', timeout=15)
```

---

### 3. 个股与指数区分说明

通达信底层协议严格区分股票与指数，调用时请注意：

1. **同代码歧义（如 `000001`）**：
   * `client.quotes("000001")`：默认判定为深市股票 **平安银行**（`000001.SZ`）。
   * `client.quotes("sh000001")`：显式指定沪市前缀，获取 **上证综合指数**。
2. **接口专业分工**：
   * **指数专用 K 线**：使用 `client.index(...)` 或 `client.index_bars(...)`。
   * **个股/ETF K 线**：使用 `client.bars(...)`。
3. **程序化判定证券类型**：
   使用内置分类器 [`classify_security`](tdxhub/security.py) 判断品种：
   ```python
   from tdxhub.security import classify_security

   classify_security(0, "000001")  # 'a_stock' (平安银行)
   classify_security(1, "000001")  # 'index'   (上证指数)
   classify_security(2, "899050")  # 'index'   (北证50)
   classify_security(1, "510300")  # 'etf'     (沪深300ETF)
   ```

---

### 4. 资金流向与主力监控 (Capital Flow)

资金流向模块基于分笔（Tick）成交金额分档，提供主力与散户资金流估算。它不是交易所官方资金流，不保证与其他软件的算法或日 K 成交额完全一致；适用范围与排除记录见上方「数据口径与准确性边界」。

#### 4.1 划分标准与计算逻辑

按单笔成交金额划分四个档位（默认阈值可自定义；“主力/散户”是金额分档标签，不代表已识别交易者身份）：
- **超大单**：单笔成交额 $\ge 100$ 万元
- **大单**：$20 \text{ 万元} \le$ 单笔成交额 $< 100$ 万元
- **中单**：$4 \text{ 万元} \le$ 单笔成交额 $< 20$ 万元
- **小单**：单笔成交额 $< 4$ 万元
- **聚合分类**：
  - **主力资金** = 超大单 + 大单
  - **散户资金** = 中单 + 小单
- **主动买卖判定**：按分笔协议的买卖方向标记估算；中性成交计入总成交额，不计入买卖净额：
  $$\text{主力净流入} = \text{主力主动买入额} - \text{主力主动卖出额}$$
  $$\text{主力净流入占比} = \frac{\text{主力净流入额}}{\text{全天总成交金额}} \times 100\%$$

#### 4.2 实时与单日个股资金流向 (`capital_flow`)

支持盘中实时切片统计（默认当日）与历史单日资金流分析，内置历史交易日内存高速缓存：

```python
with Quotes.factory("std", timeout=5) as client:
    # 1. 盘中实时个股资金流向（默认拉取当日全量逐笔实时聚合）
    flow = client.capital_flow("002594")  # 比亚迪
    print(flow)

    # 快捷提取核心属性（存储于 DataFrame attrs 中）
    print("主力净买入(元):", flow.attrs["main_net"])
    print("主力净买入占比:", f"{flow.attrs['main_net_pct']}%")
    print("散户净买入(元):", flow.attrs["retail_net"])
    print("全天总成交额(元):", flow.attrs["total_turnover"])

    # 2. 查询指定历史单日资金流向（自动命中本地缓存加速）
    hist_flow = client.capital_flow("sh600519", date="20260914")
```

#### 4.3 多日资金流向与主力 5日 / 20日 累计净流入 (`capital_flow_history`)

回溯指定个股近 N 个交易日（默认 20 日，`days` 必须为正整数），逐日计算成交额与各档位资金流，并输出 **5日/20日主力滚动累计净额及占比**。
`days` 控制返回行数，SDK 会额外读取最多 19 个预热交易日；同一截止日和数据源下，`days=5` 不会把 20 日指标缩成 5 日合计。

仅完整窗口才计算累计指标；历史不足时为 `NaN`（HTTP 为 `null`）。使用
`window_5d_complete` / `window_20d_complete` 判断窗口是否完整，
`window_5d_days` / `window_20d_days` 查看实际覆盖天数；缺少计算所需成交数据则抛出异常，而非填零。

```python
with Quotes.factory("std", timeout=5) as client:
    # 回溯近 20 个交易日资金流向
    hist = client.capital_flow_history("002594", days=20)

    # 1. 查看 5日 / 20日 主力汇总指标（attrs 属性字典）
    print("5日主力净流入:", f"{hist.attrs['main_5d_net'] / 1e8:.3f} 亿元, 占比: {hist.attrs['main_5d_pct']}%")
    print("20日主力净流入:", f"{hist.attrs['main_20d_net'] / 1e8:.3f} 亿元, 占比: {hist.attrs['main_20d_pct']}%")

    # 2. 查看逐日明细与滚动累计列
    cols = ["date", "close", "change_pct", "main_net", "main_5d_net", "main_20d_net"]
    print(hist[cols].tail())
```

#### 4.4 行业板块资金流向汇总及成分股穿透 (`sector_capital_flow`)

支持按行业名称模糊检索（通达信行业及申万一级/二级/三级行业，如 "白酒"、"半导体"、"银行"、"汽车整车"），或传入自定义成分股代码列表。行业覆盖以当前配置文件为准。
输出板块主力净流入汇总、净流入/净流出最多的成分股，以及按主力净买入降序排列的明细。
成分股按市场与代码去重；任一成分股失败或无有效成交数据时，整体抛出 `TdxhubIncompleteDataError`，不交付部分板块总量。

```python
with Quotes.factory("std", timeout=5) as client:
    # 1. 行业板块资金流汇总（别名 block_capital_flow）
    sector = client.sector_capital_flow("白酒")

    # 板块汇总指标（attrs 属性字典）
    print(f"板块名称: {sector.attrs['sector_name']} (成分股数量: {sector.attrs['stock_count']})")
    print(f"主力净流入: {sector.attrs['main_net'] / 1e8:.3f} 亿元 (占比: {sector.attrs['main_net_pct']}%)")
    print(f"资金流入龙头: {sector.attrs['top_inflow_code']}")
    print(f"主力出逃最多: {sector.attrs['top_outflow_code']}")

    # 成分股主力资金流排行（按 main_net 降序排列）
    print(sector[["code", "main_net", "main_pct", "super_net", "large_net", "total_amount"]].head())

    # 2. 自定义成分股池统计（例如自选股主力流向）
    custom_sector = client.sector_capital_flow(
        name="核心资产",
        symbols=["600519", "000858", "002594", "300750"],
    )
```

#### 4.5 资金流向接口速查

| Python API | 核心参数 | 返回结构 | 核心指标与 attrs |
|---|---|---|---|
| `capital_flow(symbol, date=None)` | `symbol`: 股票代码<br>`date`: 日期 (YYYYMMDD，默认当日) | 7行分档明细 DataFrame<br>(超大/大/中/小/主力/散户/合计) | `main_net`, `main_net_pct`, `retail_net`, `total_turnover`, `trade_count` |
| `capital_flow_history(symbol, days=20)` | `symbol`: 股票代码<br>`days`: 回溯交易日数 (默认20) | N行逐日明细 DataFrame<br>(含 5/20 日滚动列) | `main_5d_net`, `main_5d_pct`, `main_20d_net`, `main_20d_pct` |
| `sector_capital_flow(name, symbols=None)` | `name`: 行业/板块名<br>`symbols`: 自选成分股列表 | 成分股排行 DataFrame<br>(按主力净买入降序) | `sector_name`, `stock_count`, `main_net`, `main_net_pct`, `top_inflow_code` |
| `money_flow(symbol=None, days=None)` | `symbol`: 可选，为 None 时返回官方报告 | 兼容入口，自动按参数路由至对应资金流接口 | - |

> **HTTP API 对应路由**：
> - 实时/单日个股资金流：`GET /capital_flow?code=002594&date=20260914`
> - 多日累计与逐日明细：`GET /capital_flow/history?code=002594&days=20`
> - 行业/板块资金流汇总：`GET /capital_flow/sector?name=白酒`


---

## 数据完整性与异常处理

在线数据的“缺失”“请求失败”和“真实零成交”含义不同，不应统一填成 0：

| 场景 | 当前行为 | 调用方建议 |
|---|---|---|
| 批量快照 | `quotes(..., diagnostics=True)` 在 `attrs["diagnostics"]` 提供 `status`、`missing`、批次数与去重数 | 检查 `ok` / `missing` / `request_failed` 等状态，不只检查表是否为空 |
| K 线/分笔分页 | 连接失败抛出 `TdxhubConnectionError`；到达协议偏移上限仍无法确认完整性时抛出 `TdxhubIncompleteDataError` | 缩小区间或检查节点，不将部分数据当作全量历史 |
| 多日资金流 | 窗口历史不足返回缺失指标；计算范围内缺少必要成交数据则抛出异常 | 检查窗口完整性字段，区分历史不足与请求失败 |
| 板块资金流 | 任一成分股失败导致整次汇总失败 | 查看异常原因并重试，不忽略失败成分 |

```python
from tdxhub.exceptions import TdxhubConnectionError, TdxhubIncompleteDataError
from tdxhub.quotes import Quotes

try:
    with Quotes.factory("std", timeout=5, request_timeout=8) as client:
        history = client.bars_all("sh600000", frequency=9, since="20250101")
except (TdxhubConnectionError, TdxhubIncompleteDataError) as exc:
    print(f"未能取得完整历史数据：{exc}")
```

这不是所有异常类型的穷举：非法参数和底层协议错误仍可能抛出各自异常。
DataFrame 的 `attrs` 是 SDK 辅助元数据，导出或 pandas 转换后不保证保留；需要诊断时应单独保存。
HTTP 不序列化 `attrs`，应同时检查响应状态与业务字段 `code`。

---

## 复权、GBBQ 与历史换手率

全量日 K 线可在分页合并后自动执行前复权/后复权，并结合历史流通股本计算换手率：

```python
from tdxhub.quotes import Quotes

with Quotes.factory("std", timeout=5) as client:
    # 自动计算前复权 (qfq) 及历史换手率 (turnover)
    qfq_bars = client.bars_all(
        "sh600000",
        frequency=9,
        since="20200101",
        adjust="qfq",
        turnover=True,
    )
```

复权只调整价格，不改变实际成交量和成交金额；历史换手率仍使用当日实际成交量及对应流通股本。
基金/ETF 支持现金分红与扩缩股事件，保留三位价格精度，股票保留两位。

底层复权与股本快照模块亦可独立调用（`prices` 为行情 DataFrame，`actions` 为对应证券的 GBBQ/XDXR 事件）：

```python
from tdxhub.gbbq import adjust_prices, get_equity_snapshot, normalize_gbbq

events = normalize_gbbq(actions)
adjusted = adjust_prices(prices, events, method="qfq")
snapshot = get_equity_snapshot(events, at="2026-09-10")
```

---

## 官方配置与财务数据

### 1. 官方配置解析

通达信客户端 `hq_cache` 目录下的二进制配置文件可直接读取为结构化数据：

```python
from tdxhub.official import associate_industries, parse_official

# 板块与北交所配置
blocks = parse_official(r"D:\new_tdx\T0002\hq_cache\spblock.dat")
beijing = parse_official(r"D:\new_tdx\T0002\hq_cache\tdxbjmore.cfg")

# 行业匹配
assignments = parse_official(r"D:\new_tdx\T0002\hq_cache\tdxhy.cfg")
dictionary = parse_official(r"D:\new_tdx\T0002\hq_cache\incon.dat")
industries = associate_industries(assignments, dictionary)
```

无本地客户端时，也支持在线从行情站下载最新行业配置文件：
```python
with Quotes.factory("std", timeout=5) as client:
    industries = client.stock_industries(["sh600519", "sz000001"])
```

### 2. 个股综合统计与估值指标 (Statistics)

直接获取通达信官方报告中的全市场个股综合统计数据（`tdxstat.cfg`），包含**市盈率TTM、静态市盈率、股息率、当日涨跌幅、连涨连跌天数、区间涨跌幅（5日/10日/20日/60日/年初至今）**：

```python
with Quotes.factory("std", timeout=5) as client:
    # 查询指定个股（支持单个代码或列表；为 None 时返回当前报告覆盖的全部证券）
    stats = client.statistics(["600519", "002594", "300750", "000001"])
    print(stats[["code", "date", "pe_ttm", "pe_static", "dividend_yield", "trend_days", "change_pct", "change_5d", "change_20d", "change_ytd"]])
```

> **HTTP API**：`GET /tdx/stat`（支持 `?codes=600519,002594` 过滤）。

### 3. 新股申购日历与发行信息 (IPO / XGSG)

直接从通达信官方报告归档（`xgsg.cfg`）获取全市场近期拟上市及已申购新股（包含沪市主板/科创板、深市主板/创业板、北交所）的申购日期、发行价格与市场归属：

```python
with Quotes.factory("std", timeout=5) as client:
    # 1. 获取全市场近期新股申购日历（别名 client.ipo() / client.new_stocks()）
    ipos = client.xgsg()
    print(ipos[["market", "code", "name", "date", "issue_price"]].head())

    # 2. 查询指定新股发行信息
    single_ipo = client.xgsg("001246")  # 支持纯数字或 "sz001246"
```

> **HTTP API**：`GET /tdx/xgsg` 或 `GET /ipo`（支持 `?code=001246` 过滤）。

### 4. 专业财务文件管理（Affair）


```python
from tdxhub.affair import Affair

# 获取官方财务数据压缩包清单
manifest = Affair.files()

# 下载并解析单季财务数据
archive = Affair.fetch(downdir="output", filename="gpcw19960630.zip")
finance_df = Affair.parse(downdir="output", filename="gpcw19960630.zip")
```

财务文件按分块实际内容组装，分块长度不一致会抛出 `tdxhub.tdx.errors.ProtocolError`。
按清单下载（`Affair.fetch()` 不指定 `filename`）还会校验 MD5；已知文件长度时，
超出清单大小或连续三次空块仍未下载完成也会抛出 `ProtocolError`，不返回截断文件。
未知长度的底层下载以空块作为结束标志；指定 `filename` 的直接下载不执行清单 MD5 校验。

内置财务 crawler 支持默认临时文件和指定保存路径；ZIP 按内容识别，不依赖下载文件的后缀，
压缩包需包含且仅包含一个 DAT 文件。`fetch_and_parse()` 在成功或解析失败后均关闭下载流；
直接调用 crawler 的 `parse()` 时，文件流由调用方负责关闭。

HTTP 模式始终按 `chunksize` 分块读取（默认 50 KiB，必须为正整数），不因缺少长度头而一次性读入内存。
存在 `Content-Length` 时校验其格式及响应体长度，截断或读取到超出声明长度的数据会抛出 `ProtocolError`；
缺少长度头时以 EOF 为结束标志，无法仅凭长度检查完整性。进度回调为 `(downloaded, total_size)`，
未知总长为 `0`，正常 EOF 时也会回调一次。
`fetch_via_http()` 及 HTTP 模式下的 `fetch_and_parse()` 支持 `timeout=30`（秒，可设置为有限正数），
限制阻塞网络操作的等待时间，而非整个文件的下载时限；连接、读取、校验或回调失败都会关闭下载流。
`chunksize`、`timeout` 非法时，在联网或打开目标文件前抛出 `ValueError`。
底层 `get_report_file_by_size()` 的 `filesize` 必须为非负整数（`0` 表示未知长度），
非法值抛出 `ValueError`；畸形分块响应统一抛出包含文件名及偏移的 `ProtocolError`。这些数值参数均不接受布尔值。

---

## 增量同步与 SQLite 持久化

`tdxhub.pull` 模块支持按半开区间 `[start, end)` 智能计算缺失区间并增量同步至 SQLite，事务保证幂等写入：

```python
from tdxhub.pull import PullService, QuoteFetcher, SQLiteStore
from tdxhub.quotes import Quotes

with Quotes.factory("std", timeout=5) as client, SQLiteStore("market.db") as store:
    fetcher = QuoteFetcher(client, source_volume_unit="lots")
    service = PullService(store, fetcher)
    bars = service.sync(
        market="sh",
        code="600000",
        frequency=9,
        start="2026-09-01",
        end="2026-09-11",
        adjustment="none",
    )
```

---

## HTTP API 服务

安装 `tdxhub-sdk[server]` 后，可通过依赖注入启动 FastAPI 数据服务。将下面代码保存为 `serve_tdxhub.py`：

```python
# serve_tdxhub.py
from contextlib import asynccontextmanager
from tdxhub.http import MarketDataService, create_app
from tdxhub.quotes import Quotes

def make_app():
    service = MarketDataService(standard_quotes=None)
    app = create_app(service)

    @asynccontextmanager
    async def lifespan(_app):
        client = Quotes.factory("std", timeout=5)
        service.standard_quotes = client
        try:
            yield
        finally:
            service.standard_quotes = None
            client.close()

    app.router.lifespan_context = lifespan
    return app
```

启动命令：
```bash
python -m uvicorn serve_tdxhub:make_app --factory --host 127.0.0.1 --port 8000 --workers 1
```
源码开发环境下，在命令前加 `uv run --frozen`。启动后访问交互式 Swagger UI 文档：<http://127.0.0.1:8000/docs>。
`GET /` 检查进程存活，`GET /ready` 检查是否已注入标准行情客户端（不主动探测上游连接或数据新鲜度）。此示例仅注入标准行情客户端，
使用 `/ex/*` 扩展行情路由时还需注入扩展行情客户端并管理其生命周期。
默认仅监听本机；对外提供服务前应另行配置认证、限流和访问控制。

响应使用统一包络 `{"code": 0, "msg": "ok", "data": ...}`，失败时为 `code=1, data=null`。
部分上游请求失败仍返回 HTTP 200，**不能仅凭 HTTP 状态码判断成功**；DataFrame 转为行列表，
缺失数值为 `null`，不包含 `attrs` 中的资金流汇总与诊断信息。

### 核心接口路由速查

| 分类 | 请求方式与路径 | 查询参数 | 功能说明 |
|---|---|---|---|
| **资金流向** | `GET /capital_flow` | `code` (必需), `date` (可选, YYYYMMDD) | 单日/实时逐笔分档资金流向与主力净流入 |
| **资金流向** | `GET /capital_flow/history` | `code` (必需), `days` (可选, 默认20) | 历史逐日资金流明细及 5日/20日主力累计净流入 |
| **资金流向** | `GET /capital_flow/sector` | `name` (板块名) 或 `codes` (代码列表), `date` | 行业/板块主力资金流汇总、龙头股与成分股穿透 |
| **官方统计** | `GET /tdx/stat` | `codes` (可选, 多代码逗号分隔) | 官方估值与综合统计（市盈TTM、股息率、连涨天数等） |
| **新股申购** | `GET /tdx/xgsg` 或 `GET /ipo` | `code` (可选) | 全市场拟上市及已申购新股日历与发行信息 |
| **官方报告** | `GET /tdx/stat2` | 无 | 官方盘后资金流数据包 |
| **实时行情** | `GET /quote` | `codes` (如 `sh600000,000001`) | 多标的实时切片快照 |
| **分时逐笔** | `GET /minute` / `GET /trade/all` | `code` | 盘中分时与当日逐笔成交明细 |
| **K 线行情** | `GET /kline/day/all` | `code`, `since`, `adjust` (qfq/hfq) | 全量历史日 K 线（支持自动前/后复权） |
| **指数行情** | `GET /index/day/all` | `code` | 沪深北大盘与行业指数全量日 K 线 |

#### 资金流向接口调用示例

```bash
# 1. 查询个股实时资金流分档切片（主力净额、散户净额及占比）
curl -s "http://127.0.0.1:8000/capital_flow?code=002594"

# 2. 查询个股 20 日历史资金流明细与 5日/20日主力累计净流入
curl -s "http://127.0.0.1:8000/capital_flow/history?code=002594&days=20"

# 3. 查询白酒行业板块主力资金流向及成分股净买入排行
curl -sG "http://127.0.0.1:8000/capital_flow/sector" --data-urlencode "name=白酒"

# 4. 查询指定股票估值统计指标（市盈TTM、静态市盈、股息率、连涨天数、区间涨跌幅）
curl -s "http://127.0.0.1:8000/tdx/stat?codes=600519,002594"
```

---

## 命令行工具 (CLI)

基础安装已提供 `tdxhub` 命令，也可使用 `python -m tdxhub`。源码环境下使用 `uv run --frozen tdxhub ...`。

```bash
# 查看全局帮助与版本
tdxhub --help
tdxhub --version

# 读取本地日 K 线
tdxhub reader --tdxdir "D:\new_tdx" --symbol 600000 --action daily

# 查询在线行情日 K 线
tdxhub quotes --symbol 600000 --action daily

# 测试并优选最快的前 5 个行情服务器节点
tdxhub bestip --limit 5

# 查看与下载财务文件
tdxhub affair --listfile

# 批量导出指定股票当前分页行情（不是全量历史，默认保存到 bundle/）
tdxhub bundle --symbol 600000,000001 --action daily
```

---

## 测试与本地开发

```bash
# 安装锁定的完整运行/测试依赖
uv sync --frozen --all-extras

# 锁文件、依赖、关键 Ruff、编译、离线回归与 wheel/sdist 检查
make check

# 仓库外新建环境，只安装 wheel 和锁定依赖，验证无外部 tdxpy/pytdx/Cython
make wheel-check

# 可选：报告包含既有代码债务的全部 Ruff 规则
make lint-all
```

常用任务（完整列表见 `make help`）：

| 命令 | 用途 |
|---|---|
| `make test` | 默认离线回归，排除 `network` 与 `integration` 标记 |
| `make test-network` | 公共网络行情测试，需要可访问的外部节点 |
| `make test-vipdoc TDXHUB_TDXDIR="/path/to/new_tdx"` | 使用本地通达信数据运行集成测试 |
| `make build` | 生成 `dist/` 下的 wheel 与 sdist |
| `make package-check` | 构建并校验元数据、源码、README、许可证及必需资源 |
| `make pack` | 打包包含源码、测试、文档和锁文件的源码快照 |

`make test`、`make check` 等测试命令使用锁定的完整依赖；`make lint-all` 可能报告既有代码债务，
不是默认 `make check` 的全量 Ruff 门禁。网络测试结果受节点及交易日影响，不替代离线回归。

CI 使用 uv：Linux Python 3.11–3.14 的 core/all 依赖矩阵，另有 Linux/macOS/Windows
Python 3.12 的独立 wheel 冒烟。网络行情测试不在默认离线门禁中；工作流配置不等于远端已验证。

---

## TDX 模块维护

源自 [tdxpy](https://github.com/mootdx/tdxpy) **0.2.7**（MIT），现由本项目维护。
不依赖外部 `tdxpy` / `pytdx` / Cython，不注册顶层 `tdx` 或 `tdxpy` 别名。

### 模块职责

| 模块 | 职责 |
| --- | --- |
| `client.py` / `extended.py` | 标准行情 `StandardClient` / 扩展行情 `ExtendedClient` |
| `round2.py` / `proxy.py` | round2 新协议（握手/心跳/K 线/快照/服务端复权）与 HTTP CONNECT/SOCKS5 代理 |
| `transport.py` / `heartbeat.py` | Socket 连接、流量统计、调用包装与心跳 |
| `protocol/std` / `protocol/ext` | 请求编码、响应解析；公共收发及解压校验在 `protocol/base.py` |
| `codec.py` / `constants.py` | 二进制字段解码、证券分类、价格系数与协议常量 |
| `errors.py` | 连接、调用与协议异常，上层无需导入具体解析器 |
| `files/` | 本地行情、板块、财务文件读取 |
| `crawler/` | 财务文件读取器所需的下载支持 |

上层行情、主站探测、财务共用 `StandardClient`；基金价格在解析时按系数转换一次，
证券目录昨收直接解码 float32，不再通过业务兼容层补做。日线分类统一在 `files`，
旧 `contrib.compat.TdxhubDailyBarReader` 保留为别名。Socket 兼容派生类保持原有行为。
协议日志名称为 `tdxhub.tdx`。版本统一使用 `tdxhub.__version__` 或 `python -m tdxhub --version`，不保留底层 `version.py`。

`tdxhub.tdx` 不带下划线，但仍是底层实现，**不承诺其 API 跨版本稳定**。
应用继续使用 `tdxhub.quotes.Quotes`、`tdxhub.reader.Reader` 等现有入口。
需要捕获协议错误时可从 `tdxhub.tdx.errors` 导入；外部 `tdxpy.exceptions` 类型不能捕获内部异常。

### 来源与授权

- [根目录 LICENSE](LICENSE)：本项目当前版权声明及完整 MIT 条款；
  [AUTHORS.rst](AUTHORS.rst) 为开发团队文档，不要求固定作者名单。
  两份文件均以根目录最新内容为准：sdist 保留在根目录，wheel 放入 `.dist-info/licenses/`。
- 上游来源与版本统一记录在本节，不再维护独立来源清单或历史哈希文件。
- 当前源码由 Git 与回归测试维护；包检查逐字节比较 wheel/sdist 和当前工作区源码及许可证，
  并校验根 README 的分发内容。引入新的上游版本时，同步更新本节来源说明并保留相应版权与许可条款。

### 维护验证

1. 按职责修改模块，不整体覆盖上游代码；保留版权与来源。
2. 补充 `tests/tdx` 的离线报文/文件回归，防止重复缩放、旧路径或外部依赖回流。
3. 运行 `make check`，再运行 `make wheel-check`，验证仓库外安装且全部底层模块可加载。
4. 打包检查拒绝旧 `_vendor`、`_tdx`、`tdxpy_compat`、旧许可证资源包、模块 README、独立来源清单以及编译残留。
5. 本节统一维护模块说明，不再保留 `tdxhub/tdx` 下的独立 README。根 README 随 sdist 分发，
   并作为 wheel 元数据中的项目说明；发布校验确保说明、LICENSE、AUTHORS.rst 和源码与当前工作区一致，并检查 MIT 基本条款与许可证元数据。

---

## 文档索引

更多深度用法与接口规范请参阅项目完整文档库：

- **快速上手**：[快速上手指南](docs/quick.md) · [环境安装与配置](docs/setup.md)
- **在线行情 API**：[标准行情接口 (StdQuotes)](docs/api/quote1.md) · [扩展行情接口 (ExtQuotes)](docs/api/quote2.md) · [辅助函数说明](docs/api/extras.md)
- **本地读取与财务**：[VIPDOC 本地文件读取](docs/api/reader.md) · [专业财务数据 (Affair)](docs/api/affair.md) · [财务字段映射表](docs/api/fields.md)
- **命令行使用**：[在线行情 CLI](docs/cli/quotes.md) · [本地读取 CLI](docs/cli/reader.md) · [财务下载 CLI](docs/cli/affair.md) · [行情测速 CLI](docs/cli/bestip.md) · [在线批量导出 CLI](docs/cli/bundle.md)
- **协议内置化**：[TDX 模块维护](#tdx-模块维护)

---

## License

本项目遵循 [MIT License](LICENSE) 开源协议。仅供个人学习、量化研究及合法的数据合规处理使用。

内置 TDX 代码源自 tdxpy 0.2.7（MIT），来源与维护方式见 [TDX 模块维护](#tdx-模块维护)。根目录 [LICENSE](LICENSE) 和 [AUTHORS.rst](AUTHORS.rst) 随源码和发行包一并提供。

# QMT API 参考手册

本文档是 `qmt-trader` skill 的完整 API 参考。当 SKILL.md 的速查不够用时，查阅本文件获取
参数细节、返回值结构和已知陷阱。

---

## 1. 初始化与配置

### 配置来源（优先级从高到低）

1. **环境变量**
   | 变量 | 默认 | 说明 |
   |------|------|------|
   | `BIGQMT_ACCOUNT_ID` | — | 资金账号 |
   | `BIGQMT_REDIS_HOST` | `127.0.0.1` | Redis 地址 |
   | `BIGQMT_REDIS_PORT` | `6379` | Redis 端口 |
   | `BIGQMT_REDIS_DB` | `5` | Redis DB |
   | `BIGQMT_REDIS_PASSWORD` | — | Redis 密码 |
   | `BIGQMT_RPC_TRANSPORT` | `redis` | 传输方式 redis/zmq |
   | `BIGQMT_RPC_TIMEOUT_SECONDS` | `6.0` | RPC 超时 |

2. **配置文件** `bigqmt_signal_trader_client_config.py`（在 PYTHONPATH 中，gitignored）

3. **备选配置文件** `bigqmt_signal_trader_local_config.py`

### Python 初始化

```python
from bigqmt_signal_trader.xtquant_compat import StockAccount, configure, xt_trader, xtdata

configure()  # 从配置/环境变量初始化
acc = StockAccount(xt_trader.client.account_id, "STOCK")
```

---

## 2. 行情数据 API

### 2.1 get_full_tick — 实时五档盘口

```python
xtdata.get_full_tick(code_list)
```

- **参数**: `code_list: list[str]`，如 `["000001.SZ", "600000.SH"]`；也支持整市场 `["SH"]`, `["SZ"]`
- **返回**: `dict[code -> dict]`，每只含 `lastPrice`/`open`/`high`/`low`/`lastClose`/`volume`/`amount`/
  `bidPrice`(10档)/`askPrice`(10档)/`bidVol`/`askVol`/`time`/`stime`
- **CLI**: `uv run python qmt.py tick 600000.SH 000001.SZ`
- **注意**: 整市场快照数据量大（5000+ 股），超时自动设 30 秒

### 2.2 get_market_data_ex — K线/历史行情

```python
xtdata.get_market_data_ex(
    field_list=None,      # ["close","open","high","low","volume","amount"] 或 None=全部
    stock_list=None,      # ["000001.SZ"]
    period="1d",          # "1d"/"1m"/"5m"/"15m"/"30m"/"60m"/"tick"
    start_time="",        # "YYYYMMDD" 或 "YYYYMMDDHHMMSS"
    end_time="",
    count=-1,             # -1=不限
    dividend_type="none", # "none"/"front"(前复权)/"back"(后复权)
    fill_data=True,       # 是否填充缺失
)
```

- **返回**: `dict[code -> pandas.DataFrame]`，index 是时间戳字符串，列含 `time`(epoch ms)/`open`/`high`/`low`/`close`/`volume`/`amount`
- **CLI**: `uv run python qmt.py kline 600000.SH --period 1d --count 60 --dividend front`
- **自愈**: 请求复权但服务端缺原始数据时（返回全 0），自动触发下载+重试
- **陷阱**: 前/后复权必须先在服务端下载原始数据，否则返回全 0（已自愈但仍可能首次慢）

### 2.3 get_instrument_detail — 合约详情

```python
xtdata.get_instrument_detail(stock_code)  # 别名 get_instrumentdetail
```

- **返回**: `dict`，含名称/上市日/合约乘数/最小变动价位等约 30 字段
- **CLI**: `uv run python qmt.py instrument 600000.SH`

### 2.4 get_stock_list_in_sector — 板块成分股

```python
xtdata.get_stock_list_in_sector(sector_name)  # 如 "沪深A股", "科创板", "创业板"
```

- **返回**: `list[str]` 代码列表
- **CLI**: `uv run python qmt.py sector "沪深A股"`

### 2.5 get_sector_list — 板块列表

```python
xtdata.get_sector_list()
```

- **返回**: `list[str]`
- **CLI**: `uv run python qmt.py sector`
- **注意**: 大 QMT 环境 fallback 返回 13 个常用板块名（非完整列表）

### 2.6 get_trading_dates — 交易日历

```python
xtdata.get_trading_dates(market="SH", start_time="", end_time="", count=-1)
```

- **CLI**: `uv run python qmt.py trading-dates --count 10`

### 2.7 get_north_finance_change — 北向资金

```python
xtdata.get_north_finance_change(period="1d")
```

- **CLI**: `uv run python qmt.py north`

### 2.8 get_longhubang — 龙虎榜

```python
xtdata.get_longhubang(stock_list=["600000.SH"], start_time="", end_time="", count=5)
```

- **返回**: `pandas.DataFrame`
- **CLI**: `uv run python qmt.py longhubang 600000.SH --count 5`

### 2.9 get_financial_data — 财务数据

```python
xtdata.get_financial_data(
    stock_list=["000001.SZ"],
    table_list=["Capital.CAPITAL"],  # 表名
    start_time="", end_time="",
)
```

- **CLI**: `uv run python qmt.py financial 000001.SZ --tables Capital.CAPITAL`

### 2.10 download_history_data2 — 下载历史数据

```python
xtdata.download_history_data2(
    stock_list=["600654.SH"], period="1d",
    start_time="20240101", dividend_type="front",
)
```

- **返回**: `{"finished": N, "total": M}`
- **CLI**: `uv run python qmt.py download 600654.SH --period 1d --start 20240101 --dividend front`

### 2.11 subscribe_whole_quote — 全推行情订阅

```python
sub_id = xtdata.subscribe_whole_quote(["SH","SZ"], callback=on_quote)
# ... 运行策略 ...
xtdata.unsubscribe_quote(sub_id)
```

- **机制**: 服务端真推送（非轮询），增量推送有变化的品种
- **CLI**: `uv run python qmt.py quote-subscribe SH SZ --max 10 --timeout 30`
- **心跳**: 客户端 3 秒一次 keepalive，服务端重启后自动恢复

---

## 3. 账户/持仓/委托查询 API

### 3.1 query_stock_asset — 查询资产

```python
asset = xt_trader.query_stock_asset(acc)
```

- **返回属性**: `account_id` / `cash`(可用现金) / `frozen_cash` / `total_asset` / `market_value`
- **CLI**: `uv run python qmt.py account`
- **容错**: RPC 失败时从 Redis 缓存 `bigqmt:positions:{account_id}` 读取

### 3.2 query_stock_positions — 查询全部持仓

```python
positions = xt_trader.query_stock_positions(acc)
```

- **返回属性**: `stock_code` / `stock_name` / `volume`(总持仓) / `can_use_volume`(可用) /
  `avg_price`(成本) / `price`(最新价) / `market_value` / `frozen_volume` / `yesterday_volume`
- **CLI**: `uv run python qmt.py positions [code]`

### 3.3 query_stock_position — 查询单只持仓

```python
pos = xt_trader.query_stock_position(acc, "600000.SH")
```

- **返回**: 单个对象或 `None`

### 3.4 query_stock_orders — 查询委托

```python
orders = xt_trader.query_stock_orders(acc, cancelable_only=False, strategy_name="")
```

- **返回属性**: `stock_code` / `order_type`(23=BUY,24=SELL) / `order_status` /
  `order_volume` / `traded_volume` / `price` / `order_sysid` / `order_remark`
- **CLI**: `uv run python qmt.py orders [--cancelable] [--strategy ""]`
- **⚠️ strategy_name 陷阱**: 下单时的 strategy_name 必须和查询时一致。服务端默认 `""` 返回全部；
  客户端 `BigQmtXtTrader` 默认 `"bigqmt_signal_trader"`。用 `""` 查全部最安全。

### 3.5 query_stock_trades — 查询成交

```python
trades = xt_trader.query_stock_trades(acc, strategy_name="")
```

- **返回属性**: `stock_code` / `order_type` / `traded_volume` / `traded_price` /
  `traded_at` / `order_sysid` / `trade_id`
- **CLI**: `uv run python qmt.py trades`

### 3.6 委托状态码

| 值 | 常量 | 含义 |
|----|------|------|
| 48 | ORDER_UNREPORTED | 未申报 |
| 49 | ORDER_WAIT_REPORTING | 等待申报 |
| 50 | ORDER_REPORTED | 已申报 |
| 51 | ORDER_REPORTED_CANCEL | 已申报撤单 |
| 52 | ORDER_PARTSUCC_CANCEL | 部成撤单 |
| 53 | ORDER_PART_CANCEL | 部撤 |
| 54 | ORDER_CANCELED | 已撤 |
| 55 | ORDER_PART_SUCC | 部分成交 |
| 56 | ORDER_SUCCEEDED | 全部成交 |
| 57 | ORDER_JUNK | 废单 |
| 255 | ORDER_UNKNOWN | 未知 |

可撤状态: 49, 50, 55

---

## 4. 下单 API

### 4.1 order_stock — 同步下单

```python
from bigqmt_signal_trader.xtquant_compat import STOCK_BUY, STOCK_SELL, FIX_PRICE, LATEST_PRICE

order_id = xt_trader.order_stock(
    acc,            # StockAccount
    stock_code,     # "600000.SH"
    order_type,     # STOCK_BUY(23) / STOCK_SELL(24)
    order_volume,   # int，委托数量
    price_type,     # FIX_PRICE(11) / LATEST_PRICE(5)
    price,          # float，限价单价格（最新价时传 0）
    strategy_name,  # str
    order_remark,   # str，user_order_id
)
```

- **返回**: `order_sys_id`(字符串) 或 `-1`(失败)
- **CLI**: `uv run python qmt.py buy 600000.SH 100 --price 7.50 [--strategy s] [--remark r]`
- **CLI**: `uv run python qmt.py sell 600000.SH 100 --price 7.50`
- **⚠️ 权限**: 服务端默认 `rpc_allow_order_methods=False`，必须显式开启才能下单
- **⚠️ 超时**: 超时后委托可能已提交，先查 `query_orders` 确认，避免重复下单

### 4.2 order_stock_async — 异步下单

```python
seq = xt_trader.order_stock_async(acc, code, order_type, vol, price_type, price, strategy, remark)
```

- **返回**: seq（结果通过 callback 回调）

### 4.3 order_stock_batch — 批量下单

```python
results = xt_trader.order_stock_batch(acc, orders, batch_id="")
# orders: list[dict]，每项含 stock_code/action/volume/price/price_type/strategy_name
```

- **上限**: 500 条/批

### 4.4 信用交易委托类型

| 常量 | 值 | 用途 |
|------|-----|------|
| CREDIT_BUY | 23 | 担保品买入 |
| CREDIT_SELL | 24 | 担保品卖出 |
| CREDIT_FIN_BUY | 27 | 融资买入 |
| CREDIT_SLO_SELL | 28 | 融券卖出 |
| CREDIT_BUY_SECU_REPAY | 29 | 买券还券 |
| CREDIT_DIRECT_SECU_REPAY | 30 | 直接还券 |
| CREDIT_SELL_SECU_REPAY | 31 | 卖券还款 |
| CREDIT_DIRECT_CASH_REPAY | 32 | 直接还款 |

---

## 5. 撤单 API

### 5.1 cancel_order_stock_sysid

```python
success = xt_trader.cancel_order_stock_sysid(acc, market, order_sysid)
# market: "SH" / "SZ" / ""
```

- **CLI**: `uv run python qmt.py cancel <order_sysid> --market SH`

### 5.2 cancel_order_stock

```python
success = xt_trader.cancel_order_stock(acc, order_id)
# 等价于 cancel_order_stock_sysid(acc, "", order_id)
```

---

## 6. 回调系统

```python
from bigqmt_signal_trader.xtquant_compat import XtQuantTraderCallback

class MyCallback(XtQuantTraderCallback):
    def on_stock_order(self, order): ...       # 委托变更
    def on_stock_trade(self, trade): ...        # 成交推送
    def on_order_error(self, error): ...        # 委托错误
    def on_cancel_error(self, error): ...       # 撤单错误
    def on_order_stock_async_response(self, resp): ...
    def on_account_status(self, status): ...

xt_trader.register_callback(MyCallback())
xt_trader.start()
xt_trader.connect()
xt_trader.subscribe(acc)
```

事件推送通过 Redis pubsub 频道:
- `bigqmt:exec:order:{account_id}`
- `bigqmt:exec:trade:{account_id}`
- `bigqmt:exec:order_error:{account_id}`
- `bigqmt:exec:cancel_error:{account_id}`

---

## 7. 关键陷阱速查

### 7.1 strategy_name 不匹配
- 下单用 `strategy_name="rpc_test"` → 查询用 `strategy_name="bigqmt_signal_trader"` → 返回空
- **解决**: 查询时传 `strategy_name=""` 返回全部，或保持一致

### 7.2 下单静默失败
- `passorder` 调用成功但委托没进系统（QMT 风控拒绝但没报错）
- **解决**: 服务端下单后等 0.5 秒查 `query_orders` 确认；检查返回的 `server_error` 字段

### 7.3 复权 K 线返回全 0
- 服务端缺原始数据时，前/后复权返回的 close 全是 0.0
- **解决**: 先 `download_history_data2` 下载原始数据（客户端有自愈机制）

### 7.4 Transport 不匹配
- 客户端 redis / 服务端 zmq → ping 超时
- **解决**: 两端 `transport` 字段保持一致

### 7.5 QMT 必须运行在实盘模式
- 模拟模式下委托进 QMT 界面但不在真实委托队列，`query_orders` 查不到
- `order_stock` 返回 -1，触发 `on_order_error`

### 7.6 整市场快照数据量大
- `get_full_tick(["SH"])` 返回 5000+ 股完整盘口
- **解决**: 启用 `full_tick_cache` 或增大超时（已自动设 30 秒）

### 7.7 全推行情是增量的
- `subscribe_whole_quote` 的大 QMT 回调只推有变化的品种
- **解决**: 订阅成功后客户端自动调一次 `get_full_tick` 打底

### 7.8 下单超时与重复下单
- `order_stock` 超时 → 委托可能已提交但没收到响应
- **解决**: 超时后先查 `query_orders`/`query_trades` 确认状态，再决定是否重试

---

## 8. 常量速查

### 交易常量

| 常量 | 值 | 用途 |
|------|-----|------|
| STOCK_BUY | 23 | 股票买入 |
| STOCK_SELL | 24 | 股票卖出 |
| FIX_PRICE | 11 | 限价/指定价 |
| LATEST_PRICE | 5 | 最新价 |
| MARKET_PEER_PRICE_FIRST | 44 | 对手方最优价 |

### 账号类型

| 常量 | 值 |
|------|-----|
| FUTURE_ACCOUNT | 1 |
| SECURITY_ACCOUNT | 2 |
| CREDIT_ACCOUNT | 3 |
| FUTURE_OPTION_ACCOUNT | 5 |
| STOCK_OPTION_ACCOUNT | 6 |

### 期货委托类型（部分）

| 常量 | 值 | 用途 |
|------|-----|------|
| FUTURE_OPEN_LONG | 0 | 开多 |
| FUTURE_CLOSE_LONG_TODAY | 2 | 平今多 |
| FUTURE_OPEN_SHORT | 3 | 开空 |
| FUTURE_CLOSE_SHORT_TODAY | 4 | 平今空 |
| FUTURE_CLOSE_LONG_HISTORY | 6 | 平昨多 |
| FUTURE_CLOSE_SHORT_HISTORY | 7 | 平昨空 |

---

## 9. 直接 RPC 调用（绕过兼容层）

当兼容层方法不够用时，可直接调 RPC：

```python
from bigqmt_signal_trader.redis_rpc import call_redis_rpc
import redis

r = redis.Redis(host="...", port=6379, db=5, password="...")
resp = call_redis_rpc(r, "ACCOUNT_ID", "get_full_tick", {"codes": ["000001.SZ"]})
print(resp["data"]["000001.SZ"]["lastPrice"])
```

- **万能入口**: `xtdata.call_method("get_float_caps", stockcode="000001.SZ")`
- **方法别名映射**:
  - `get_full_tick` → `get_ticks`
  - `get_instrument_detail` → `get_instrument`
  - `query_stock_asset` → `get_asset`
  - `query_stock_positions` → `get_positions`
  - `query_stock_orders` → `query_orders`
  - `query_stock_trades` → `query_trades`
  - `order_stock` → `submit_order`
  - `cancel_order_stock` → `cancel_order`

### RPC 响应结构

```json
{
    "ok": true,
    "data": {...},
    "error": "",
    "server_error": "",
    "handled_at": "2024-07-01 15:00:00"
}
```

- `ok=true`: `data` 为方法返回值（DataFrame 已序列化，客户端自动还原 pandas 对象）
- `ok=false`: `error` 为错误信息
- `server_error`: 额外诊断（如 passorder 提交但委托未进系统）

---

## 10. 可用 RPC 方法白名单（117 个只读 + 3 个下单/撤单）

### 行情快照
`get_ticks`/`get_full_tick`, `get_instrument`/`get_instrument_detail`, `get_instrument_type`,
`get_stock_name`, `get_stock_type`, `get_last_close`, `get_last_volume`, `get_float_caps`,
`get_total_share`, `get_turn_over_rate`, `get_weight_in_index`, `get_contract_multiplier`,
`get_contract_expire_date`, `get_open_date`, `get_svol`, `get_bvol`, `get_risk_free_rate`,
`is_stock_type`, `get_cb_info`

### K线/历史
`get_market_data`, `get_market_data_ex`, `get_local_data`, `get_close_price`, `get_index_weight`

### L2 行情（需 L2 权限）
`get_l2_quote`, `get_l2_order`, `get_l2_transaction`, `subscribe_l2thousand`

### 板块
`get_stock_list_in_sector`, `get_sector_list`, `get_sector_info`, `create_sector`, `add_sector`, `remove_sector`

### 交易日历/时段
`get_trading_dates`, `get_holidays`, `get_markets`, `get_market_last_trade_date`,
`get_date_location`, `get_trading_calendar`, `get_trade_times`

### 数据下载
`download_history_data`, `download_history_data2`, `download_holiday_data`,
`download_etf_info`, `download_cb_data`, `download_history_contracts`,
`download_index_weight`, `download_sector_data`

### 财务/因子
`get_financial_data`, `download_financial_data`, `download_financial_data2`,
`get_raw_financial_data`, `get_factor_data`

### ETF/期权/期货
`get_etf_info`, `get_ipo_info`, `get_option_list`, `get_his_option_list`,
`get_his_option_list_batch`, `get_option_detail_data`, `get_option_undl_data`,
`get_option_undl`, `get_ETF_list`, `get_main_contract`, `get_his_contract_list`

### 期权定价
`bsm_price`, `bsm_iv`, `get_option_iv`

客户端扩展（不走 RPC 数学计算）：

- `get_option_analytics(opt_code, option_price=None, underlying_price=None, as_of=None, risk_free_rate=None, dividend_yield=0.0, price_period="1m", include_native_iv=False)`：返回 IV、Delta/Gamma/Vega/Theta/Rho、内在/时间价值和明确的 Greek 单位。缺省价格取期权和标的最新 close；盘口中间价应通过显式价格传入。
- `get_option_chain_analytics(undl_code, dedate, ...)`：批量计算一个到期月份，返回 `valid_count` / `error_count` 和 `contracts`。无套利边界不成立或缺价的合约保留 `analytics_error`，不会让整条链失败。

CLI：`option-greeks 10010975.SHO`（单合约）或 `option-greeks 510050.SH --expiry 202609`（整条链）。

### 龙虎榜/股东
`get_longhubang`, `get_top10_share_holder`, `get_holder_num`, `get_turnover_rate`,
`get_industry`, `get_his_st_data`, `get_his_index_data`

### 资金流
`get_north_finance_change`, `get_hkt_statistics`, `get_hkt_details`, `get_hkt_exchange_rate`

### 因子/模型
`call_formula`, `subscribe_formula`, `unsubscribe_formula`, `get_formula_result`, `gen_factor_index`

### 时间转换（纯本地）
`datetime_to_timetag`, `timetag_to_datetime`

### 账户查询
`get_asset`, `get_positions`, `query_stock_position`, `query_orders`, `query_trades`,
`get_history_trade_detail_data`, `get_value_by_order_id`, `get_last_order_id`

### 融资融券（需两融权限）
`get_assure_contract`, `get_enable_short_contract`, `get_unclosed_compacts`,
`get_closed_compacts`, `get_debt_contract`

### 期权持仓
`get_option_subject_position`, `get_comb_option`

### 持仓同步
`sync_positions`

### 下单/撤单（需开启 rpc_allow_order_methods）
`submit_order`/`order_stock`, `submit_orders_batch`/`order_stock_batch`,
`cancel_order`/`cancel_order_stock`/`cancel_order_stock_sysid`

### 全推行情
`subscribe_whole_quote`, `unsubscribe_whole_quote`, `quote_keepalive`

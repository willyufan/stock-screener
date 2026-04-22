# Stock Screener

A股 / 港股通 左右侧信号筛选器，基于 Flask 的本地 Web 应用。

## 功能

- **股票筛选**：对 A 股全市场和沪深港通标的进行左侧/右侧信号评分，支持实时行情和历史日期扫描
- **批量扫描**：指定日期区间一次性扫描多个交易日，自动跳过非交易日
- **扫描历史**：按日期浏览历史扫描结果，对比信号变化
- **自选股**：维护自选列表，随时查看最新信号
- **自动策略**：内置 Kelly 仓位信号策略，可配置触发条件自动推送候选股
- **回测**：对任意策略在历史数据上逐日回测，评估胜率与收益
- **DCF 估值**：现金流折现估值计算器，支持保存与历史对比

## 数据来源

| 用途 | 来源 |
|------|------|
| A 股日线行情 | akshare（东方财富）|
| 港股通成分股 | akshare（7 天本地缓存）|
| 港股日线 K 线 | akshare → 新浪港股（直连，无频率限制）|
| 交易日历 | Tushare `trade_cal`（精确到节假日）|
| 分钟行情 | Tushare `stk_mins` |

## 环境要求

- Python 3.10+
- Tushare token（日线接口需 2000 积分，分钟接口需分钟权限）

## 快速开始

### 1. 配置 Token

```bash
cp config.example.py config.py
```

编辑 `config.py`，填入 Tushare token：

```python
TUSHARE_TOKEN_DAILY  = "your_daily_token"
TUSHARE_TOKEN_MINUTE = "your_minute_token"
```

也可以用环境变量（优先级更高）：

```bash
export TUSHARE_TOKEN_DAILY=your_daily_token
export TUSHARE_TOKEN_MINUTE=your_minute_token
```

> 在 [tushare.pro](https://tushare.pro) 注册后获取 token。`config.py` 已加入 `.gitignore`，不会提交到 git。

### 2. 启动

**首次运行**（自动创建虚拟环境并安装依赖）：

```bash
bash run.sh
```

**日常启动**（依赖已装好，直接启动）：

```bash
source .venv/bin/activate
python app.py
```

> `run.sh` 会检测 `requirements.txt` 是否有变更，只在需要时才重装依赖，之后直接用上面的命令更快。

启动后访问 [http://localhost:5005](http://localhost:5005)。

## 目录结构

```
stock-screener/
├── app.py                  # Flask 主应用，API 路由，定时刷新
├── data_fetcher.py         # 行情数据获取（A股/港股）
├── strategy.py             # 虚拟交易策略引擎
├── backtest.py             # 回测引擎
├── dcf_calc.py             # DCF 估值计算
├── auto_strategy/          # 自动策略（Kelly 信号等）
├── templates/
│   ├── index.html          # 主界面
│   └── dcf.html            # DCF 估值页
├── data/                   # 运行时数据（历史扫描、策略、自选等）
├── config.example.py       # Token 配置模板
├── config.py               # 本地配置（gitignored）
├── requirements.txt
└── run.sh                  # 一键启动脚本
```

## 自动刷新

应用启动后，APScheduler 会在每日收盘后自动触发数据刷新：
- A 股：11:30 / 15:00（北京时间）
- 港股：12:00 / 16:00（北京时间）

也可在界面上手动触发扫描或指定历史日期扫描。

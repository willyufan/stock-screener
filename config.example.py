# 复制此文件为 config.py 并填入你的 Token
# Copy this file to config.py and fill in your tokens
#
# 或使用环境变量（优先级高于 config.py，推荐生产环境使用）：
#   export TUSHARE_TOKEN_DAILY=your_daily_token
#   export TUSHARE_TOKEN_MINUTE=your_minute_token

# Tushare token：在 https://tushare.pro 注册后获取
# DAILY  token：用于 daily / daily_basic / stock_basic 等日线接口（2000积分以上）
# MINUTE token：用于 stk_mins 分钟级接口（需要分钟行情权限）
TUSHARE_TOKEN_DAILY  = ""
TUSHARE_TOKEN_MINUTE = ""

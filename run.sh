#!/bin/bash
set -e
cd "$(dirname "$0")"

# 首次运行：创建虚拟环境
if [ ! -d ".venv" ]; then
  echo "创建虚拟环境..."
  python3 -m venv .venv
fi

source .venv/bin/activate

# 仅在 requirements.txt 有变更时才重新安装依赖
REQ_HASH_FILE=".venv/.req_hash"
REQ_HASH=$(md5 -q requirements.txt 2>/dev/null || md5sum requirements.txt 2>/dev/null | awk '{print $1}')
if [ ! -f "$REQ_HASH_FILE" ] || [ "$(cat $REQ_HASH_FILE)" != "$REQ_HASH" ]; then
  echo "安装/更新依赖..."
  pip install -q -r requirements.txt
  echo "$REQ_HASH" > "$REQ_HASH_FILE"
fi

echo "启动 stock-screener，访问 http://localhost:5005"
python app.py

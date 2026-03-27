#!/bin/bash
set -e
cd "$(dirname "$0")"

# 安装依赖（首次运行）
if [ ! -d ".venv" ]; then
  echo "创建虚拟环境..."
  python3 -m venv .venv
fi

source .venv/bin/activate

echo "安装/更新依赖..."
pip install -q -r requirements.txt

echo "启动 stock-screener，访问 http://localhost:5005"
python app.py

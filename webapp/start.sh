#!/bin/bash
# 启动 Flask Web 应用

cd "$(dirname "$0")"

echo "========================================"
echo "  电商推荐系统 - Flask Web 应用"
echo "========================================"
echo ""

# 设置 Spark/Hadoop 环境变量
export SPARK_HOME=${SPARK_HOME:-/opt/spark}
export HADOOP_HOME=${HADOOP_HOME:-/opt/hadoop}
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export YARN_CONF_DIR=${YARN_CONF_DIR:-$HADOOP_HOME/etc/hadoop}
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

# 检查 Python 环境
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 python3"
    exit 1
fi

# 激活虚拟环境
if [ -d "venv" ]; then
    source venv/bin/activate
else
    echo "创建虚拟环境..."
    python3 -m venv venv
    source venv/bin/activate
    pip install flask pyspark numpy pandas
fi

echo ""
echo "启动 Flask 服务..."
echo "访问地址: http://localhost:5000"
echo ""
echo "按 Ctrl+C 停止服务"
echo "----------------------------------------"
echo ""

# 启动应用
python run.py

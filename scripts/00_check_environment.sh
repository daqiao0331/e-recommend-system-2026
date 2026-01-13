#!/bin/bash
# ============================================
# 环境检查脚本 - 检查Hadoop和Spark集群状态
# ============================================

echo "=========================================="
echo "       电子商务推荐系统 - 环境检查        "
echo "=========================================="

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 检查函数
check_service() {
    if $1 &> /dev/null; then
        echo -e "${GREEN}[✓]${NC} $2"
        return 0
    else
        echo -e "${RED}[✗]${NC} $2"
        return 1
    fi
}

echo ""
echo "1. 检查Hadoop环境..."
echo "-------------------------------------------"

# 检查HADOOP_HOME
if [ -n "$HADOOP_HOME" ]; then
    echo -e "${GREEN}[✓]${NC} HADOOP_HOME = $HADOOP_HOME"
else
    echo -e "${RED}[✗]${NC} HADOOP_HOME 未设置"
fi

# 检查Hadoop版本
hadoop version 2>/dev/null | head -1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} Hadoop 命令可用"
else
    echo -e "${RED}[✗]${NC} Hadoop 命令不可用"
fi

# 检查HDFS
echo ""
echo "2. 检查HDFS状态..."
echo "-------------------------------------------"
hdfs dfsadmin -report 2>/dev/null | head -20
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} HDFS 运行正常"
else
    echo -e "${YELLOW}[!]${NC} HDFS 可能未启动，请运行: start-dfs.sh"
fi

# 检查YARN
echo ""
echo "3. 检查YARN状态..."
echo "-------------------------------------------"
yarn node -list 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} YARN 运行正常"
else
    echo -e "${YELLOW}[!]${NC} YARN 可能未启动，请运行: start-yarn.sh"
fi

echo ""
echo "4. 检查Spark环境..."
echo "-------------------------------------------"

# 检查SPARK_HOME
if [ -n "$SPARK_HOME" ]; then
    echo -e "${GREEN}[✓]${NC} SPARK_HOME = $SPARK_HOME"
else
    echo -e "${RED}[✗]${NC} SPARK_HOME 未设置"
fi

# 检查Spark版本
spark-submit --version 2>&1 | head -5
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} Spark 命令可用"
else
    echo -e "${RED}[✗]${NC} Spark 命令不可用"
fi

# 检查PySpark
echo ""
echo "5. 检查PySpark..."
echo "-------------------------------------------"
# 优先使用虚拟环境的 python
PYTHON_CMD="python3"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
if [ -f "$SCRIPT_DIR/venv/bin/python" ]; then
    PYTHON_CMD="$SCRIPT_DIR/venv/bin/python"
fi
$PYTHON_CMD -c "import pyspark; print(f'PySpark版本: {pyspark.__version__}')" 2>/dev/null
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} PySpark 可用"
else
    echo -e "${YELLOW}[!]${NC} PySpark 未安装，请运行: pip3 install pyspark"
fi

echo ""
echo "6. 检查Hive环境..."
echo "-------------------------------------------"
hive --version 2>/dev/null | head -1
if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓]${NC} Hive 可用"
else
    echo -e "${YELLOW}[!]${NC} Hive 可能未安装或未配置"
fi

echo ""
echo "7. 集群节点检查..."
echo "-------------------------------------------"
echo "DataNode 节点列表:"
hdfs dfsadmin -report 2>/dev/null | grep "Name:" | awk '{print $2}'

echo ""
echo "=========================================="
echo "           环境检查完成                   "
echo "=========================================="
echo ""
echo "如果有服务未启动，请执行以下命令:"
echo "  启动HDFS:  start-dfs.sh"
echo "  启动YARN:  start-yarn.sh"
echo "  启动全部:  start-all.sh"
echo ""

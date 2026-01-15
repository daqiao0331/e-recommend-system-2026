#!/bin/bash
set -euo pipefail
# ============================================
# 一键运行脚本 - 电子商务推荐系统
# ============================================

echo "=========================================="
echo "   电子商务推荐系统 - 一键部署运行脚本    "
echo "=========================================="

# 项目目录（自动使用脚本所在目录，避免硬编码路径）
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

# 如果存在虚拟环境，自动激活
if [ -f "$PROJECT_DIR/venv/bin/activate" ]; then
    source "$PROJECT_DIR/venv/bin/activate"
    echo "已激活虚拟环境: $VIRTUAL_ENV"
fi

# 确保 Hive 环境变量可用
export HIVE_HOME="${HIVE_HOME:-/home/daqiao/apache-hive-3.1.3-bin}"
export PATH="$PATH:$HIVE_HOME/bin"

# 配置 PySpark 使用系统 Python（确保所有节点路径一致）
export PYSPARK_PYTHON=/usr/bin/python3
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3

# 配置 HDFS 基础路径 (使用当前用户，避免权限问题)
export HDFS_BASE="/user/$(whoami)/ecommerce"
echo "使用 HDFS 路径: $HDFS_BASE"

# 步骤1: 环境检查
echo ""
echo "[Step 1/7] 检查环境..."
./scripts/00_check_environment.sh
read -p "环境检查通过？(y/n): " env_ok
if [ "$env_ok" != "y" ]; then
    echo "请先解决环境问题后再运行"
    exit 1
fi

# 步骤2: 上传数据
echo ""
echo "[Step 2/7] 上传数据到HDFS..."
./scripts/01_upload_to_hdfs.sh

# 步骤3: 创建Hive表
echo ""
echo "[Step 3/7] 创建Hive表..."
hive --hivevar HDFS_BASE="${HDFS_BASE}" -f sql/01_create_tables.sql 2>&1 | grep -v "WARN" || true
echo "Hive表创建/更新完成"

# 步骤4: Hive数据分析 (可选，跳过以加快速度)
echo ""
echo "[Step 4/7] 跳过 Hive 数据分析（将由 Spark 处理）..."
# hive --hivevar HDFS_BASE="${HDFS_BASE}" -f sql/02_data_analysis.sql
echo "[提示] 如需运行 Hive 分析，请手动执行: hive -f sql/02_data_analysis.sql"

# 步骤5: Spark数据预处理
echo ""
echo "[Step 5/7] 运行Spark数据预处理..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --num-executors 2 \
    --executor-cores 2 \
    --conf "spark.driver.extraJavaOptions=-Xss4m" \
    --conf "spark.executor.extraJavaOptions=-Xss4m" \
    spark/02_data_preprocessing.py

# 步骤6: 训练ALS模型
echo ""
echo "[Step 6/7] 训练ALS推荐模型..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 4g \
    --executor-memory 4g \
    --num-executors 2 \
    --executor-cores 2 \
    --conf "spark.driver.extraJavaOptions=-Xss4m" \
    --conf "spark.executor.extraJavaOptions=-Xss4m" \
    spark/03_train_als_model.py

# 步骤7: 可视化
echo ""
echo "[Step 7/7] 生成可视化图表..."
spark-submit \
    --master yarn \
    --deploy-mode client \
    --driver-memory 2g \
    --executor-memory 2g \
    --conf "spark.driver.extraJavaOptions=-Xss4m" \
    --conf "spark.executor.extraJavaOptions=-Xss4m" \
    spark/04_visualization.py

echo ""
echo "=========================================="
echo "           全部任务完成！                 "
echo "=========================================="
echo ""
echo "推荐结果: ecommerce.user_recommendations"
echo "可视化图表: output/figures/"
echo ""

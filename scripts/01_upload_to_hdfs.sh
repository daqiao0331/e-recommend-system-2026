#!/bin/bash
# ============================================
# 数据上传脚本 - 将原始数据上传到HDFS
# ============================================

echo "=========================================="
echo "       电子商务推荐系统 - 数据上传        "
echo "=========================================="

# 项目根目录（根据实际情况修改）
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data"

# HDFS目标路径
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/ecommerce}"
HDFS_RAW="$HDFS_BASE/raw_data"
HDFS_PROCESSED="$HDFS_BASE/processed"
HDFS_MODEL="$HDFS_BASE/model"
HDFS_OUTPUT="$HDFS_BASE/output"

echo ""
echo "Step 1: 创建HDFS目录结构..."
echo "-------------------------------------------"

hdfs dfs -mkdir -p $HDFS_RAW/events
hdfs dfs -mkdir -p $HDFS_RAW/item_properties  
hdfs dfs -mkdir -p $HDFS_RAW/category_tree
hdfs dfs -mkdir -p $HDFS_PROCESSED
hdfs dfs -mkdir -p $HDFS_MODEL
hdfs dfs -mkdir -p $HDFS_OUTPUT

echo "[✓] HDFS目录创建完成"

echo ""
echo "Step 2: 上传原始数据文件（跳过已存在）..."
echo "-------------------------------------------"

# 上传用户行为数据
if [ -f "$DATA_DIR/events.csv" ]; then
    if hdfs dfs -test -e $HDFS_RAW/events/events.csv; then
        echo "[✓] events.csv 已存在，跳过上传"
    else
        hdfs dfs -put $DATA_DIR/events.csv $HDFS_RAW/events/
        echo "[✓] events.csv 上传成功"
    fi
else
    echo "[!] events.csv 不存在，请先下载数据"
fi

# 上传商品属性数据（如果文件较大，可能分成多个part）
if ls $DATA_DIR/item_properties*.csv 1> /dev/null 2>&1; then
    # 检查是否已有文件
    if hdfs dfs -test -d $HDFS_RAW/item_properties && hdfs dfs -ls $HDFS_RAW/item_properties/*.csv 1> /dev/null 2>&1; then
        echo "[✓] item_properties 已存在，跳过上传"
    else
        hdfs dfs -put $DATA_DIR/item_properties*.csv $HDFS_RAW/item_properties/
        echo "[✓] item_properties 上传成功"
    fi
else
    echo "[!] item_properties 文件不存在"
fi

# 上传类目树数据
if [ -f "$DATA_DIR/category_tree.csv" ]; then
    if hdfs dfs -test -e $HDFS_RAW/category_tree/category_tree.csv; then
        echo "[✓] category_tree.csv 已存在，跳过上传"
    else
        hdfs dfs -put $DATA_DIR/category_tree.csv $HDFS_RAW/category_tree/
        echo "[✓] category_tree.csv 上传成功"
    fi
else
    echo "[!] category_tree.csv 不存在"
fi

echo ""
echo "Step 3: 验证上传结果..."
echo "-------------------------------------------"

echo "HDFS目录结构:"
hdfs dfs -ls -R $HDFS_BASE

echo ""
echo "文件大小统计:"
hdfs dfs -du -h $HDFS_RAW

echo ""
echo "=========================================="
echo "           数据上传完成                   "
echo "=========================================="
echo ""
echo "下一步: 运行 Hive 建表脚本"
echo "  hive -f sql/01_create_tables.sql"
echo ""

#!/bin/bash
# ============================================
# 下载ALS模型到本地
# ============================================

echo "=========================================="
echo "    下载训练好的ALS模型到本地"
echo "=========================================="

# 项目根目录
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOCAL_MODEL_DIR="$PROJECT_DIR/model"

# HDFS路径
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/ecommerce}"
HDFS_MODEL_BASE="$HDFS_BASE/model"

echo ""
echo "Step 1: 查找最新的模型..."
echo "-------------------------------------------"

# 获取最新的模型路径
LATEST_MODEL=$(hdfs dfs -ls -t $HDFS_MODEL_BASE | grep "als_model_" | head -1 | awk '{print $8}')

if [ -z "$LATEST_MODEL" ]; then
    echo "[!] 未找到训练好的模型"
    echo "    请先运行训练脚本: ./run_all.sh"
    exit 1
fi

echo "[✓] 找到最新模型: $LATEST_MODEL"

echo ""
echo "Step 2: 创建本地model目录..."
echo "-------------------------------------------"

mkdir -p "$LOCAL_MODEL_DIR"
echo "[✓] 本地目录: $LOCAL_MODEL_DIR"

echo ""
echo "Step 3: 下载模型文件..."
echo "-------------------------------------------"

# 提取模型名称
MODEL_NAME=$(basename $LATEST_MODEL)
LOCAL_PATH="$LOCAL_MODEL_DIR/$MODEL_NAME"

# 如果本地已存在，先删除
if [ -d "$LOCAL_PATH" ]; then
    echo "本地已存在旧模型，删除中..."
    rm -rf "$LOCAL_PATH"
fi

# 下载模型
hdfs dfs -get $LATEST_MODEL $LOCAL_MODEL_DIR/

if [ $? -eq 0 ]; then
    echo "[✓] 模型下载成功: $LOCAL_PATH"
else
    echo "[!] 模型下载失败"
    exit 1
fi

echo ""
echo "Step 4: 下载ID映射表..."
echo "-------------------------------------------"

# 下载用户ID映射
MAPPING_DIR="$LOCAL_MODEL_DIR/mappings"
mkdir -p "$MAPPING_DIR"

hdfs dfs -get $HDFS_BASE/hive_warehouse/user_id_mapping $MAPPING_DIR/ 2>/dev/null && \
    echo "[✓] 用户ID映射下载成功"

hdfs dfs -get $HDFS_BASE/hive_warehouse/item_id_mapping $MAPPING_DIR/ 2>/dev/null && \
    echo "[✓] 商品ID映射下载成功"

echo ""
echo "=========================================="
echo "           模型下载完成！"
echo "=========================================="
echo ""
echo "模型位置: $LOCAL_PATH"
echo "ID映射位置: $MAPPING_DIR"
echo ""
echo "模型大小:"
du -sh "$LOCAL_PATH"
echo ""


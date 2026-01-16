#!/bin/bash
set -e
# ============================================
# 数据上传脚本 - 将原始数据上传到HDFS (支持本地及HDFS模式)
# ============================================

echo "=========================================="
echo "       电子商务推荐系统 - 数据上传        "
echo "=========================================="

# 项目根目录
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data"

# HDFS目标路径
HDFS_BASE="${HDFS_BASE:-/user/$(whoami)/ecommerce}"

# 检测模式
IS_LOCAL=0
if [[ "$HDFS_BASE" == file://* ]]; then
    IS_LOCAL=1
    LOCAL_BASE="${HDFS_BASE#file://}"
    echo "Mode: Local Filesystem ($LOCAL_BASE)"
else
    echo "Mode: HDFS ($HDFS_BASE)"
fi

HDFS_RAW="$HDFS_BASE/raw_data"
HDFS_PROCESSED="$HDFS_BASE/processed"
HDFS_MODEL="$HDFS_BASE/model"
HDFS_OUTPUT="$HDFS_BASE/output"

# 工具函数
fs_mkdir() {
    path=$1
    if [ $IS_LOCAL -eq 1 ]; then
        local_path="${path#file://}"
        echo "mkdir -p $local_path"
        mkdir -p "$local_path"
    else
        hdfs dfs -mkdir -p "$path"
    fi
}

fs_put_glob() {
    # 专门处理通配符的情况
    # 参数1: 源目录
    # 参数2: 文件名模式（如 *.csv）
    # 参数3: 目标目录
    
    src_dir=$1
    pattern=$2
    dst_dir=$3
    
    echo "Uploading $pattern from $src_dir to $dst_dir"
    
    if [ $IS_LOCAL -eq 1 ]; then
        local_dst_dir="${dst_dir#file://}"
        mkdir -p "$local_dst_dir"
        
        # 使用 find 找到文件并复制，这是最安全的方式
        find "$src_dir" -maxdepth 1 -name "$pattern" -exec cp -f {} "$local_dst_dir" \;
    else
        # HDFS put 支持通配符
        hdfs dfs -put -f "$src_dir/$pattern" "$dst_dir"
    fi
}

fs_put_file() {
    # 专门处理当个文件
    src=$1
    dst_dir=$2
    
    if [ $IS_LOCAL -eq 1 ]; then
        local_dst_dir="${dst_dir#file://}"
        mkdir -p "$local_dst_dir"
        cp -f "$src" "$local_dst_dir"
    else
        hdfs dfs -put -f "$src" "$dst_dir"
    fi
}

echo ""
echo "Step 1: 创建目录结构..."
echo "-------------------------------------------"

fs_mkdir "$HDFS_RAW/events"
fs_mkdir "$HDFS_RAW/item_properties"  
fs_mkdir "$HDFS_RAW/category_tree"
fs_mkdir "$HDFS_PROCESSED"
fs_mkdir "$HDFS_MODEL"
fs_mkdir "$HDFS_OUTPUT"

echo "[✓] 目录结构已准备"

echo ""
echo "Step 2: 上传原始数据文件..."
echo "-------------------------------------------"

# 上传用户行为数据
if [ -f "$DATA_DIR/events.csv" ]; then
    fs_put_file "$DATA_DIR/events.csv" "$HDFS_RAW/events/"
    echo "[✓] events.csv 上传成功"
else
    echo "[!] events.csv 不存在，请先下载数据"
    exit 1
fi

# 上传商品属性数据
# 检测是否存在匹配文件
count=$(find "$DATA_DIR" -maxdepth 1 -name "item_properties*.csv" | wc -l)
if [ "$count" != "0" ]; then
    fs_put_glob "$DATA_DIR" "item_properties*.csv" "$HDFS_RAW/item_properties/"
    echo "[✓] item_properties ($count files) 上传成功"
else
    echo "[!] item_properties 文件不存在"
fi

# 上传类目树数据
if [ -f "$DATA_DIR/category_tree.csv" ]; then
    fs_put_file "$DATA_DIR/category_tree.csv" "$HDFS_RAW/category_tree/"
    echo "[✓] category_tree.csv 上传成功"
else
    echo "[!] category_tree.csv 不存在"
fi

echo ""
echo "Step 3: 验证结果..."
echo "-------------------------------------------"

if [ $IS_LOCAL -eq 1 ]; then
    echo "本地目录文件数统计 ($LOCAL_BASE):"
    find "$LOCAL_BASE" -type f | wc -l
else
    echo "HDFS目录结构:"
    hdfs dfs -ls -R $HDFS_BASE
fi

echo ""
echo "=========================================="
echo "           数据上传完成                   "
echo "=========================================="

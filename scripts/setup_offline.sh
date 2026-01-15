#!/bin/bash
# ============================================
# 使用本地模型进行推荐（无需连接Hive）
# ============================================

chmod +x scripts/download_model.sh
./scripts/download_model.sh

echo ""
echo "下载完成！现在可以离线使用模型了。"
echo ""
echo "使用方式:"
echo "  1. 在线推荐（需要Hive表）:"
echo "     spark-submit spark/recommend.py 257597"
echo ""
echo "  2. 离线推荐（使用本地模型）:"
echo "     python spark/recommend_offline.py"
echo ""

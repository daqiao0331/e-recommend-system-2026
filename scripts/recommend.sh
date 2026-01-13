#!/bin/bash
# 推荐查询脚本 - 使用Hive直接查询
# 使用: ./scripts/recommend.sh <user_id>

if [ $# -lt 1 ]; then
    echo "使用方式: ./scripts/recommend.sh <user_id>"
    echo "示例: ./scripts/recommend.sh 257597"
    exit 1
fi

USER_ID=$1

echo "========================================"
echo "  用户 $USER_ID 的画像和推荐"
echo "========================================"

# 用户历史
echo ""
echo "【用户历史行为】"
hive -e "
SELECT event_type, COUNT(*) as count, COUNT(DISTINCT item_id) as unique_items
FROM ecommerce.user_events
WHERE visitor_id = '$USER_ID'
GROUP BY event_type
ORDER BY count DESC;
" 2>/dev/null | grep -v "WARN\|INFO\|Time taken"

# 最近浏览
echo ""
echo "【最近浏览商品】"
hive -e "
SELECT item_id, event_type, from_unixtime(event_time/1000) as time
FROM ecommerce.user_events
WHERE visitor_id = '$USER_ID'
ORDER BY event_time DESC
LIMIT 5;
" 2>/dev/null | grep -v "WARN\|INFO\|Time taken"

echo ""
echo "【推荐商品】"
echo "提示: 需要先运行完整训练流程生成推荐"
echo ""
echo "或者运行 Python 脚本获取实时推荐:"
echo "  spark-submit spark/05_recommend_predict.py --user $USER_ID"
echo ""

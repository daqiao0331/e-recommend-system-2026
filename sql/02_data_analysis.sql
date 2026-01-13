-- ============================================
-- Hive数据分析脚本 - 电子商务推荐系统
-- 执行方式: hive -f 02_data_analysis.sql
-- ============================================

USE ecommerce;

-- ============================================
-- 1. 基础数据统计
-- ============================================

-- 1.1 数据总量统计
SELECT '========== 数据总量统计 ==========' AS info;

SELECT 
    COUNT(*) AS total_events,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    COUNT(DISTINCT item_id) AS unique_items
FROM user_events;

-- 1.2 事件类型分布
SELECT '========== 事件类型分布 ==========' AS info;

SELECT 
    event_type,
    COUNT(*) AS event_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM user_events
GROUP BY event_type
ORDER BY event_count DESC;

-- ============================================
-- 2. 用户行为分析
-- ============================================

-- 2.1 用户活跃度分布
SELECT '========== 用户活跃度分布 ==========' AS info;

SELECT 
    activity_level,
    COUNT(*) AS user_count
FROM (
    SELECT 
        visitor_id,
        CASE 
            WHEN COUNT(*) >= 100 THEN '高活跃(100+)'
            WHEN COUNT(*) >= 50 THEN '中活跃(50-99)'
            WHEN COUNT(*) >= 10 THEN '低活跃(10-49)'
            ELSE '新用户(<10)'
        END AS activity_level
    FROM user_events
    GROUP BY visitor_id
) t
GROUP BY activity_level
ORDER BY user_count DESC;

-- 2.2 用户转化漏斗
SELECT '========== 用户转化漏斗 ==========' AS info;

SELECT 
    'Step1_浏览' AS stage,
    COUNT(DISTINCT visitor_id) AS users
FROM user_events 
WHERE event_type = 'view'

UNION ALL

SELECT 
    'Step2_加购' AS stage,
    COUNT(DISTINCT visitor_id) AS users
FROM user_events 
WHERE event_type = 'addtocart'

UNION ALL

SELECT 
    'Step3_购买' AS stage,
    COUNT(DISTINCT visitor_id) AS users
FROM user_events 
WHERE event_type = 'transaction';

-- ============================================
-- 3. 商品分析
-- ============================================

-- 3.1 热门商品TOP20(按浏览量)
SELECT '========== 热门商品TOP20 ==========' AS info;

SELECT 
    item_id,
    COUNT(*) AS view_count,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM user_events
WHERE event_type = 'view'
GROUP BY item_id
ORDER BY view_count DESC
LIMIT 20;

-- 3.2 转化率最高的商品TOP20
SELECT '========== 高转化商品TOP20 ==========' AS info;

SELECT 
    item_id,
    view_cnt,
    buy_cnt,
    ROUND(buy_cnt * 100.0 / view_cnt, 2) AS conversion_rate
FROM (
    SELECT 
        item_id,
        SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS view_cnt,
        SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS buy_cnt
    FROM user_events
    GROUP BY item_id
    HAVING SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) >= 100
) t
ORDER BY conversion_rate DESC
LIMIT 20;

-- ============================================
-- 4. 时间维度分析
-- ============================================

-- 4.1 按日期统计
SELECT '========== 每日事件统计 ==========' AS info;

SELECT 
    FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'yyyy-MM-dd') AS event_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT visitor_id) AS dau,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS views,
    SUM(CASE WHEN event_type = 'addtocart' THEN 1 ELSE 0 END) AS add_to_carts,
    SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS transactions
FROM user_events
GROUP BY FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'yyyy-MM-dd')
ORDER BY event_date;

-- 4.2 按小时统计(用户活跃时段)
SELECT '========== 小时活跃分布 ==========' AS info;

SELECT 
    HOUR(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT))) AS hour_of_day,
    COUNT(*) AS event_count,
    COUNT(DISTINCT visitor_id) AS unique_visitors
FROM user_events
GROUP BY HOUR(FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT)))
ORDER BY hour_of_day;

-- ============================================
-- 5. 生成每日统计汇总并插入结果表
-- ============================================

INSERT OVERWRITE TABLE daily_statistics
SELECT 
    FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'yyyy-MM-dd') AS stat_date,
    COUNT(*) AS total_events,
    COUNT(DISTINCT visitor_id) AS unique_visitors,
    SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS view_count,
    SUM(CASE WHEN event_type = 'addtocart' THEN 1 ELSE 0 END) AS cart_count,
    SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) AS buy_count,
    ROUND(
        SUM(CASE WHEN event_type = 'transaction' THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END), 0), 
        4
    ) AS conversion_rate
FROM user_events
GROUP BY FROM_UNIXTIME(CAST(event_time/1000 AS BIGINT), 'yyyy-MM-dd');

SELECT '========== 统计结果已保存到 daily_statistics 表 ==========' AS info;

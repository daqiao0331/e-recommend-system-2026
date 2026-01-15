-- ============================================
-- Hive建表脚本 - 电子商务推荐系统
-- 执行方式: hive -f 01_create_tables.sql
-- ============================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS ecommerce
COMMENT '电子商务推荐系统数据仓库'
LOCATION '${HDFS_BASE}/hive_warehouse';

USE ecommerce;

-- ============================================
-- 1. 用户行为事件表 (核心表)
-- ============================================
DROP TABLE IF EXISTS user_events;

CREATE EXTERNAL TABLE user_events (
    event_time      BIGINT      COMMENT '事件时间戳(毫秒)',
    visitor_id      STRING      COMMENT '访客ID',
    event_type      STRING      COMMENT '事件类型: view/addtocart/transaction',
    item_id         STRING      COMMENT '商品ID',
    transaction_id  STRING      COMMENT '交易ID(仅transaction事件有值)'
)
COMMENT '用户行为事件表 - 记录浏览、加购、购买行为'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw_data/events'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ============================================
-- 2. 商品属性表
-- ============================================
DROP TABLE IF EXISTS item_properties;

CREATE EXTERNAL TABLE item_properties (
    property_time   BIGINT      COMMENT '属性记录时间戳',
    item_id         STRING      COMMENT '商品ID',
    property_name   STRING      COMMENT '属性名称',
    property_value  STRING      COMMENT '属性值'
)
COMMENT '商品属性表 - 记录商品的各类属性'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw_data/item_properties'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ============================================
-- 3. 商品类目树表
-- ============================================
DROP TABLE IF EXISTS category_tree;

CREATE EXTERNAL TABLE category_tree (
    category_id     STRING      COMMENT '类目ID',
    parent_id       STRING      COMMENT '父类目ID'
)
COMMENT '商品类目树 - 记录类目层级关系'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${HDFS_BASE}/raw_data/category_tree'
TBLPROPERTIES (
    'skip.header.line.count'='1',
    'serialization.null.format'=''
);

-- ============================================
-- 4. 用户商品评分表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS user_item_ratings;

CREATE TABLE IF NOT EXISTS user_item_ratings (
    visitor_id          STRING      COMMENT '访客ID',
    item_id             STRING      COMMENT '商品ID',
    rating              DOUBLE      COMMENT '隐式评分',
    interaction_count   INT         COMMENT '交互次数',
    max_event           STRING      COMMENT '最高级别事件',
    has_addtocart       INT         COMMENT '是否有加购行为(0/1)',
    first_interaction   DATE        COMMENT '首次交互日期',
    last_interaction    DATE        COMMENT '最后交互日期',
    rating_normalized   DOUBLE      COMMENT '对数标准化评分'
)
COMMENT '用户商品评分矩阵 - 用于推荐模型训练(基于view预测addtocart)'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/processed/user_item_ratings';

-- ============================================
-- 5. 用户特征表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS user_features;

CREATE TABLE IF NOT EXISTS user_features (
    visitor_id      STRING      COMMENT '访客ID',
    total_events    BIGINT      COMMENT '总事件数',
    view_count      BIGINT      COMMENT '浏览次数',
    cart_count      BIGINT      COMMENT '加购次数',
    buy_count       BIGINT      COMMENT '购买次数',
    unique_items    BIGINT      COMMENT '交互商品数',
    active_days     BIGINT      COMMENT '活跃天数',
    first_visit     DATE        COMMENT '首次访问日期',
    last_visit      DATE        COMMENT '最后访问日期',
    cart_rate       DOUBLE      COMMENT '加购转化率(cart/view)',
    buy_rate        DOUBLE      COMMENT '购买转化率(buy/view)'
)
COMMENT '用户行为特征表 - 用于冷启动和用户分析'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/processed/user_features';

-- ============================================
-- 6. 商品特征表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS item_features;

CREATE TABLE IF NOT EXISTS item_features (
    item_id         STRING      COMMENT '商品ID',
    total_events    BIGINT      COMMENT '总事件数',
    view_count      BIGINT      COMMENT '浏览次数',
    cart_count      BIGINT      COMMENT '加购次数',
    buy_count       BIGINT      COMMENT '购买次数',
    unique_visitors BIGINT      COMMENT '独立访客数',
    cart_rate       DOUBLE      COMMENT '加购转化率(cart/view)',
    conversion_rate DOUBLE      COMMENT '购买转化率(buy/view)'
)
COMMENT '商品特征表 - 用于冷启动和商品分析'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/processed/item_features';

-- ============================================
-- 7. 推荐结果表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS user_recommendations;

CREATE TABLE IF NOT EXISTS user_recommendations (
    user_id         INT         COMMENT '用户索引ID',
    item_id         INT         COMMENT '商品索引ID', 
    score           DOUBLE      COMMENT '推荐分数'
)
COMMENT '用户推荐结果表'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/output/recommendations';

-- ============================================
-- 8. 完整推荐结果表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS user_recommendations_full;

CREATE TABLE IF NOT EXISTS user_recommendations_full (
    visitor_id       STRING      COMMENT '原始访客ID',
    recommended_item STRING      COMMENT '推荐商品ID',
    score            DOUBLE      COMMENT '推荐分数',
    user_id          INT         COMMENT '用户索引ID',
    item_id          INT         COMMENT '商品索引ID'
)
COMMENT '完整推荐结果表(含原始ID)'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/output/recommendations_full';

-- ============================================
-- 9. ID映射表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS user_id_mapping;

CREATE TABLE IF NOT EXISTS user_id_mapping (
    user_id          INT         COMMENT '用户索引ID',
    original_user_id STRING      COMMENT '原始访客ID'
)
COMMENT '用户ID映射表'
STORED AS PARQUET;

DROP TABLE IF EXISTS item_id_mapping;

CREATE TABLE IF NOT EXISTS item_id_mapping (
    item_id          INT         COMMENT '商品索引ID',
    original_item_id STRING      COMMENT '原始商品ID'
)
COMMENT '商品ID映射表'
STORED AS PARQUET;

-- ============================================
-- 10. 模型评估指标表 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS model_metrics;

CREATE TABLE IF NOT EXISTS model_metrics (
    rmse               DOUBLE      COMMENT '均方根误差',
    mae                DOUBLE      COMMENT '平均绝对误差',
    precision_at_10    DOUBLE      COMMENT 'Precision@10',
    recall_at_10       DOUBLE      COMMENT 'Recall@10',
    hit_rate           DOUBLE      COMMENT '命中率',
    ndcg_at_10         DOUBLE      COMMENT 'NDCG@10',
    v2c_precision_at_10 DOUBLE     COMMENT 'View→Cart Precision@10',
    v2c_recall_at_10   DOUBLE      COMMENT 'View→Cart Recall@10'
)
COMMENT 'ALS模型评估指标(含view→cart预测)'
STORED AS PARQUET;

-- ============================================
-- 11. View→AddToCart 预测数据集 (由Spark生成)
-- ============================================
DROP TABLE IF EXISTS view_to_cart_dataset;

CREATE TABLE IF NOT EXISTS view_to_cart_dataset (
    visitor_id      STRING      COMMENT '访客ID',
    item_id         STRING      COMMENT '商品ID',
    label           INT         COMMENT '是否加购(0/1)'
)
COMMENT 'View→AddToCart预测数据集 - 核心任务数据'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/processed/view_to_cart';

-- ============================================
-- 12. 统计结果表
-- ============================================
DROP TABLE IF EXISTS daily_statistics;

CREATE TABLE IF NOT EXISTS daily_statistics (
    stat_date       STRING      COMMENT '统计日期',
    total_events    BIGINT      COMMENT '总事件数',
    unique_visitors BIGINT      COMMENT '独立访客数',
    view_count      BIGINT      COMMENT '浏览次数',
    cart_count      BIGINT      COMMENT '加购次数',
    buy_count       BIGINT      COMMENT '购买次数',
    conversion_rate DOUBLE      COMMENT '转化率'
)
COMMENT '每日统计汇总表'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/output/daily_stats';

-- ============================================
-- 验证表创建结果
-- ============================================
SHOW TABLES;

-- 查看表结构
DESCRIBE FORMATTED user_events;

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
    max_event           STRING      COMMENT '最高级别事件'
)
COMMENT '用户商品评分矩阵 - 用于推荐模型训练'
STORED AS PARQUET
LOCATION '${HDFS_BASE}/processed/user_item_ratings';

-- ============================================
-- 5. 推荐结果表 (由Spark生成)
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
-- 6. 统计结果表
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

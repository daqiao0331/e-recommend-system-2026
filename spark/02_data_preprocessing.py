# -*- coding: utf-8 -*-
"""
============================================
Spark数据预处理与特征工程
电子商务推荐系统
============================================
任务目标: 基于用户"浏览"事件预测"加购"行为
执行方式: spark-submit --master yarn 02_data_preprocessing.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum, when, min, max, avg,
    from_unixtime, to_date, hour, dayofweek, log1p, lit
)
from pyspark.sql.window import Window
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """创建SparkSession"""
    spark = SparkSession.builder \
        .appName("Ecommerce-DataPreprocessing") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "100") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def load_data(spark):
    """从Hive或HDFS加载数据"""
    logger.info("正在加载数据...")
    
    hdfs_base = os.environ.get("HDFS_BASE", f"/user/{os.environ.get('USER', 'daqiao')}/ecommerce")
    
    try:
        events_df = spark.sql("SELECT * FROM ecommerce.user_events")
        logger.info("从 Hive 表加载数据")
    except Exception:
        events_path = f"{hdfs_base}/raw_data/events/events.csv"
        logger.info(f"从 HDFS 读取: {events_path}")
        events_df = spark.read.csv(events_path, header=True, inferSchema=True) \
            .toDF("event_time", "visitor_id", "event_type", "item_id", "transaction_id")
    
    logger.info(f"原始数据总量: {events_df.count():,} 条")
    return events_df


def clean_data(events_df):
    """
    数据清洗
    - 过滤空值和无效事件
    - 添加时间特征
    """
    logger.info("正在清洗数据...")
    
    # 过滤空值和无效事件类型
    valid_events = ['view', 'addtocart', 'transaction']
    cleaned_df = events_df \
        .filter(col("visitor_id").isNotNull()) \
        .filter(col("item_id").isNotNull()) \
        .filter(col("event_type").isin(valid_events))
    
    # 添加时间特征
    cleaned_df = cleaned_df \
        .withColumn("event_datetime", from_unixtime(col("event_time") / 1000)) \
        .withColumn("event_date", to_date(col("event_datetime"))) \
        .withColumn("event_hour", hour(col("event_datetime"))) \
        .withColumn("day_of_week", dayofweek(col("event_datetime")))
    
    logger.info(f"清洗后数据量: {cleaned_df.count():,} 条")
    return cleaned_df


def create_user_item_ratings(cleaned_df):
    """
    构建用户-商品隐式评分矩阵
    
    针对任务目标优化: 预测view→addtocart转化
    评分策略（强调加购和购买行为）:
    - view: 1分
    - addtocart: 3分（核心预测目标，权重提高）
    - transaction: 5分（最高价值行为）
    """
    logger.info("正在构建用户-商品评分矩阵...")
    
    # 为不同行为赋予优化后的权重
    scored_df = cleaned_df.withColumn("score",
        when(col("event_type") == "view", 1.0)
        .when(col("event_type") == "addtocart", 3.0)  # 加购权重提高
        .when(col("event_type") == "transaction", 5.0)  # 购买最高权重
        .otherwise(0.0)
    )
    
    # 聚合用户对商品的评分
    user_item_ratings = scored_df \
        .groupBy("visitor_id", "item_id") \
        .agg(
            sum("score").alias("rating"),
            count("*").alias("interaction_count"),
            max("event_type").alias("max_event"),
            # 标记是否有加购行为（用于评估）
            max(when(col("event_type") == "addtocart", 1).otherwise(0)).alias("has_addtocart"),
            min("event_date").alias("first_interaction"),
            max("event_date").alias("last_interaction")
        )
    
    # 对数标准化（压缩评分范围，避免极端值）
    user_item_ratings = user_item_ratings \
        .withColumn("rating_normalized", log1p(col("rating")))
    
    logger.info(f"用户-商品对数量: {user_item_ratings.count():,}")
    return user_item_ratings


def create_user_features(cleaned_df):
    """构建用户特征（用于冷启动和分析）"""
    logger.info("正在构建用户特征...")
    
    user_features = cleaned_df \
        .groupBy("visitor_id") \
        .agg(
            count("*").alias("total_events"),
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "addtocart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "transaction", 1).otherwise(0)).alias("buy_count"),
            countDistinct("item_id").alias("unique_items"),
            countDistinct("event_date").alias("active_days"),
            min("event_date").alias("first_visit"),
            max("event_date").alias("last_visit")
        ) \
        .withColumn("cart_rate", col("cart_count") / col("view_count")) \
        .withColumn("buy_rate", col("buy_count") / col("view_count"))
    
    logger.info(f"用户数: {user_features.count():,}")
    return user_features


def create_item_features(cleaned_df):
    """构建商品特征（用于冷启动和分析）"""
    logger.info("正在构建商品特征...")
    
    item_features = cleaned_df \
        .groupBy("item_id") \
        .agg(
            count("*").alias("total_events"),
            sum(when(col("event_type") == "view", 1).otherwise(0)).alias("view_count"),
            sum(when(col("event_type") == "addtocart", 1).otherwise(0)).alias("cart_count"),
            sum(when(col("event_type") == "transaction", 1).otherwise(0)).alias("buy_count"),
            countDistinct("visitor_id").alias("unique_visitors")
        ) \
        .withColumn("cart_rate", col("cart_count") / col("view_count")) \
        .withColumn("conversion_rate", col("buy_count") / col("view_count"))
    
    logger.info(f"商品数: {item_features.count():,}")
    return item_features


def save_to_hive(df, table_name):
    """保存到Hive表"""
    logger.info(f"保存到 ecommerce.{table_name}...")
    df.write.mode("overwrite").saveAsTable(f"ecommerce.{table_name}")
    logger.info(f"[✓] {table_name} 保存成功")


def print_statistics(spark):
    """打印统计信息"""
    print("\n" + "="*60)
    print("              数据预处理统计结果")
    print("="*60)
    
    stats = spark.sql("""
        SELECT 
            COUNT(*) as total_pairs,
            COUNT(DISTINCT visitor_id) as users,
            COUNT(DISTINCT item_id) as items,
            ROUND(AVG(rating), 2) as avg_rating,
            SUM(has_addtocart) as addtocart_pairs
        FROM ecommerce.user_item_ratings
    """).collect()[0]
    
    print(f"\n  用户-商品对: {stats['total_pairs']:,}")
    print(f"  用户数: {stats['users']:,}")
    print(f"  商品数: {stats['items']:,}")
    print(f"  平均评分: {stats['avg_rating']}")
    print(f"  含加购行为对: {stats['addtocart_pairs']:,}")
    print("="*60 + "\n")


def main():
    print("\n" + "="*60)
    print("       电子商务推荐系统 - 数据预处理")
    print("="*60 + "\n")
    
    spark = create_spark_session()
    
    try:
        # 加载和清洗
        events_df = load_data(spark)
        cleaned_df = clean_data(events_df)
        cleaned_df.cache()
        
        # 构建特征矩阵
        user_item_ratings = create_user_item_ratings(cleaned_df)
        save_to_hive(user_item_ratings, "user_item_ratings")
        
        user_features = create_user_features(cleaned_df)
        save_to_hive(user_features, "user_features")
        
        item_features = create_item_features(cleaned_df)
        save_to_hive(item_features, "item_features")
        
        print_statistics(spark)
        
        print("[✓] 数据预处理完成！")
        print("下一步: spark-submit --master yarn spark/03_train_als_model.py\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

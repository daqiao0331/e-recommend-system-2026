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
    from_unixtime, to_date, hour, dayofweek, log1p, lit,
    row_number, dense_rank, lag, lead, datediff, expr
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
    
    # 获取项目根目录 (假设脚本在 spark/ 目录下)
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    
    if "HDFS_BASE" in os.environ:
        hdfs_base = os.environ["HDFS_BASE"]
    else:
        # 本地路径默认值
        hdfs_base = f"file://{project_root}/output_hdfs"
    
    try:
        events_df = spark.sql("SELECT * FROM ecommerce.user_events")
        logger.info("从 Hive 表加载数据")
    except Exception:
        events_path = f"{hdfs_base}/raw_data/events/events.csv"
        logger.info(f"从 HDFS 读取: {events_path}")
        events_df = spark.read.csv(events_path, header=True, inferSchema=True) \
            .toDF("event_time", "visitor_id", "event_type", "item_id", "transaction_id")
    
    original_count = events_df.count()
    logger.info(f"原始数据总量: {original_count:,} 条")
    return events_df, original_count


def clean_data(events_df, original_count):
    """
    深度数据清洗
    
    处理问题：
    1. 空值和无效事件类型
    2. 重复记录（同一时间戳的相同事件）
    3. 异常用户（机器人/爬虫 - 交互次数异常多）
    4. 异常商品（几乎没人看的商品）
    5. 时间范围异常的数据
    """
    logger.info("="*50)
    logger.info("开始深度数据清洗...")
    logger.info("="*50)
    
    # ========== Step 1: 基础清洗 ==========
    logger.info("\n[Step 1/5] 基础清洗：过滤空值和无效事件类型...")
    # 只保留有效的事件类型（过滤掉异常的'event'类型）
    valid_events = ['view', 'addtocart', 'transaction']
    cleaned_df = events_df \
        .filter(col("visitor_id").isNotNull()) \
        .filter(col("item_id").isNotNull()) \
        .filter(col("event_type").isNotNull()) \
        .filter(col("event_time").isNotNull()) \
        .filter(col("event_type").isin(valid_events))
    
    step1_count = cleaned_df.count()
    logger.info(f"  过滤空值后: {step1_count:,} 条 (移除 {original_count - step1_count:,})")
    
    # ========== Step 2: 去除重复记录 ==========
    logger.info("\n[Step 2/5] 去重：移除同一时间戳的重复事件...")
    # 同一用户、同一商品、同一时间、同一事件类型 -> 只保留一条
    cleaned_df = cleaned_df.dropDuplicates(["visitor_id", "item_id", "event_time", "event_type"])
    
    step2_count = cleaned_df.count()
    logger.info(f"  去重后: {step2_count:,} 条 (移除重复 {step1_count - step2_count:,})")
    
    # ========== Step 3: 过滤异常用户（机器人/爬虫）==========
    logger.info("\n[Step 3/5] 过滤异常用户（疑似机器人）...")
    
    # 计算每个用户的交互次数
    user_stats = cleaned_df.groupBy("visitor_id").agg(
        count("*").alias("event_count"),
        countDistinct("item_id").alias("unique_items")
    )
    
    # 【调整】使用99.9分位数，更宽松的阈值（之前是99分位）
    thresholds = user_stats.selectExpr(
        "percentile_approx(event_count, 0.999) as event_threshold",
        "percentile_approx(unique_items, 0.999) as item_threshold"
    ).collect()[0]
    
    event_threshold = thresholds['event_threshold']
    item_threshold = thresholds['item_threshold']
    logger.info(f"  用户交互次数99.9分位阈值: {event_threshold}")
    logger.info(f"  用户商品数99.9分位阈值: {item_threshold}")
    
    # 【调整】只过滤极端异常用户，保留更多数据
    # 移除：最低交互次数限制（之前要求>=2）
    normal_users = user_stats \
        .filter(col("event_count") <= event_threshold) \
        .select("visitor_id")
    
    cleaned_df = cleaned_df.join(normal_users, "visitor_id", "inner")
    
    step3_count = cleaned_df.count()
    logger.info(f"  过滤异常用户后: {step3_count:,} 条 (移除 {step2_count - step3_count:,})")
    
    # ========== Step 4: 过滤冷门商品（更宽松）==========
    logger.info("\n[Step 4/5] 过滤冷门商品...")
    
    # 【调整】保留所有商品，不再过滤冷门商品
    # 之前：至少被2个用户交互过的商品
    # 现在：保留所有商品（>=1即可）
    item_stats = cleaned_df.groupBy("item_id").agg(
        countDistinct("visitor_id").alias("unique_visitors")
    )
    valid_items = item_stats.filter(col("unique_visitors") >= 1).select("item_id")
    
    cleaned_df = cleaned_df.join(valid_items, "item_id", "inner")
    
    step4_count = cleaned_df.count()
    logger.info(f"  过滤冷门商品后: {step4_count:,} 条 (移除 {step3_count - step4_count:,})")
    
    # ========== Step 5: 添加时间特征 ==========
    logger.info("\n[Step 5/5] 添加时间特征...")
    cleaned_df = cleaned_df \
        .withColumn("event_datetime", from_unixtime(col("event_time") / 1000)) \
        .withColumn("event_date", to_date(col("event_datetime"))) \
        .withColumn("event_hour", hour(col("event_datetime"))) \
        .withColumn("day_of_week", dayofweek(col("event_datetime")))
    
    # ========== 清洗总结 ==========
    final_count = cleaned_df.count()
    logger.info("\n" + "="*50)
    logger.info("数据清洗完成！")
    logger.info(f"  原始数据: {original_count:,}")
    logger.info(f"  清洗后:   {final_count:,}")
    logger.info(f"  清洗比例: {(original_count - final_count) / original_count * 100:.2f}%")
    logger.info("="*50)
    
    return cleaned_df


def create_user_item_ratings(cleaned_df):
    """
    构建用户-商品隐式评分矩阵 (优化版: 引入 TF-IDF 思想)
    
    改进策略:
    1. Event Weight: 保持 view=1, cart=3, buy=10
    2. Item Weighting (IIF): 降低热门商品的权重。
       如果一个商品被所有人都看过，那么看过它并不能代表用户的特殊偏好。
       公式: Score = EventScore * (1 / log(ItemPopularity + 2))
    """
    logger.info("正在构建用户-商品评分矩阵 (IIF加权)...")
    
    # 1. 计算每个商品的全局热度 (Total Views)
    item_popularity = cleaned_df.groupBy("item_id").agg(count("*").alias("item_global_count"))
    
    # 2. 基础行为评分
    scored_df = cleaned_df.withColumn("base_score",
        when(col("event_type") == "view", 1.0)
        .when(col("event_type") == "addtocart", 3.0)  # 原5.0 -> 3.0, 适当降低避免独占
        .when(col("event_type") == "transaction", 10.0) 
        .otherwise(0.0)
    )
    
    # 3. 关联热度并计算最终加权分
    # 聚合每个用户对每个商品的总基础分
    user_item_base = scored_df \
        .groupBy("visitor_id", "item_id") \
        .agg(
            sum("base_score").alias("total_base_score"),
            count("*").alias("interaction_count"),
            max("event_type").alias("max_event"),
            max(when(col("event_type") == "addtocart", 1).otherwise(0)).alias("has_addtocart"),
            max(when(col("event_type") == "transaction", 1).otherwise(0)).alias("has_transaction")
        )
    
    # 加入商品热度权重
    # IIF系数: 1 / log10(count + 2). count=10 -> 0.92, count=100 -> 0.5, count=1000 -> 0.33
    ratings_final = user_item_base.join(item_popularity, "item_id") \
        .withColumn("iif_weight", 1.0 / log1p(col("item_global_count") + 1)) \
        .withColumn("rating", col("total_base_score") * col("iif_weight")) \
        .drop("item_global_count", "iif_weight")

    # 注意：这里不再做 log1p(rating)，因为 IIF 已经起到了缩放作用，
    # 且我们希望通过 alpha 参数在模型训练阶段控制置信度
    
    logger.info(f"用户-商品对数量: {ratings_final.count():,}")
    return ratings_final
    return user_item_ratings


def create_view_to_cart_dataset(cleaned_df):
    """
    专门为"view→addtocart"预测任务创建数据集
    
    关键：确保时序正确性！
    - 正样本：用户先浏览后加购的商品（view时间 < addtocart时间）
    - 负样本：用户只浏览未加购的商品
    
    这是任务的核心：基于"浏览"预测"加购"
    """
    logger.info("="*50)
    logger.info("创建 view→addtocart 预测数据集（时序验证）")
    logger.info("="*50)
    
    # ========== Step 1: 获取所有浏览事件（带时间）==========
    view_events = cleaned_df \
        .filter(col("event_type") == "view") \
        .select(
            col("visitor_id"),
            col("item_id"),
            col("event_time").alias("view_time")
        )
    
    # ========== Step 2: 获取所有加购事件（带时间）==========
    cart_events = cleaned_df \
        .filter(col("event_type") == "addtocart") \
        .select(
            col("visitor_id"),
            col("item_id"),
            col("event_time").alias("cart_time")
        )
    
    # ========== Step 3: 时序验证 - 先浏览后加购 ==========
    # 关联浏览和加购，检查时间顺序
    view_then_cart = view_events \
        .join(cart_events, ["visitor_id", "item_id"], "left")
    
    # 正样本：浏览时间 < 加购时间（时序正确）
    # 负样本：没有加购 或 加购在浏览之前（无效）
    view_cart_df = view_then_cart \
        .withColumn("label",
            when(
                (col("cart_time").isNotNull()) & 
                (col("cart_time") > col("view_time")),  # 关键：时序验证
                1
            ).otherwise(0)
        ) \
        .groupBy("visitor_id", "item_id") \
        .agg(
            max("label").alias("label"),  # 只要有一次正确的时序就是正样本
            min("view_time").alias("first_view_time"),
            max("view_time").alias("last_view_time"),
            count("*").alias("view_count")
        )
    
    # ========== 统计 ==========
    total = view_cart_df.count()
    positive = view_cart_df.filter(col("label") == 1).count()
    negative = total - positive
    
    logger.info(f"\nview→addtocart 数据集（时序验证后）:")
    logger.info(f"  总样本数:          {total:,}")
    logger.info(f"  正样本(先看后加购): {positive:,} ({positive/total*100:.2f}%)")
    logger.info(f"  负样本(只看不加购): {negative:,} ({negative/total*100:.2f}%)")
    logger.info(f"  转化率:            {positive/total*100:.2f}%")
    logger.info("="*50)
    
    return view_cart_df


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
        # 加载数据
        events_df, original_count = load_data(spark)
        
        # 深度清洗
        cleaned_df = clean_data(events_df, original_count)
        cleaned_df.cache()
        
        # 构建特征矩阵
        user_item_ratings = create_user_item_ratings(cleaned_df)
        save_to_hive(user_item_ratings, "user_item_ratings")
        
        user_features = create_user_features(cleaned_df)
        save_to_hive(user_features, "user_features")
        
        item_features = create_item_features(cleaned_df)
        save_to_hive(item_features, "item_features")
        
        # 创建view→addtocart预测专用数据集（核心任务，含时序验证）
        view_cart_df = create_view_to_cart_dataset(cleaned_df)
        save_to_hive(view_cart_df, "view_to_cart_dataset")
        
        print_statistics(spark)
        
        print("[✓] 数据预处理完成！")
        print("下一步: spark-submit --master yarn spark/03_train_als_model.py\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

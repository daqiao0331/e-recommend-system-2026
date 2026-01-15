# -*- coding: utf-8 -*-
"""
============================================
Spark ALS推荐模型训练
电子商务推荐系统
============================================
任务目标: 基于用户浏览行为预测加购行为
执行方式: 
    export PYSPARK_PYTHON=/usr/bin/python3
    export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
    spark-submit --master yarn --deploy-mode client \
        --driver-memory 4g --executor-memory 4g \
        --num-executors 2 --executor-cores 2 \
        --conf "spark.driver.extraJavaOptions=-Xss4m" \
        --conf "spark.executor.extraJavaOptions=-Xss4m" \
        spark/03_train_als_model.py
    
    或直接运行: ./run_all.sh
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, count, sum, when, avg, size, collect_list
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer
import logging
import time
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session():
    """创建SparkSession"""
    spark = SparkSession.builder \
        .appName("Ecommerce-ALS-Training") \
        .config("spark.sql.shuffle.partitions", "100") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def prepare_data(spark):
    """准备训练数据（过滤冷启动）"""
    logger.info("准备训练数据...")
    
    ratings = spark.sql("""
        SELECT visitor_id, item_id, rating, has_addtocart
        FROM ecommerce.user_item_ratings
        WHERE rating > 0
    """)
    
    original_count = ratings.count()
    logger.info(f"原始评分数据: {original_count:,} 条")
    
    # 过滤冷启动：保留交互>=3次的用户和商品
    user_counts = ratings.groupBy("visitor_id").count()
    active_users = user_counts.filter(col("count") >= 3).select("visitor_id")
    
    item_counts = ratings.groupBy("item_id").count()
    popular_items = item_counts.filter(col("count") >= 3).select("item_id")
    
    ratings_filtered = ratings \
        .join(active_users, "visitor_id") \
        .join(popular_items, "item_id")
    
    filtered_count = ratings_filtered.count()
    logger.info(f"过滤后数据: {filtered_count:,} 条 (保留 {filtered_count/original_count*100:.1f}%)")
    
    return ratings_filtered


def index_ids(ratings_df):
    """将字符串ID转换为数值索引"""
    logger.info("转换ID为数值索引...")
    
    # 用户ID索引
    user_indexer = StringIndexer(inputCol="visitor_id", outputCol="user_index", handleInvalid="skip")
    user_model = user_indexer.fit(ratings_df)
    ratings_indexed = user_model.transform(ratings_df)
    
    # 商品ID索引
    item_indexer = StringIndexer(inputCol="item_id", outputCol="item_index", handleInvalid="skip")
    item_model = item_indexer.fit(ratings_indexed)
    ratings_indexed = item_model.transform(ratings_indexed)
    
    # 转换为整数
    ratings_final = ratings_indexed \
        .withColumn("user_id", col("user_index").cast("integer")) \
        .withColumn("item_id_int", col("item_index").cast("integer")) \
        .select(
            col("user_id"),
            col("item_id_int").alias("item_id"),
            col("rating").cast("float"),
            col("has_addtocart"),
            col("visitor_id").alias("original_user_id"),
            col("item_id").alias("original_item_id")
        )
    
    # ID映射表
    user_mapping = ratings_final.select("user_id", "original_user_id").distinct()
    item_mapping = ratings_final.select("item_id", "original_item_id").distinct()
    
    logger.info(f"用户数: {user_mapping.count():,}, 商品数: {item_mapping.count():,}")
    
    return ratings_final, user_mapping, item_mapping


def train_als_model(training_data):
    """
    训练ALS模型（优化版V2）
    
    参数调优:
    - rank=80: 更多隐因子捕捉复杂模式
    - maxIter=25: 充分迭代
    - regParam=0.005: 更低正则化
    - alpha=5: 降低置信权重，让模型更关注评分差异
    """
    logger.info("训练ALS模型...")
    start_time = time.time()
    
    als = ALS(
        rank=80,                    # 增加隐因子
        maxIter=25,                 # 增加迭代
        regParam=0.005,             # 更低正则化
        alpha=5.0,                  # 降低置信权重
        userCol="user_id",
        itemCol="item_id",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        implicitPrefs=True
    )
    
    model = als.fit(training_data)
    
    logger.info(f"训练完成，耗时: {time.time() - start_time:.2f}秒")
    return model


def evaluate_model(model, test_data):
    """评估模型 - 回归指标"""
    logger.info("评估模型性能...")
    
    predictions = model.transform(test_data)
    
    # RMSE
    rmse_eval = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
    rmse = rmse_eval.evaluate(predictions)
    
    # MAE  
    mae_eval = RegressionEvaluator(metricName="mae", labelCol="rating", predictionCol="prediction")
    mae = mae_eval.evaluate(predictions)
    
    return rmse, mae


def evaluate_ranking_metrics(spark, model, test_data, k=10):
    """
    评估排序指标 - 推荐系统核心指标
    - Precision@K: Top-K推荐中命中的比例
    - Recall@K: 实际交互中被推荐到Top-K的比例  
    - Hit Rate: 至少命中一个的用户比例
    - NDCG@K: 归一化折损累计增益（考虑排序位置）
    
    重点评估: 推荐商品是否包含用户实际加购的商品
    """
    logger.info(f"计算排序指标 (K={k})...")
    
    import math
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType, IntegerType, StructType, StructField
    
    # 核心：获取测试集中用户实际加购的商品（has_addtocart=1）
    # 这是任务目标：预测哪些浏览的商品会被加购
    actual_items = test_data \
        .filter(col("has_addtocart") == 1) \
        .groupBy("user_id") \
        .agg(collect_list("item_id").alias("actual_items"))
    
    # 为测试集用户生成Top-K推荐
    test_users = test_data.select("user_id").distinct()
    user_recs = model.recommendForUserSubset(test_users, k)
    
    # 展开推荐结果
    user_recs_flat = user_recs \
        .select(col("user_id"), explode(col("recommendations")).alias("rec")) \
        .select(col("user_id"), col("rec.item_id").alias("rec_item"))
    
    # 聚合为推荐列表
    rec_lists = user_recs_flat \
        .groupBy("user_id") \
        .agg(collect_list("rec_item").alias("rec_items"))
    
    # 关联实际交互和推荐列表
    eval_df = rec_lists.join(actual_items, "user_id", "inner")
    
    eval_count = eval_df.count()
    if eval_count == 0:
        logger.warning("没有可评估的用户（无加购行为）")
        return {'precision': 0, 'recall': 0, 'hit_rate': 0, 'ndcg': 0}
    
    logger.info(f"可评估用户数（有加购行为）: {eval_count:,}")
    
    # 命中数UDF
    def calc_hits(rec_items, actual_items):
        if not rec_items or not actual_items:
            return 0
        return len(set(rec_items) & set(actual_items))
    
    hits_udf = udf(calc_hits, IntegerType())
    
    # NDCG UDF
    def calc_ndcg(rec_items, actual_items):
        if not rec_items or not actual_items:
            return 0.0
        actual_set = set(actual_items)
        dcg = 0.0
        for i, item in enumerate(rec_items):
            if item in actual_set:
                dcg += 1.0 / math.log2(i + 2)
        ideal_hits = min(len(actual_set), len(rec_items))
        idcg = 0.0
        for i in range(ideal_hits):
            idcg += 1.0 / math.log2(i + 2)
        return dcg / idcg if idcg > 0 else 0.0
    
    ndcg_udf = udf(calc_ndcg, DoubleType())
    
    # 计算指标
    eval_df = eval_df \
        .withColumn("hits", hits_udf(col("rec_items"), col("actual_items"))) \
        .withColumn("rec_size", size(col("rec_items"))) \
        .withColumn("actual_size", size(col("actual_items"))) \
        .withColumn("ndcg", ndcg_udf(col("rec_items"), col("actual_items")))
    
    # 使用Spark SQL聚合
    eval_df.createOrReplaceTempView("eval_metrics")
    metrics = spark.sql("""
        SELECT 
            SUM(hits) / SUM(rec_size) as precision,
            SUM(hits) / SUM(actual_size) as recall,
            SUM(CASE WHEN hits > 0 THEN 1 ELSE 0 END) / COUNT(*) as hit_rate,
            AVG(ndcg) as ndcg
        FROM eval_metrics
    """).collect()[0]
    
    return {
        'precision': float(metrics['precision'] or 0),
        'recall': float(metrics['recall'] or 0),
        'hit_rate': float(metrics['hit_rate'] or 0),
        'ndcg': float(metrics['ndcg'] or 0)
    }


def evaluate_view_to_cart_prediction(spark, model, ratings_indexed, k=10):
    """
    专门评估"view→addtocart"预测任务
    
    核心指标：
    - 对于只浏览未加购的用户-商品对，推荐结果是否能预测他们会加购
    - 这是任务的核心目标
    """
    logger.info("评估 view→addtocart 预测性能...")
    
    from pyspark.sql.functions import row_number
    from pyspark.sql.window import Window
    
    # 获取有加购行为的用户-商品对
    actual_cart = ratings_indexed \
        .filter(col("has_addtocart") == 1) \
        .select("user_id", "item_id") \
        .distinct()
    
    actual_cart_count = actual_cart.count()
    logger.info(f"实际加购的用户-商品对: {actual_cart_count:,}")
    
    # 获取所有用户的Top-K推荐
    all_users = ratings_indexed.select("user_id").distinct()
    user_recs = model.recommendForUserSubset(all_users, k)
    
    # 展开推荐
    user_recs_flat = user_recs \
        .select(col("user_id"), explode(col("recommendations")).alias("rec")) \
        .select(col("user_id"), col("rec.item_id").alias("item_id"))
    
    # 计算推荐与实际加购的交集
    hits = user_recs_flat.join(actual_cart, ["user_id", "item_id"], "inner")
    hit_count = hits.count()
    
    # 计算指标
    total_recs = user_recs_flat.count()
    precision = hit_count / total_recs if total_recs > 0 else 0
    recall = hit_count / actual_cart_count if actual_cart_count > 0 else 0
    
    logger.info(f"  推荐命中加购: {hit_count:,} / {total_recs:,}")
    logger.info(f"  View→Cart Precision@{k}: {precision:.4f}")
    logger.info(f"  View→Cart Recall@{k}: {recall:.4f}")
    
    return {
        'v2c_precision': precision,
        'v2c_recall': recall,
        'v2c_hits': hit_count
    }


def generate_recommendations(model, user_mapping, item_mapping, top_n=10):
    """生成推荐结果"""
    logger.info(f"生成Top-{top_n}推荐...")
    
    user_recs = model.recommendForAllUsers(top_n)
    
    # 展开推荐
    user_recs_flat = user_recs \
        .select(col("user_id"), explode(col("recommendations")).alias("rec")) \
        .select(col("user_id"), col("rec.item_id").alias("item_id"), col("rec.rating").alias("score"))
    
    # 关联原始ID
    recommendations = user_recs_flat \
        .join(user_mapping, "user_id") \
        .join(item_mapping, "item_id") \
        .select(
            col("original_user_id").alias("visitor_id"),
            col("original_item_id").alias("recommended_item"),
            col("score"),
            col("user_id"),
            col("item_id")
        )
    
    logger.info(f"生成推荐数: {recommendations.count():,}")
    return recommendations, user_recs_flat


def save_results(spark, model, recommendations, user_recs_flat, user_mapping, item_mapping):
    """保存模型和结果"""
    logger.info("保存模型和结果...")
    
    # 保存模型（使用环境变量配置路径）
    import os
    hdfs_base = os.environ.get("HDFS_BASE", f"/user/{os.environ.get('USER', 'daqiao')}/ecommerce")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    model_path = f"{hdfs_base}/model/als_model_{timestamp}"
    model.write().overwrite().save(model_path)
    logger.info(f"[✓] 模型: {model_path}")
    
    # 保存推荐结果
    user_recs_flat.write.mode("overwrite").saveAsTable("ecommerce.user_recommendations")
    logger.info("[✓] 推荐索引: ecommerce.user_recommendations")
    
    recommendations.write.mode("overwrite").saveAsTable("ecommerce.user_recommendations_full")
    logger.info("[✓] 完整推荐: ecommerce.user_recommendations_full")
    
    # 保存ID映射
    user_mapping.write.mode("overwrite").saveAsTable("ecommerce.user_id_mapping")
    item_mapping.write.mode("overwrite").saveAsTable("ecommerce.item_id_mapping")
    logger.info("[✓] ID映射表已保存")


def show_sample(spark):
    """展示示例推荐"""
    print("\n" + "="*60)
    print("           示例推荐结果（前5用户）")
    print("="*60)
    
    spark.sql("""
        SELECT 
            visitor_id,
            COLLECT_LIST(recommended_item) AS recommendations,
            ROUND(AVG(score), 4) AS avg_score
        FROM ecommerce.user_recommendations_full
        GROUP BY visitor_id
        LIMIT 5
    """).show(truncate=False)


def main():
    print("\n" + "="*60)
    print("       电子商务推荐系统 - ALS模型训练")
    print("="*60 + "\n")
    
    spark = create_spark_session()
    
    try:
        # 准备数据
        ratings = prepare_data(spark)
        ratings_indexed, user_mapping, item_mapping = index_ids(ratings)
        
        # 缓存
        ratings_indexed.cache()
        user_mapping.cache()
        item_mapping.cache()
        
        # 划分数据集
        training, test = ratings_indexed.randomSplit([0.8, 0.2], seed=42)
        logger.info(f"训练集: {training.count():,}, 测试集: {test.count():,}")
        
        # 训练
        model = train_als_model(training)
        
        # 评估 - 回归指标
        rmse, mae = evaluate_model(model, test)
        
        # 评估 - 排序指标 (核心)
        ranking_metrics = evaluate_ranking_metrics(spark, model, test, k=10)
        
        # 评估 - view→addtocart 预测（任务核心目标）
        v2c_metrics = evaluate_view_to_cart_prediction(spark, model, ratings_indexed, k=10)
        
        # 打印评估结果
        print("\n" + "="*60)
        print("              模型评估结果")
        print("="*60)
        print("\n  【回归指标】")
        print(f"    RMSE (均方根误差):    {rmse:.4f}")
        print(f"    MAE  (平均绝对误差):  {mae:.4f}")
        print("\n  【排序指标】 @K=10")
        print(f"    Precision@10:  {ranking_metrics['precision']:.4f}  (推荐命中率)")
        print(f"    Recall@10:     {ranking_metrics['recall']:.4f}  (召回率)")
        print(f"    Hit Rate:      {ranking_metrics['hit_rate']:.4f}  (用户命中比例)")
        print(f"    NDCG@10:       {ranking_metrics['ndcg']:.4f}  (排序质量)")
        print("\n  【View→AddToCart 预测】 ★ 核心任务目标")
        print(f"    V2C Precision@10:  {v2c_metrics['v2c_precision']:.4f}")
        print(f"    V2C Recall@10:     {v2c_metrics['v2c_recall']:.4f}")
        print(f"    命中加购数:        {v2c_metrics['v2c_hits']:,}")
        print("="*60 + "\n")
        
        # 生成推荐
        recommendations, user_recs_flat = generate_recommendations(
            model, user_mapping, item_mapping, top_n=10
        )
        
        # 保存
        save_results(spark, model, recommendations, user_recs_flat, user_mapping, item_mapping)
        
        # 保存指标到Hive供可视化使用
        metrics_data = [(rmse, mae, ranking_metrics['precision'], 
                        ranking_metrics['recall'], ranking_metrics['hit_rate'], 
                        ranking_metrics['ndcg'],
                        v2c_metrics['v2c_precision'], v2c_metrics['v2c_recall'])]
        metrics_df = spark.createDataFrame(metrics_data, 
            ["rmse", "mae", "precision_at_10", "recall_at_10", "hit_rate", "ndcg_at_10",
             "v2c_precision_at_10", "v2c_recall_at_10"])
        metrics_df.write.mode("overwrite").saveAsTable("ecommerce.model_metrics")
        logger.info("[✓] 模型指标已保存到 ecommerce.model_metrics")
        
        # 示例
        show_sample(spark)
        
        print("\n" + "="*60)
        print("       ALS模型训练完成！")
        print("="*60)
        print(f"\n核心指标:")
        print(f"  Precision@10={ranking_metrics['precision']:.4f}, Recall@10={ranking_metrics['recall']:.4f}")
        print(f"  View→Cart Precision@10={v2c_metrics['v2c_precision']:.4f}")
        print("\n下一步: spark-submit --master yarn spark/04_visualization.py\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

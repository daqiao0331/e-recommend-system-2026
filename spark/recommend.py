# -*- coding: utf-8 -*-
"""
实时推荐脚本 - 简化版
使用: 
  spark-submit spark/recommend.py 257597  # 指定用户
  spark-submit spark/recommend.py         # 随机用户
"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import sys

def main():
    visitor_id = None
    
    if len(sys.argv) >= 2:
        visitor_id = sys.argv[1]
    
    spark = SparkSession.builder \
        .appName("Recommend") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # 如果没有指定用户ID，随机选择一个
    if not visitor_id:
        print("\n未指定用户，随机选择一个训练集用户...\n")
        random_user = spark.sql("""
            SELECT original_user_id
            FROM ecommerce.user_id_mapping
            ORDER BY RAND()
            LIMIT 1
        """).collect()
        
        if random_user:
            visitor_id = random_user[0]['original_user_id']
            print(f"已随机选择用户: {visitor_id}\n")
        else:
            print("错误: 无法找到可用用户")
            spark.stop()
            return
    
    print("\n" + "="*70)
    print(f"  用户 {visitor_id} 的推荐系统")
    print("="*70)
    
    # 1. 用户画像
    print("\n【用户历史】")
    history = spark.sql(f"""
        SELECT event_type, COUNT(*) as cnt, COUNT(DISTINCT item_id) as items
        FROM ecommerce.user_events
        WHERE visitor_id = '{visitor_id}'
        GROUP BY event_type
    """).collect()
    
    if not history:
        print(f"  用户 {visitor_id} 无历史记录")
        spark.stop()
        return
    
    # 事件类型中文
    event_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
    
    total = sum(h['cnt'] for h in history)
    for h in history:
        event_cn = event_map.get(h['event_type'], h['event_type'])
        print(f"  {event_cn:6s}: {h['cnt']:4d} 次 ({h['cnt']/total*100:5.1f}%)  |  {h['items']} 个商品")
    
    # 2. 最近浏览
    print("\n【最近浏览】")
    recent = spark.sql(f"""
        SELECT e.item_id, e.event_type, p.property_value as category_id
        FROM ecommerce.user_events e
        LEFT JOIN ecommerce.item_properties p 
            ON e.item_id = p.item_id AND p.property_name = 'categoryid'
        WHERE e.visitor_id = '{visitor_id}'
        ORDER BY e.event_time DESC
        LIMIT 5
    """).collect()
    
    # 事件类型中文
    event_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
    
    for idx, r in enumerate(recent, 1):
        event_cn = event_map.get(r['event_type'], r['event_type'])
        category = f"类目{r['category_id']}" if r['category_id'] else '未分类'
        print(f"  {idx}. 商品 {r['item_id']:<8}  [{event_cn}]  {category}")
    
    # 3. 获取推荐
    print("\n【推荐结果】")
    
    # 查找用户ID映射
    user_map = spark.sql(f"""
        SELECT user_id
        FROM ecommerce.user_id_mapping
        WHERE original_user_id = '{visitor_id}'
    """).collect()
    
    if not user_map:
        print(f"  用户 {visitor_id} 不在训练集（可能交互太少被过滤）")
        spark.stop()
        return
    
    user_id = user_map[0]['user_id']
    
    # 加载模型
    try:
        model = ALSModel.load("/user/ecommerce/model/als_model")
    except:
        print("  模型未找到，请先训练: spark-submit spark/03_train_als_model.py")
        spark.stop()
        return
    
    # 生成推荐
    from pyspark.sql.functions import col, explode
    user_df = spark.createDataFrame([(user_id,)], ["user_id"])
    recs = model.recommendForUserSubset(user_df, 10)
    
    # 获取商品映射
    item_map_df = spark.sql("SELECT item_id, original_item_id FROM ecommerce.item_id_mapping")
    item_map = {r['item_id']: r['original_item_id'] for r in item_map_df.collect()}
    
    # 展开推荐
    rec_list = recs.select(explode(col("recommendations")).alias("rec")).select(
        col("rec.item_id"), col("rec.rating")
    ).collect()
    
    # 获取推荐商品的类目
    item_ids = [item_map.get(r['item_id'], r['item_id']) for r in rec_list]
    item_props = {}
    if item_ids:
        props_df = spark.sql(f"""
            SELECT item_id, property_value
            FROM ecommerce.item_properties
            WHERE item_id IN ({','.join(map(str, item_ids))})
            AND property_name = 'categoryid'
        """).collect()
        for p in props_df:
            item_props[p['item_id']] = f"类目{p['property_value']}"
    
    print(f"\n  为用户推荐以下商品:\n")
    print(f"  {'排名':<6} {'商品ID':<12} {'商品类目':<20} {'推荐分数':<10}")
    print("  " + "-"*70)
    for idx, r in enumerate(rec_list, 1):
        original_item = item_map.get(r['item_id'], r['item_id'])
        category = item_props.get(original_item, '未分类')
        print(f"  {idx:<6} {str(original_item):<12} {category:<20} {r['rating']:8.4f}")
    
    print("\n" + "="*70 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()

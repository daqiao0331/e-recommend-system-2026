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
        SELECT e.item_id, e.event_type, e.event_time,
               p.property_value as category_id
        FROM ecommerce.user_events e
        LEFT JOIN ecommerce.item_properties p 
            ON e.item_id = p.item_id AND p.property_name = 'categoryid'
        WHERE e.visitor_id = '{visitor_id}'
        ORDER BY e.event_time DESC
        LIMIT 5
    """).collect()
    
    # 事件类型中文
    event_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
    
    from datetime import datetime
    for idx, r in enumerate(recent, 1):
        event_cn = event_map.get(r['event_type'], r['event_type'])
        category = f"类目{r['category_id']}" if r['category_id'] else '未分类'
        # 转换时间戳为可读时间
        time_str = datetime.fromtimestamp(r['event_time']/1000).strftime('%m-%d %H:%M:%S')
        print(f"  {idx}. {time_str}  商品{r['item_id']:<8}  [{event_cn}]  {category}")
    
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
    
    # 自动查找最新模型
    model_path = "/user/ecommerce/model/als_model"
    import os
    # 检查本地是否有output_hdfs目录（适配本地运行环境）
    local_model_dir = "output_hdfs/model"
    if os.path.exists(local_model_dir):
        dirs = [d for d in os.listdir(local_model_dir) if d.startswith("als_model_")]
        if dirs:
            latest_model = sorted(dirs)[-1]
            # 使用绝对路径确保Spark能找到
            abs_path = os.path.abspath(os.path.join(local_model_dir, latest_model))
            model_path = f"file://{abs_path}"
            print(f"  [提示] 自动加载最新训练的模型: {latest_model}")

    # 加载模型
    try:
        model = ALSModel.load(model_path)
    except:
        print(f"  模型未找到 ({model_path})，请先训练: spark-submit spark/03_train_als_model.py")
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
    
    rec_categories = []
    for idx, r in enumerate(rec_list, 1):
        original_item = item_map.get(r['item_id'], r['item_id'])
        category = item_props.get(original_item, '未分类')
        rec_categories.append(category)
        print(f"  {idx:<6} {str(original_item):<12} {category:<20} {r['rating']:8.4f}")
    
    # 推荐质量评估
    print("\n【推荐质量评估】")
    
    # 1. 分数范围
    scores = [r['rating'] for r in rec_list]
    print(f"  分数范围: {min(scores):.4f} ~ {max(scores):.4f}  (差值: {max(scores)-min(scores):.4f})")
    
    # 2. 类目多样性
    unique_cats = len(set(rec_categories))
    print(f"  类目多样性: {unique_cats}/10 个不同类目", end="")
    if unique_cats >= 8:
        print("  ✓ 多样性好")
    elif unique_cats >= 5:
        print("  ⚠ 多样性中等")
    else:
        print("  ✗ 多样性差，推荐过于集中")
    
    # 3. 结果解释与分析
    print("\n【推荐逻辑深度解析】")
    
    hist_categories = spark.sql(f"""
        SELECT DISTINCT p.property_value as cat
        FROM ecommerce.user_events e
        JOIN ecommerce.item_properties p 
            ON e.item_id = p.item_id AND p.property_name = 'categoryid'
        WHERE e.visitor_id = '{visitor_id}'
    """).collect()
    
    hist_cats = set(f"类目{r['cat']}" for r in hist_categories if r['cat'])
    rec_cats_set = set(rec_categories) - {'未分类'}
    
    # 计算交集和差集
    intersection = hist_cats & rec_cats_set
    difference = rec_cats_set - hist_cats
    
    print(f"  用户历史偏好类目: {list(hist_cats)[:5]}{'...' if len(hist_cats)>5 else ''}")
    print(f"  推荐结果覆盖类目: {len(rec_cats_set)} 个")
    
    if intersection:
        print(f"  ✓ {len(intersection)} 个类目直接命中历史兴趣 (如 {list(intersection)[0]})")
    
    if difference:
        print(f"\n  ? 为什么会出现 {len(difference)} 个非历史类目 (如 {list(difference)[0]})？")
        print("  -------------------------------------------------------------")
        print("  这里是 ALS 协同过滤算法的核心特性体现：")
        print("  1. 【隐式关联挖掘】: 算法通过大数据发现：")
        print("     “看过[您的历史类目]的许多其他用户，往往也购买了[这些新推荐类目]”。")
        print("     -> 系统预测：这可能是您的潜在需求（例如买了手机的人通常会看手机壳/耳机）。")
        print("  2. 【全局热门补充】: 如果您的历史行为较少（少于5-10次），算法个性化信号不足，")
        print("     为了保证推荐列表不为空，会自动填充全网评分最高的“万金油”商品。")
        
    if not intersection and difference:
         print("\n  [总结] 当前推荐完全基于“人群相似度”而非“内容相似度”。")
         print("  系统判断您可能属于一个购买习惯更广泛的用户群组。")
         print("  若您认为这不准确，希望强制限定在历史兴趣圈内，请使用【类目过滤推荐模式】：")
         print(f"  >>> spark-submit spark/recommend_category_filtered.py {visitor_id}")
    
    print("\n" + "="*70 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()

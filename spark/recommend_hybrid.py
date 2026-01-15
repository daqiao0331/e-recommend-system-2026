# -*- coding: utf-8 -*-
"""
混合推荐脚本 - 结合协同过滤(ALS) + 基于内容(类目)
使用: 
  spark-submit spark/recommend_hybrid.py 257597  # 指定用户
  spark-submit spark/recommend_hybrid.py         # 随机用户
"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from pyspark.sql.functions import col, explode
import sys

def main():
    visitor_id = None
    
    if len(sys.argv) >= 2:
        visitor_id = sys.argv[1]
    
    spark = SparkSession.builder \
        .appName("HybridRecommend") \
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
    print(f"  用户 {visitor_id} 的混合推荐系统")
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
    
    # 2. 最近浏览和用户偏好类目
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
    
    from datetime import datetime
    for idx, r in enumerate(recent, 1):
        event_cn = event_map.get(r['event_type'], r['event_type'])
        category = f"类目{r['category_id']}" if r['category_id'] else '未分类'
        time_str = datetime.fromtimestamp(r['event_time']/1000).strftime('%m-%d %H:%M:%S')
        print(f"  {idx}. {time_str}  商品{r['item_id']:<8}  [{event_cn}]  {category}")
    
    # 获取用户偏好类目（带权重）
    print("\n【用户类目偏好】")
    category_pref = spark.sql(f"""
        SELECT p.property_value as category_id, 
               COUNT(*) as view_count,
               SUM(CASE WHEN e.event_type = 'addtocart' THEN 3 
                        WHEN e.event_type = 'transaction' THEN 5 
                        ELSE 1 END) as weight
        FROM ecommerce.user_events e
        JOIN ecommerce.item_properties p 
            ON e.item_id = p.item_id AND p.property_name = 'categoryid'
        WHERE e.visitor_id = '{visitor_id}'
        GROUP BY p.property_value
        ORDER BY weight DESC
        LIMIT 5
    """).collect()
    
    prefer_categories = set()
    for idx, c in enumerate(category_pref, 1):
        print(f"  {idx}. 类目{c['category_id']:<8}  浏览 {c['view_count']:3d} 次  权重 {c['weight']:4d}")
        prefer_categories.add(str(c['category_id']))
    
    # 3. ALS协同过滤推荐
    print("\n【推荐过程】")
    
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
    
    # 生成更多候选推荐（50个）用于后续过滤
    print("  步骤1: ALS模型生成候选推荐 (Top 50)...")
    user_df = spark.createDataFrame([(user_id,)], ["user_id"])
    recs = model.recommendForUserSubset(user_df, 50)
    
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
            item_props[p['item_id']] = str(p['property_value'])
    
    # 步骤2: 混合策略 - 类目加权
    print("  步骤2: 应用类目偏好加权...")
    
    weighted_recs = []
    for r in rec_list:
        original_item = item_map.get(r['item_id'], r['item_id'])
        category_id = item_props.get(original_item, '')
        
        # 基础分数
        base_score = float(r['rating'])
        
        # 如果商品类目在用户偏好类目中，给予加权
        if category_id in prefer_categories:
            # 偏好类目商品加权 50%
            weighted_score = base_score * 1.5
            is_preferred = True
        else:
            weighted_score = base_score
            is_preferred = False
        
        weighted_recs.append({
            'item_id': original_item,
            'category_id': category_id,
            'base_score': base_score,
            'weighted_score': weighted_score,
            'is_preferred': is_preferred
        })
    
    # 排序并取Top 10
    weighted_recs.sort(key=lambda x: x['weighted_score'], reverse=True)
    final_recs = weighted_recs[:10]
    
    # 展示推荐结果
    print("\n【推荐结果】（类目偏好加权）\n")
    print(f"  {'排名':<6} {'商品ID':<12} {'商品类目':<20} {'基础分':<10} {'加权分':<10} {'标记':<10}")
    print("  " + "-"*80)
    
    rec_categories = []
    for idx, r in enumerate(final_recs, 1):
        category = f"类目{r['category_id']}" if r['category_id'] else '未分类'
        rec_categories.append(category)
        tag = "★偏好类目" if r['is_preferred'] else ""
        print(f"  {idx:<6} {str(r['item_id']):<12} {category:<20} {r['base_score']:8.4f}  {r['weighted_score']:8.4f}  {tag}")
    
    # 推荐质量评估
    print("\n【推荐质量评估】")
    
    # 1. 类目多样性
    unique_cats = len(set(rec_categories))
    print(f"  类目多样性: {unique_cats}/10 个不同类目", end="")
    if unique_cats >= 8:
        print("  ✓ 多样性好")
    elif unique_cats >= 5:
        print("  ⚠ 多样性中等")
    else:
        print("  ✗ 多样性差，推荐过于集中")
    
    # 2. 偏好类目命中率
    preferred_count = sum(1 for r in final_recs if r['is_preferred'])
    print(f"  偏好类目命中: {preferred_count}/10 ({preferred_count*10}%)", end="")
    if preferred_count >= 5:
        print("  ✓ 个性化强")
    elif preferred_count >= 3:
        print("  ⚠ 个性化中等")
    else:
        print("  ✗ 个性化弱")
    
    # 3. 对比纯ALS推荐
    print("\n【对比：纯ALS推荐 vs 混合推荐】")
    pure_als_top10 = weighted_recs[:10] if len(weighted_recs) >= 10 else weighted_recs
    pure_als_preferred = sum(1 for r in pure_als_top10 if r['category_id'] in prefer_categories)
    
    print(f"  纯ALS推荐: 偏好类目命中 {pure_als_preferred}/10")
    print(f"  混合推荐: 偏好类目命中 {preferred_count}/10", end="")
    if preferred_count > pure_als_preferred:
        print(f"  ✓ 提升 {preferred_count - pure_als_preferred} 个")
    else:
        print(f"  - 无变化")
    
    print("\n" + "="*70 + "\n")
    
    spark.stop()

if __name__ == "__main__":
    main()

# -*- coding: utf-8 -*-
"""
推荐服务 - 调用 ALS 模型生成推荐
"""
from pyspark.sql.functions import col, explode
from . import spark_service
import json
import os
from datetime import datetime


class RecommendService:
    """推荐服务类"""
    
    def __init__(self):
        self.spark_service = spark_service
        self._item_map = None
        self._user_map = None
    
    @property
    def item_map(self):
        """商品ID映射缓存"""
        if self._item_map is None:
            spark = self.spark_service.get_spark()
            try:
                item_df = spark.sql("SELECT item_id, original_item_id FROM ecommerce.item_id_mapping")
                self._item_map = {r['item_id']: r['original_item_id'] for r in item_df.collect()}
            except:
                self._item_map = {}
        return self._item_map
    
    def get_recommendations(self, visitor_id: str, top_n: int = 10):
        """获取用户推荐"""
        spark = self.spark_service.get_spark()
        
        # 获取映射后的用户ID
        try:
            user_map = spark.sql(f"""
                SELECT user_id FROM ecommerce.user_id_mapping
                WHERE original_user_id = '{visitor_id}'
            """).collect()
        except Exception as e:
            return {'error': f'查询用户失败: {str(e)}'}
        
        if not user_map:
            return {'error': f'用户 {visitor_id} 不在训练集中'}
        
        user_id = user_map[0]['user_id']
        
        # 加载模型
        model = self.spark_service.load_als_model()
        if model is None:
            return {'error': '模型未找到，请先训练模型'}
        
        # 生成推荐
        try:
            user_df = spark.createDataFrame([(user_id,)], ["user_id"])
            recs = model.recommendForUserSubset(user_df, top_n)
            
            # 解析结果
            rec_list = recs.select(explode(col("recommendations")).alias("rec")).select(
                col("rec.item_id"), col("rec.rating")
            ).collect()
            
            # 获取商品类目信息
            item_ids = [self.item_map.get(r['item_id'], r['item_id']) for r in rec_list]
            item_props = {}
            if item_ids:
                props_df = spark.sql(f"""
                    SELECT item_id, property_value
                    FROM ecommerce.item_properties
                    WHERE item_id IN ({','.join(map(str, item_ids))})
                    AND property_name = 'categoryid'
                """).collect()
                for p in props_df:
                    item_props[p['item_id']] = p['property_value']
            
            # 转换为原始商品ID
            recommendations = []
            for idx, r in enumerate(rec_list, 1):
                original_item = self.item_map.get(r['item_id'], r['item_id'])
                category = item_props.get(original_item, '未知')
                recommendations.append({
                    'rank': idx,
                    'item_id': original_item,
                    'category_id': category,
                    'score': round(float(r['rating']), 4)
                })
            
            return {
                'user_id': visitor_id,
                'recommendations': recommendations,
                'count': len(recommendations),
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
        except Exception as e:
            return {'error': f'推荐生成失败: {str(e)}'}
    
    def get_random_user(self):
        """获取随机用户ID"""
        try:
            result = self.spark_service.execute_query("""
                SELECT original_user_id
                FROM ecommerce.user_id_mapping
                ORDER BY RAND()
                LIMIT 1
            """)
            return result[0]['original_user_id'] if result else None
        except:
            return None
    
    def get_active_users(self, limit: int = 20):
        """获取活跃用户列表"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT u.original_user_id, COUNT(*) as event_count
                FROM ecommerce.user_events e
                JOIN ecommerce.user_id_mapping u ON e.visitor_id = u.original_user_id
                GROUP BY u.original_user_id
                ORDER BY event_count DESC
                LIMIT {limit}
            """)
            return [{'user_id': r['original_user_id'], 'events': r['event_count']} for r in result]
        except:
            return []
    
    def save_recommendations(self, user_id: str, recommendations: dict, output_dir: str):
        """保存推荐结果到文件"""
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"recommend_{user_id}_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(recommendations, f, ensure_ascii=False, indent=2)
        
        return filepath
    
    def batch_recommend(self, user_ids: list, top_n: int = 10):
        """批量推荐"""
        results = []
        for uid in user_ids:
            rec = self.get_recommendations(uid, top_n)
            results.append(rec)
        return results


# 全局实例
recommend_service = RecommendService()

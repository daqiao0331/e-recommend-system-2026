# -*- coding: utf-8 -*-
"""
用户服务 - 用户画像和历史查询
"""
from . import spark_service


class UserService:
    """用户服务类"""
    
    def __init__(self):
        self.spark_service = spark_service
    
    def get_user_profile(self, visitor_id: str):
        """获取用户画像"""
        spark = self.spark_service.get_spark()
        
        # 行为统计
        try:
            stats = spark.sql(f"""
                SELECT 
                    event_type,
                    COUNT(*) as count,
                    COUNT(DISTINCT item_id) as unique_items
                FROM ecommerce.user_events
                WHERE visitor_id = '{visitor_id}'
                GROUP BY event_type
            """).collect()
        except Exception as e:
            return {'error': str(e)}
        
        if not stats:
            return None
        
        profile = {
            'user_id': visitor_id,
            'view_count': 0,
            'cart_count': 0,
            'purchase_count': 0,
            'view_items': 0,
            'cart_items': 0,
            'purchase_items': 0
        }
        
        for s in stats:
            if s['event_type'] == 'view':
                profile['view_count'] = s['count']
                profile['view_items'] = s['unique_items']
            elif s['event_type'] == 'addtocart':
                profile['cart_count'] = s['count']
                profile['cart_items'] = s['unique_items']
            elif s['event_type'] == 'transaction':
                profile['purchase_count'] = s['count']
                profile['purchase_items'] = s['unique_items']
        
        profile['total_events'] = profile['view_count'] + profile['cart_count'] + profile['purchase_count']
        profile['total_items'] = profile['view_items'] + profile['cart_items'] + profile['purchase_items']
        
        # 计算转化率
        if profile['view_count'] > 0:
            profile['cart_rate'] = round(profile['cart_count'] / profile['view_count'] * 100, 2)
            profile['purchase_rate'] = round(profile['purchase_count'] / profile['view_count'] * 100, 2)
        else:
            profile['cart_rate'] = 0
            profile['purchase_rate'] = 0
        
        return profile
    
    def get_recent_items(self, visitor_id: str, limit: int = 10):
        """获取用户最近浏览的商品"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT 
                    e.item_id,
                    e.event_type,
                    e.event_time,
                    p.property_value as category_id
                FROM ecommerce.user_events e
                LEFT JOIN ecommerce.item_properties p 
                    ON e.item_id = p.item_id AND p.property_name = 'categoryid'
                WHERE e.visitor_id = '{visitor_id}'
                ORDER BY e.event_time DESC
                LIMIT {limit}
            """)
            
            event_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
            items = []
            for r in result:
                items.append({
                    'item_id': r['item_id'],
                    'event_type': r['event_type'],
                    'event_type_cn': event_map.get(r['event_type'], r['event_type']),
                    'event_time': r['event_time'],
                    'category_id': r['category_id'] or '未分类'
                })
            return items
        except Exception as e:
            return []
    
    def get_user_categories(self, visitor_id: str):
        """获取用户偏好类目"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT 
                    p.property_value as category_id,
                    COUNT(*) as count
                FROM ecommerce.user_events e
                JOIN ecommerce.item_properties p 
                    ON e.item_id = p.item_id AND p.property_name = 'categoryid'
                WHERE e.visitor_id = '{visitor_id}'
                GROUP BY p.property_value
                ORDER BY count DESC
                LIMIT 10
            """)
            return [{'category_id': r['category_id'], 'count': r['count']} for r in result]
        except:
            return []
    
    def search_users(self, keyword: str = None, limit: int = 20):
        """搜索用户"""
        try:
            if keyword:
                query = f"""
                    SELECT DISTINCT original_user_id
                    FROM ecommerce.user_id_mapping
                    WHERE original_user_id LIKE '%{keyword}%'
                    LIMIT {limit}
                """
            else:
                query = f"""
                    SELECT original_user_id
                    FROM ecommerce.user_id_mapping
                    LIMIT {limit}
                """
            result = self.spark_service.execute_query(query)
            return [r['original_user_id'] for r in result]
        except:
            return []
    
    def check_user_exists(self, visitor_id: str):
        """检查用户是否存在于训练集"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT COUNT(*) as cnt
                FROM ecommerce.user_id_mapping
                WHERE original_user_id = '{visitor_id}'
            """)
            return result[0]['cnt'] > 0 if result else False
        except:
            return False


# 全局实例
user_service = UserService()

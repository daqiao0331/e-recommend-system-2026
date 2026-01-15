# -*- coding: utf-8 -*-
"""
数据分析服务 - 统计和可视化数据
"""
from . import spark_service


class AnalysisService:
    """数据分析服务类"""
    
    def __init__(self):
        self.spark_service = spark_service
    
    def get_overview_stats(self):
        """获取系统概览统计"""
        try:
            result = self.spark_service.execute_query("""
                SELECT 
                    COUNT(*) as total_events,
                    COUNT(DISTINCT visitor_id) as total_users,
                    COUNT(DISTINCT item_id) as total_items
                FROM ecommerce.user_events
            """)
            
            if result:
                return {
                    'total_events': result[0]['total_events'],
                    'total_users': result[0]['total_users'],
                    'total_items': result[0]['total_items']
                }
        except Exception as e:
            print(f"Error: {e}")
        return {'total_events': 0, 'total_users': 0, 'total_items': 0}
    
    def get_event_distribution(self):
        """获取事件类型分布"""
        try:
            result = self.spark_service.execute_query("""
                SELECT 
                    event_type,
                    COUNT(*) as count
                FROM ecommerce.user_events
                GROUP BY event_type
                ORDER BY count DESC
            """)
            
            event_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
            total = sum(r['count'] for r in result)
            
            return [{
                'event_type': r['event_type'],
                'event_type_cn': event_map.get(r['event_type'], r['event_type']),
                'count': r['count'],
                'percentage': round(r['count'] / total * 100, 2) if total > 0 else 0
            } for r in result]
        except:
            return []
    
    def get_hot_items(self, limit: int = 20):
        """获取热门商品"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT 
                    e.item_id,
                    COUNT(*) as view_count,
                    COUNT(DISTINCT e.visitor_id) as unique_visitors,
                    MAX(p.property_value) as category_id
                FROM ecommerce.user_events e
                LEFT JOIN ecommerce.item_properties p 
                    ON e.item_id = p.item_id AND p.property_name = 'categoryid'
                WHERE e.event_type = 'view'
                GROUP BY e.item_id
                ORDER BY view_count DESC
                LIMIT {limit}
            """)
            
            return [{
                'rank': idx + 1,
                'item_id': r['item_id'],
                'view_count': r['view_count'],
                'unique_visitors': r['unique_visitors'],
                'category_id': r['category_id'] or '未分类'
            } for idx, r in enumerate(result)]
        except:
            return []
    
    def get_conversion_funnel(self):
        """获取转化漏斗数据"""
        try:
            result = self.spark_service.execute_query("""
                SELECT 'view' as stage, COUNT(DISTINCT visitor_id) as users
                FROM ecommerce.user_events WHERE event_type = 'view'
                UNION ALL
                SELECT 'addtocart', COUNT(DISTINCT visitor_id)
                FROM ecommerce.user_events WHERE event_type = 'addtocart'
                UNION ALL
                SELECT 'transaction', COUNT(DISTINCT visitor_id)
                FROM ecommerce.user_events WHERE event_type = 'transaction'
            """)
            
            stage_map = {'view': '浏览', 'addtocart': '加购', 'transaction': '购买'}
            stage_order = {'view': 1, 'addtocart': 2, 'transaction': 3}
            
            funnel = []
            for r in result:
                funnel.append({
                    'stage': r['stage'],
                    'stage_cn': stage_map.get(r['stage'], r['stage']),
                    'users': r['users'],
                    'order': stage_order.get(r['stage'], 0)
                })
            
            # 按顺序排列
            funnel.sort(key=lambda x: x['order'])
            
            # 计算转化率
            if funnel and funnel[0]['users'] > 0:
                base = funnel[0]['users']
                for f in funnel:
                    f['rate'] = round(f['users'] / base * 100, 2)
            
            return funnel
        except:
            return []
    
    def get_user_activity_distribution(self):
        """获取用户活跃度分布"""
        try:
            result = self.spark_service.execute_query("""
                SELECT 
                    activity_level,
                    COUNT(*) as user_count
                FROM (
                    SELECT 
                        visitor_id,
                        CASE 
                            WHEN COUNT(*) >= 100 THEN '高活跃(100+)'
                            WHEN COUNT(*) >= 50 THEN '中活跃(50-99)'
                            WHEN COUNT(*) >= 10 THEN '低活跃(10-49)'
                            ELSE '新用户(<10)'
                        END as activity_level
                    FROM ecommerce.user_events
                    GROUP BY visitor_id
                ) t
                GROUP BY activity_level
            """)
            
            return [{'level': r['activity_level'], 'count': r['user_count']} for r in result]
        except:
            return []
    
    def get_category_distribution(self):
        """获取类目分布"""
        try:
            result = self.spark_service.execute_query("""
                SELECT 
                    p.property_value as category_id,
                    COUNT(*) as event_count
                FROM ecommerce.user_events e
                JOIN ecommerce.item_properties p 
                    ON e.item_id = p.item_id AND p.property_name = 'categoryid'
                GROUP BY p.property_value
                ORDER BY event_count DESC
                LIMIT 15
            """)
            
            return [{'category_id': r['category_id'], 'count': r['event_count']} for r in result]
        except:
            return []
    
    def get_daily_events(self, days: int = 30):
        """获取每日事件趋势"""
        try:
            result = self.spark_service.execute_query(f"""
                SELECT 
                    DATE(FROM_UNIXTIME(event_time/1000)) as event_date,
                    COUNT(*) as event_count
                FROM ecommerce.user_events
                GROUP BY DATE(FROM_UNIXTIME(event_time/1000))
                ORDER BY event_date DESC
                LIMIT {days}
            """)
            
            return [{'date': str(r['event_date']), 'count': r['event_count']} for r in result]
        except:
            return []


# 全局实例
analysis_service = AnalysisService()

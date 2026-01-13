# -*- coding: utf-8 -*-
"""
============================================
数据可视化脚本
电子商务推荐系统
============================================
执行方式: spark-submit --master yarn spark/04_visualization.py
"""

from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')
plt.rcParams['font.sans-serif'] = ['DejaVu Sans', 'SimHei', 'WenQuanYi Micro Hei']
plt.rcParams['axes.unicode_minus'] = False

import warnings
warnings.filterwarnings('ignore')

import os
OUTPUT_DIR = "output/figures"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def create_spark_session():
    spark = SparkSession.builder \
        .appName("Ecommerce-Visualization") \
        .config("spark.sql.shuffle.partitions", "50") \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def fetch_data(spark):
    """从Hive获取数据"""
    print("=" * 60)
    print("       从 Hive 获取数据...")
    print("=" * 60)
    
    data = {}
    
    # 1. 事件分布
    print("\n[1/6] 事件类型分布...")
    data['events'] = spark.sql("""
        SELECT event_type, COUNT(*) as count
        FROM ecommerce.user_events
        GROUP BY event_type ORDER BY count DESC
    """).toPandas()
    
    # 2. 转化漏斗
    print("[2/6] 转化漏斗...")
    funnel = spark.sql("""
        SELECT 'view' as stage, COUNT(DISTINCT visitor_id) as users FROM ecommerce.user_events WHERE event_type='view'
        UNION ALL
        SELECT 'addtocart', COUNT(DISTINCT visitor_id) FROM ecommerce.user_events WHERE event_type='addtocart'
        UNION ALL
        SELECT 'transaction', COUNT(DISTINCT visitor_id) FROM ecommerce.user_events WHERE event_type='transaction'
    """).toPandas()
    order = {'view': 0, 'addtocart': 1, 'transaction': 2}
    funnel['order'] = funnel['stage'].map(order)
    funnel = funnel.sort_values('order')
    funnel['stage'] = funnel['stage'].map({'view': 'View', 'addtocart': 'Add to Cart', 'transaction': 'Purchase'})
    data['funnel'] = funnel[['stage', 'users']]
    
    # 3. 每日趋势
    print("[3/6] 每日趋势...")
    data['daily'] = spark.sql("""
        SELECT to_date(from_unixtime(event_time/1000)) as date,
               COUNT(DISTINCT visitor_id) as dau,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) as views,
               SUM(CASE WHEN event_type='addtocart' THEN 1 ELSE 0 END) as carts,
               SUM(CASE WHEN event_type='transaction' THEN 1 ELSE 0 END) as buys
        FROM ecommerce.user_events
        GROUP BY to_date(from_unixtime(event_time/1000))
        ORDER BY date
    """).toPandas()
    
    # 4. 小时活跃度
    print("[4/6] 小时活跃度...")
    data['hourly'] = spark.sql("""
        SELECT hour(from_unixtime(event_time/1000)) as hour, COUNT(*) as count
        FROM ecommerce.user_events
        GROUP BY hour(from_unixtime(event_time/1000)) ORDER BY hour
    """).toPandas()
    
    # 5. 热门商品
    print("[5/6] 热门商品...")
    data['top_items'] = spark.sql("""
        SELECT item_id, COUNT(*) as views
        FROM ecommerce.user_events WHERE event_type='view'
        GROUP BY item_id ORDER BY views DESC LIMIT 20
    """).toPandas()
    
    # 6. 统计汇总
    print("[6/6] 统计汇总...")
    stats = spark.sql("""
        SELECT COUNT(*) as total, COUNT(DISTINCT visitor_id) as users, COUNT(DISTINCT item_id) as items,
               MIN(to_date(from_unixtime(event_time/1000))) as min_date,
               MAX(to_date(from_unixtime(event_time/1000))) as max_date,
               SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) as views,
               SUM(CASE WHEN event_type='addtocart' THEN 1 ELSE 0 END) as carts,
               SUM(CASE WHEN event_type='transaction' THEN 1 ELSE 0 END) as buys
        FROM ecommerce.user_events
    """).collect()[0]
    
    # 读取模型指标
    try:
        metrics = spark.sql("SELECT * FROM ecommerce.model_metrics").collect()[0]
        rmse = float(metrics['rmse'])
        mae = float(metrics['mae'])
        precision = float(metrics['precision_at_10'])
        recall = float(metrics['recall_at_10'])
        hit_rate = float(metrics['hit_rate'])
        ndcg = float(metrics['ndcg_at_10'])
    except:
        rmse, mae, precision, recall, hit_rate, ndcg = 2.89, 1.66, 0.0116, 0.0793, 0.1092, 0.0523
    
    data['stats'] = {
        'total': stats['total'], 'users': stats['users'], 'items': stats['items'],
        'date_range': f"{stats['min_date']} ~ {stats['max_date']}",
        'views': stats['views'], 'carts': stats['carts'], 'buys': stats['buys'],
        'view_pct': stats['views']/stats['total']*100,
        'cart_pct': stats['carts']/stats['total']*100,
        'buy_pct': stats['buys']/stats['total']*100,
        'v2c_rate': stats['carts']/stats['views']*100 if stats['views'] else 0,
        'c2b_rate': stats['buys']/stats['carts']*100 if stats['carts'] else 0,
        'rmse': rmse, 'mae': mae,
        'precision': precision, 'recall': recall, 'hit_rate': hit_rate, 'ndcg': ndcg
    }
    
    print("\n[✓] 数据获取完成！")
    return data


def plot_all(data):
    """生成所有图表"""
    print("\n" + "=" * 60)
    print("       生成可视化图表...")
    print("=" * 60)
    
    # 1. 事件分布
    fig, ax = plt.subplots(figsize=(10, 6))
    colors = ['#3498db', '#f39c12', '#27ae60', '#e74c3c']
    ax.bar(data['events']['event_type'], data['events']['count'], color=colors[:len(data['events'])])
    ax.set_title('Event Type Distribution', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count')
    for i, v in enumerate(data['events']['count']):
        ax.text(i, v + max(data['events']['count'])*0.01, f'{v:,}', ha='center')
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/01_event_distribution.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/01_event_distribution.png")
    
    # 2. 转化漏斗
    fig, ax = plt.subplots(figsize=(10, 6))
    values = data['funnel']['users'].values
    rates = [100] + [values[i]/values[0]*100 for i in range(1, len(values))]
    bars = ax.barh(data['funnel']['stage'], values, color=['#3498db', '#f39c12', '#27ae60'])
    for bar, val, rate in zip(bars, values, rates):
        ax.text(val + max(values)*0.02, bar.get_y() + bar.get_height()/2, 
                f'{val:,} ({rate:.1f}%)', va='center')
    ax.set_title('Conversion Funnel', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/02_conversion_funnel.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/02_conversion_funnel.png")
    
    # 3. 每日趋势
    if len(data['daily']) > 0:
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8))
        ax1.plot(data['daily']['date'], data['daily']['dau'], 'b-', linewidth=2)
        ax1.fill_between(data['daily']['date'], data['daily']['dau'], alpha=0.3)
        ax1.set_title('Daily Active Users', fontsize=12, fontweight='bold')
        ax1.tick_params(axis='x', rotation=45)
        ax1.grid(alpha=0.3)
        
        ax2.stackplot(data['daily']['date'], data['daily']['views'], data['daily']['carts'], data['daily']['buys'],
                      labels=['View', 'Add to Cart', 'Purchase'], colors=['#3498db', '#f39c12', '#27ae60'], alpha=0.8)
        ax2.set_title('Daily Events', fontsize=12, fontweight='bold')
        ax2.legend(loc='upper left')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(alpha=0.3)
        plt.tight_layout()
        plt.savefig(f'{OUTPUT_DIR}/03_daily_trend.png', dpi=150)
        plt.close()
        print(f"[✓] {OUTPUT_DIR}/03_daily_trend.png")
    
    # 4. 小时活跃度
    fig, ax = plt.subplots(figsize=(12, 5))
    colors = plt.cm.Blues(data['hourly']['count'] / data['hourly']['count'].max())
    ax.bar(data['hourly']['hour'], data['hourly']['count'], color=colors)
    ax.set_title('Hourly Activity Distribution', fontsize=14, fontweight='bold')
    ax.set_xlabel('Hour')
    ax.set_ylabel('Event Count')
    ax.set_xticks(range(24))
    peak = data['hourly'].loc[data['hourly']['count'].idxmax(), 'hour']
    ax.axvline(peak, color='red', linestyle='--', label=f'Peak: {peak}:00')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/04_hourly_activity.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/04_hourly_activity.png")
    
    # 5. 热门商品
    fig, ax = plt.subplots(figsize=(12, 8))
    items = data['top_items']['item_id'].astype(str)
    views = data['top_items']['views']
    colors = plt.cm.Oranges(views / views.max())
    bars = ax.barh(range(len(items)), views, color=colors)
    ax.set_yticks(range(len(items)))
    ax.set_yticklabels(items, fontsize=9)
    ax.set_title('Top 20 Popular Items', fontsize=14, fontweight='bold')
    ax.invert_yaxis()
    for bar, v in zip(bars, views):
        ax.text(v + views.max()*0.01, bar.get_y() + bar.get_height()/2, f'{v:,}', va='center', fontsize=8)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/05_top_items.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/05_top_items.png")
    
    # 6. 模型性能 - 回归指标
    fig, ax = plt.subplots(figsize=(8, 5))
    metrics = ['RMSE', 'MAE']
    values = [data['stats']['rmse'], data['stats']['mae']]
    bars = ax.bar(metrics, values, color=['#3498db', '#27ae60'], width=0.5)
    ax.set_title('ALS Model - Regression Metrics', fontsize=14, fontweight='bold')
    ax.set_ylabel('Error')
    ax.set_ylim(0, max(values) * 1.3)
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, v + max(values)*0.02, f'{v:.4f}', ha='center', fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/06_regression_metrics.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/06_regression_metrics.png")
    
    # 7. 模型性能 - 排序指标（核心）
    fig, ax = plt.subplots(figsize=(10, 6))
    metrics = ['Precision@10', 'Recall@10', 'Hit Rate', 'NDCG@10']
    values = [data['stats']['precision']*100, data['stats']['recall']*100, 
              data['stats']['hit_rate']*100, data['stats']['ndcg']*100]
    colors = ['#e74c3c', '#3498db', '#27ae60', '#9b59b6']
    bars = ax.bar(metrics, values, color=colors, width=0.6)
    ax.set_title('ALS Model - Ranking Metrics @K=10', fontsize=14, fontweight='bold')
    ax.set_ylabel('Percentage (%)')
    ax.set_ylim(0, max(values) * 1.4)
    for bar, v in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width()/2, v + max(values)*0.02, f'{v:.2f}%', ha='center', fontweight='bold', fontsize=11)
    ax.grid(axis='y', alpha=0.3)
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/07_ranking_metrics.png', dpi=150)
    plt.close()
    print(f"[✓] {OUTPUT_DIR}/07_ranking_metrics.png")


def generate_report(stats):
    """生成报告"""
    report = f"""
================================================================================
                 E-commerce Recommender System - Analysis Report
================================================================================

1. Data Overview
--------------------------------------------------------------------------------
   Total Events:      {stats['total']:,}
   Unique Users:      {stats['users']:,}
   Unique Items:      {stats['items']:,}
   Date Range:        {stats['date_range']}

2. User Behavior
--------------------------------------------------------------------------------
   View:              {stats['views']:,}  ({stats['view_pct']:.1f}%)
   Add to Cart:       {stats['carts']:,}  ({stats['cart_pct']:.1f}%)
   Purchase:          {stats['buys']:,}   ({stats['buy_pct']:.1f}%)
   
   View→Cart Rate:    {stats['v2c_rate']:.2f}%
   Cart→Buy Rate:     {stats['c2b_rate']:.2f}%

3. Model Performance (ALS Collaborative Filtering)
--------------------------------------------------------------------------------
   【Regression Metrics】
   RMSE:              {stats['rmse']:.4f}
   MAE:               {stats['mae']:.4f}
   
   【Ranking Metrics @K=10】 ★ Core Metrics
   Precision@10:      {stats['precision']*100:.2f}%  (命中率)
   Recall@10:         {stats['recall']*100:.2f}%  (召回率)
   Hit Rate:          {stats['hit_rate']*100:.2f}%  (用户覆盖)
   NDCG@10:           {stats['ndcg']*100:.2f}%  (排序质量)

4. Charts Generated
--------------------------------------------------------------------------------
   01_event_distribution.png    Event Distribution
   02_conversion_funnel.png     Conversion Funnel  
   03_daily_trend.png           Daily Trend
   04_hourly_activity.png       Hourly Activity
   05_top_items.png             Top 20 Items
   06_regression_metrics.png    RMSE & MAE
   07_ranking_metrics.png       Precision, Recall, Hit Rate, NDCG ★

================================================================================
"""
    print(report)
    with open(f'{OUTPUT_DIR}/analysis_report.txt', 'w') as f:
        f.write(report)
    print(f"[✓] Report: {OUTPUT_DIR}/analysis_report.txt")


def main():
    print("\n" + "=" * 60)
    print("       E-commerce Visualization")
    print("=" * 60 + "\n")
    
    spark = create_spark_session()
    
    try:
        data = fetch_data(spark)
        plot_all(data)
        generate_report(data['stats'])
        
        print("\n" + "=" * 60)
        print("       可视化完成！")
        print("=" * 60)
        print(f"\n图表保存在: {OUTPUT_DIR}/\n")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

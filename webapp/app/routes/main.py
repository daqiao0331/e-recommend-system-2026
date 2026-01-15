# -*- coding: utf-8 -*-
"""
主页路由
"""
from flask import Blueprint, render_template
from ..services.analysis_service import analysis_service
from ..services import spark_service

main_bp = Blueprint('main', __name__)


@main_bp.route('/')
def index():
    """首页 - 系统概览"""
    # 获取统计数据
    stats = analysis_service.get_overview_stats()
    
    # 获取热门商品
    hot_items = analysis_service.get_hot_items(limit=10)
    
    # 获取事件分布
    event_dist = analysis_service.get_event_distribution()
    
    # 检查模型状态
    model_exists = spark_service.check_model_exists()
    
    return render_template('index.html',
                         stats=stats,
                         hot_items=hot_items,
                         event_dist=event_dist,
                         model_exists=model_exists)


@main_bp.route('/about')
def about():
    """关于页面"""
    return render_template('about.html')

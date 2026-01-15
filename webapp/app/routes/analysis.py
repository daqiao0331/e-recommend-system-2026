# -*- coding: utf-8 -*-
"""
数据分析路由
"""
from flask import Blueprint, render_template, jsonify, request
from ..services.analysis_service import analysis_service

analysis_bp = Blueprint('analysis', __name__)


@analysis_bp.route('/')
def index():
    """数据分析主页"""
    # 获取概览统计
    stats = analysis_service.get_overview_stats()
    
    # 获取事件分布
    event_dist = analysis_service.get_event_distribution()
    
    # 获取转化漏斗
    funnel = analysis_service.get_conversion_funnel()
    
    # 获取用户活跃度分布
    activity_dist = analysis_service.get_user_activity_distribution()
    
    # 获取热门商品
    hot_items = analysis_service.get_hot_items(limit=20)
    
    # 获取类目分布
    category_dist = analysis_service.get_category_distribution()
    
    return render_template('analysis.html',
                         stats=stats,
                         event_dist=event_dist,
                         funnel=funnel,
                         activity_dist=activity_dist,
                         hot_items=hot_items,
                         category_dist=category_dist)


# ============== API 接口 ==============

@analysis_bp.route('/api/stats')
def api_stats():
    """API: 获取系统统计"""
    stats = analysis_service.get_overview_stats()
    return jsonify({'success': True, 'data': stats})


@analysis_bp.route('/api/events')
def api_events():
    """API: 获取事件分布"""
    data = analysis_service.get_event_distribution()
    return jsonify({'success': True, 'data': data})


@analysis_bp.route('/api/funnel')
def api_funnel():
    """API: 获取转化漏斗"""
    data = analysis_service.get_conversion_funnel()
    return jsonify({'success': True, 'data': data})


@analysis_bp.route('/api/activity')
def api_activity():
    """API: 获取用户活跃度分布"""
    data = analysis_service.get_user_activity_distribution()
    return jsonify({'success': True, 'data': data})


@analysis_bp.route('/api/hot-items')
def api_hot_items():
    """API: 获取热门商品"""
    limit = request.args.get('limit', 20, type=int)
    data = analysis_service.get_hot_items(limit)
    return jsonify({'success': True, 'data': data})


@analysis_bp.route('/api/categories')
def api_categories():
    """API: 获取类目分布"""
    data = analysis_service.get_category_distribution()
    return jsonify({'success': True, 'data': data})


@analysis_bp.route('/api/daily-events')
def api_daily_events():
    """API: 获取每日事件趋势"""
    days = request.args.get('days', 30, type=int)
    data = analysis_service.get_daily_events(days)
    return jsonify({'success': True, 'data': data})

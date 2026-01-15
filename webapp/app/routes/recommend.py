# -*- coding: utf-8 -*-
"""
推荐相关路由
"""
from flask import Blueprint, render_template, request, jsonify, current_app
from ..services.recommend_service import recommend_service
from ..services.user_service import user_service
import os
import json

recommend_bp = Blueprint('recommend', __name__)


@recommend_bp.route('/', methods=['GET', 'POST'])
def index():
    """推荐查询页面"""
    recommendations = None
    user_profile = None
    recent_items = None
    error = None
    user_id = None
    
    if request.method == 'POST':
        user_id = request.form.get('user_id', '').strip()
        
        if not user_id:
            error = '请输入用户ID'
        else:
            # 获取用户画像
            user_profile = user_service.get_user_profile(user_id)
            
            if user_profile is None:
                error = f'用户 {user_id} 不存在或无历史记录'
            else:
                # 获取最近浏览
                recent_items = user_service.get_recent_items(user_id, limit=5)
                
                # 获取推荐
                result = recommend_service.get_recommendations(user_id, top_n=10)
                
                if 'error' in result:
                    error = result['error']
                else:
                    recommendations = result
    
    # 获取活跃用户列表（用于快速选择）
    active_users = recommend_service.get_active_users(limit=10)
    
    return render_template('recommend.html',
                         user_id=user_id,
                         recommendations=recommendations,
                         user_profile=user_profile,
                         recent_items=recent_items,
                         active_users=active_users,
                         error=error)


@recommend_bp.route('/random')
def random_recommend():
    """随机用户推荐"""
    user_id = recommend_service.get_random_user()
    
    if not user_id:
        return render_template('recommend.html', error='无法获取随机用户')
    
    # 获取用户画像
    user_profile = user_service.get_user_profile(user_id)
    recent_items = user_service.get_recent_items(user_id, limit=5)
    
    # 获取推荐
    recommendations = recommend_service.get_recommendations(user_id, top_n=10)
    
    error = recommendations.get('error') if 'error' in recommendations else None
    
    active_users = recommend_service.get_active_users(limit=10)
    
    return render_template('recommend.html',
                         user_id=user_id,
                         recommendations=recommendations if not error else None,
                         user_profile=user_profile,
                         recent_items=recent_items,
                         active_users=active_users,
                         error=error,
                         is_random=True)


@recommend_bp.route('/save', methods=['POST'])
def save_result():
    """保存推荐结果"""
    data = request.get_json()
    
    if not data:
        return jsonify({'success': False, 'message': '无效的数据'})
    
    user_id = data.get('user_id')
    recommendations = data.get('recommendations')
    
    if not user_id or not recommendations:
        return jsonify({'success': False, 'message': '缺少必要参数'})
    
    # 保存到文件
    output_dir = current_app.config.get('RESULTS_DIR', 'output/recommendations')
    
    try:
        filepath = recommend_service.save_recommendations(user_id, data, output_dir)
        return jsonify({
            'success': True,
            'message': '保存成功',
            'filepath': filepath
        })
    except Exception as e:
        return jsonify({'success': False, 'message': f'保存失败: {str(e)}'})


# ============== API 接口 ==============

@recommend_bp.route('/api/recommend/<user_id>')
def api_recommend(user_id):
    """API: 获取用户推荐"""
    top_n = request.args.get('top_n', 10, type=int)
    result = recommend_service.get_recommendations(user_id, top_n)
    return jsonify(result)


@recommend_bp.route('/api/user/<user_id>')
def api_user_profile(user_id):
    """API: 获取用户画像"""
    profile = user_service.get_user_profile(user_id)
    if profile:
        return jsonify({'success': True, 'data': profile})
    return jsonify({'success': False, 'message': '用户不存在'})


@recommend_bp.route('/api/user/<user_id>/history')
def api_user_history(user_id):
    """API: 获取用户历史"""
    limit = request.args.get('limit', 20, type=int)
    items = user_service.get_recent_items(user_id, limit)
    return jsonify({'success': True, 'data': items})


@recommend_bp.route('/api/random-user')
def api_random_user():
    """API: 获取随机用户"""
    user_id = recommend_service.get_random_user()
    if user_id:
        return jsonify({'success': True, 'user_id': user_id})
    return jsonify({'success': False, 'message': '无法获取随机用户'})


@recommend_bp.route('/api/active-users')
def api_active_users():
    """API: 获取活跃用户列表"""
    limit = request.args.get('limit', 20, type=int)
    users = recommend_service.get_active_users(limit)
    return jsonify({'success': True, 'data': users})


@recommend_bp.route('/api/search-users')
def api_search_users():
    """API: 搜索用户"""
    keyword = request.args.get('keyword', '')
    limit = request.args.get('limit', 20, type=int)
    users = user_service.search_users(keyword, limit)
    return jsonify({'success': True, 'data': users})

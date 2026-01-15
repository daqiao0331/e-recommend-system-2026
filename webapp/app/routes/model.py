# -*- coding: utf-8 -*-
"""
模型管理路由
"""
from flask import Blueprint, render_template, jsonify, request, current_app
from ..services.model_service import model_service
from ..services import spark_service

model_bp = Blueprint('model', __name__)


@model_bp.route('/')
def index():
    """模型管理主页"""
    # 获取模型信息
    model_info = model_service.get_model_info()
    
    # 获取训练状态
    training_status = model_service.get_training_status()
    
    # 获取训练数据统计
    data_stats = model_service.get_training_data_stats()
    
    return render_template('model.html',
                         model_info=model_info,
                         training_status=training_status,
                         data_stats=data_stats)


@model_bp.route('/train', methods=['POST'])
def train_model():
    """启动模型训练"""
    base_dir = current_app.config.get('BASE_DIR')
    result = model_service.start_training(base_dir)
    return jsonify(result)


@model_bp.route('/status')
def training_status():
    """获取训练状态"""
    status = model_service.get_training_status()
    return jsonify({'success': True, 'data': status})


@model_bp.route('/info')
def model_info():
    """获取模型信息"""
    info = model_service.get_model_info()
    return jsonify({'success': True, 'data': info})


@model_bp.route('/reload', methods=['POST'])
def reload_model():
    """重新加载模型"""
    try:
        spark_service.reload_model()
        return jsonify({'success': True, 'message': '模型重新加载成功'})
    except Exception as e:
        return jsonify({'success': False, 'message': f'加载失败: {str(e)}'})


# ============== API 接口 ==============

@model_bp.route('/api/info')
def api_model_info():
    """API: 获取模型信息"""
    info = model_service.get_model_info()
    return jsonify({'success': True, 'data': info})


@model_bp.route('/api/status')
def api_training_status():
    """API: 获取训练状态"""
    status = model_service.get_training_status()
    return jsonify({'success': True, 'data': status})


@model_bp.route('/api/data-stats')
def api_data_stats():
    """API: 获取训练数据统计"""
    stats = model_service.get_training_data_stats()
    return jsonify({'success': True, 'data': stats})

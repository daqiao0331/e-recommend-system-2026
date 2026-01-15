# -*- coding: utf-8 -*-
"""
Flask 应用工厂
"""
from flask import Flask
from .config import config


def create_app(config_name='default'):
    """创建 Flask 应用"""
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    
    # 注册蓝图
    from .routes.main import main_bp
    from .routes.recommend import recommend_bp
    from .routes.analysis import analysis_bp
    from .routes.model import model_bp
    
    app.register_blueprint(main_bp)
    app.register_blueprint(recommend_bp, url_prefix='/recommend')
    app.register_blueprint(analysis_bp, url_prefix='/analysis')
    app.register_blueprint(model_bp, url_prefix='/model')
    
    return app

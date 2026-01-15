# -*- coding: utf-8 -*-
"""
应用配置
"""
import os

class Config:
    """基础配置"""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'ecommerce-recommender-secret'
    
    # 项目根目录
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    
    # Spark 配置
    SPARK_MASTER = os.environ.get('SPARK_MASTER') or 'yarn'
    SPARK_APP_NAME = 'EcommerceWebApp'
    
    # Hive 配置
    HIVE_DATABASE = 'ecommerce'
    
    # 模型路径
    ALS_MODEL_PATH = '/user/ecommerce/model/als_model'
    
    # 输出目录
    OUTPUT_DIR = os.path.join(BASE_DIR, 'output')
    RESULTS_DIR = os.path.join(OUTPUT_DIR, 'recommendations')
    
    # 训练脚本路径
    TRAIN_SCRIPT = os.path.join(BASE_DIR, 'spark', '03_train_als_model.py')


class DevelopmentConfig(Config):
    """开发环境"""
    DEBUG = True


class ProductionConfig(Config):
    """生产环境"""
    DEBUG = False


config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

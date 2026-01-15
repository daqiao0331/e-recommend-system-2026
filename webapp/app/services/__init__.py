# -*- coding: utf-8 -*-
"""
Spark 服务封装 - 单例模式管理 SparkSession
"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
import os

# 设置 Spark 环境变量
os.environ.setdefault('SPARK_HOME', '/opt/spark')
os.environ.setdefault('HADOOP_CONF_DIR', '/opt/hadoop/etc/hadoop')
os.environ.setdefault('YARN_CONF_DIR', '/opt/hadoop/etc/hadoop')


class SparkService:
    """Spark 服务类"""
    _instance = None
    _spark = None
    _model = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_spark(self):
        """获取或创建 SparkSession"""
        if self._spark is None:
            try:
                self._spark = SparkSession.builder \
                    .appName("EcommerceWebApp") \
                    .enableHiveSupport() \
                    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                    .config("spark.driver.host", "localhost") \
                    .getOrCreate()
                self._spark.sparkContext.setLogLevel("ERROR")
            except Exception as e:
                print(f"Spark initialization error: {e}")
                return None
        return self._spark
    
    def execute_query(self, sql: str):
        """执行 Hive 查询"""
        spark = self.get_spark()
        if spark is None:
            return []
        try:
            result = spark.sql(sql).collect()
            return result
        except Exception as e:
            print(f"Query error: {e}")
            return []
    
    def execute_query_df(self, sql: str):
        """执行 Hive 查询，返回 DataFrame"""
        spark = self.get_spark()
        if spark is None:
            return None
        return spark.sql(sql)
    
    def load_als_model(self, model_path='/user/ecommerce/model/als_model'):
        """加载 ALS 推荐模型"""
        if self._model is None:
            try:
                self._model = ALSModel.load(model_path)
            except Exception as e:
                print(f"Model load error: {e}")
                return None
        return self._model
    
    def reload_model(self, model_path='/user/ecommerce/model/als_model'):
        """重新加载模型"""
        self._model = None
        return self.load_als_model(model_path)
    
    def check_model_exists(self, model_path='/user/ecommerce/model/als_model'):
        """检查模型是否存在"""
        spark = self.get_spark()
        if spark is None:
            return False
        try:
            # 检查 HDFS 路径是否存在
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path(model_path)
            return fs.exists(path)
        except:
            return False
    
    def get_model_info(self, model_path='/user/ecommerce/model/als_model'):
        """获取模型信息"""
        spark = self.get_spark()
        if spark is None:
            return {'exists': False, 'path': model_path, 'error': 'Spark未连接'}
        try:
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path(model_path)
            if fs.exists(path):
                status = fs.getFileStatus(path)
                return {
                    'exists': True,
                    'path': model_path,
                    'modification_time': status.getModificationTime(),
                    'size': status.getLen()
                }
        except Exception as e:
            print(f"Error getting model info: {e}")
        return {'exists': False, 'path': model_path}


# 全局单例
spark_service = SparkService()

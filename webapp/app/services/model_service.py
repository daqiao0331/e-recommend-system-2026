# -*- coding: utf-8 -*-
"""
模型服务 - 模型训练和管理
"""
from . import spark_service
import subprocess
import os
import threading
from datetime import datetime


class ModelService:
    """模型服务类"""
    
    def __init__(self):
        self.spark_service = spark_service
        self.training_status = {
            'is_training': False,
            'start_time': None,
            'end_time': None,
            'status': 'idle',  # idle, training, success, failed
            'message': '',
            'log': []
        }
        self._lock = threading.Lock()
    
    def get_model_info(self):
        """获取模型信息"""
        model_path = '/user/ecommerce/model/als_model'
        info = self.spark_service.get_model_info(model_path)
        
        if info.get('exists'):
            # 转换时间戳
            mod_time = info.get('modification_time', 0)
            if mod_time:
                info['modification_time_str'] = datetime.fromtimestamp(mod_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            else:
                info['modification_time_str'] = '未知'
        
        return info
    
    def get_training_status(self):
        """获取训练状态"""
        with self._lock:
            return self.training_status.copy()
    
    def start_training(self, base_dir: str):
        """启动模型训练（异步）"""
        with self._lock:
            if self.training_status['is_training']:
                return {'success': False, 'message': '模型正在训练中，请稍候'}
            
            self.training_status = {
                'is_training': True,
                'start_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': None,
                'status': 'training',
                'message': '模型训练已启动...',
                'log': ['训练开始...']
            }
        
        # 在新线程中执行训练
        train_script = os.path.join(base_dir, 'spark', '03_train_als_model.py')
        thread = threading.Thread(target=self._run_training, args=(train_script,))
        thread.daemon = True
        thread.start()
        
        return {'success': True, 'message': '模型训练已启动'}
    
    def _run_training(self, train_script: str):
        """执行训练（在线程中运行）"""
        try:
            # 使用 spark-submit 执行训练脚本
            cmd = [
                'spark-submit',
                '--master', 'yarn',
                '--driver-memory', '4g',
                '--executor-memory', '4g',
                '--num-executors', '2',
                train_script
            ]
            
            with self._lock:
                self.training_status['log'].append(f'执行命令: {" ".join(cmd)}')
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            
            # 读取输出
            for line in process.stdout:
                line = line.strip()
                if line:
                    with self._lock:
                        # 只保留最近100行日志
                        if len(self.training_status['log']) > 100:
                            self.training_status['log'] = self.training_status['log'][-50:]
                        self.training_status['log'].append(line)
            
            process.wait()
            
            with self._lock:
                if process.returncode == 0:
                    self.training_status['status'] = 'success'
                    self.training_status['message'] = '模型训练成功完成'
                    # 重新加载模型
                    self.spark_service.reload_model()
                else:
                    self.training_status['status'] = 'failed'
                    self.training_status['message'] = f'训练失败，返回码: {process.returncode}'
                
                self.training_status['is_training'] = False
                self.training_status['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
        except Exception as e:
            with self._lock:
                self.training_status['is_training'] = False
                self.training_status['status'] = 'failed'
                self.training_status['message'] = f'训练出错: {str(e)}'
                self.training_status['end_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                self.training_status['log'].append(f'错误: {str(e)}')
    
    def get_model_metrics(self):
        """获取模型评估指标（如果有保存的话）"""
        # 可以从 HDFS 或本地读取之前训练时保存的评估指标
        try:
            spark = self.spark_service.get_spark()
            # 尝试读取评估结果
            metrics_path = '/user/ecommerce/model/als_metrics'
            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
            path = spark._jvm.org.apache.hadoop.fs.Path(metrics_path)
            
            if fs.exists(path):
                # 读取指标文件
                return {'exists': True, 'path': metrics_path}
        except:
            pass
        return {'exists': False}
    
    def get_training_data_stats(self):
        """获取训练数据统计"""
        try:
            # 用户数
            user_count = self.spark_service.execute_query(
                "SELECT COUNT(*) as cnt FROM ecommerce.user_id_mapping"
            )
            
            # 商品数
            item_count = self.spark_service.execute_query(
                "SELECT COUNT(*) as cnt FROM ecommerce.item_id_mapping"
            )
            
            # 评分数据量
            rating_count = self.spark_service.execute_query("""
                SELECT COUNT(*) as cnt
                FROM ecommerce.user_events
                WHERE visitor_id IN (SELECT original_user_id FROM ecommerce.user_id_mapping)
            """)
            
            return {
                'user_count': user_count[0]['cnt'] if user_count else 0,
                'item_count': item_count[0]['cnt'] if item_count else 0,
                'rating_count': rating_count[0]['cnt'] if rating_count else 0
            }
        except:
            return {'user_count': 0, 'item_count': 0, 'rating_count': 0}


# 全局实例
model_service = ModelService()

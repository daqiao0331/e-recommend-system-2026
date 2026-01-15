# Flask 前端技术设计文档

## 📋 项目概述

基于现有的 **电子商务推荐系统**（Hadoop + Spark + ALS），开发一个 Web 前端界面，提供可视化的用户交互体验。

### 目标
- 提供用户友好的商品推荐查询界面
- 展示用户画像和行为分析
- 可视化数据统计和推荐结果
- 支持实时推荐查询

---

## 🏗️ 系统架构

```
┌─────────────────────────────────────────────────────────────────────┐
│                        前端层 (Frontend)                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │  HTML/CSS   │  │  Bootstrap  │  │    Jinja2   │                  │
│  │  模板引擎   │  │  响应式UI   │  │   模板渲染   │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        应用层 (Flask)                                │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │   Routes    │  │   Services  │  │    Models   │                  │
│  │   路由控制   │  │   业务逻辑   │  │   数据模型   │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        数据层 (Data Layer)                           │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                  │
│  │    Hive     │  │  ALS Model  │  │    HDFS     │                  │
│  │  用户数据   │  │   推荐模型   │  │   文件存储   │                  │
│  └─────────────┘  └─────────────┘  └─────────────┘                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 📁 项目结构

```
webapp/
├── app/
│   ├── __init__.py              # Flask 应用初始化
│   ├── config.py                # 配置文件
│   ├── routes/                  # 路由模块
│   │   ├── __init__.py
│   │   ├── main.py              # 主页路由
│   │   ├── recommend.py         # 推荐相关路由
│   │   ├── user.py              # 用户相关路由
│   │   └── analysis.py          # 数据分析路由
│   ├── services/                # 业务逻辑层
│   │   ├── __init__.py
│   │   ├── spark_service.py     # Spark 服务封装
│   │   ├── recommend_service.py # 推荐服务
│   │   ├── user_service.py      # 用户服务
│   │   └── analysis_service.py  # 分析服务
│   ├── models/                  # 数据模型
│   │   ├── __init__.py
│   │   └── entities.py          # 实体类定义
│   ├── templates/               # Jinja2 模板
│   │   ├── base.html            # 基础模板
│   │   ├── index.html           # 首页
│   │   ├── recommend.html       # 推荐页面
│   │   ├── user_profile.html    # 用户画像
│   │   ├── analysis.html        # 数据分析
│   │   └── components/          # 可复用组件
│   │       ├── navbar.html
│   │       ├── sidebar.html
│   │       └── footer.html
│   └── static/                  # 静态资源
│       ├── css/
│       │   └── style.css
│       ├── js/
│       │   └── main.js
│       └── images/
├── tests/                       # 测试目录
│   ├── __init__.py
│   └── test_routes.py
├── requirements.txt             # Python 依赖
├── run.py                       # 启动脚本
└── README.md                    # 项目说明
```

---

## 🔧 技术栈详情

### 后端框架
| 组件 | 版本 | 用途 |
|------|------|------|
| Flask | 3.0+ | Web 框架 |
| Flask-Bootstrap | 5.x | UI 组件库 |
| Jinja2 | 3.x | 模板引擎 |
| PySpark | 3.x | Spark 连接 |
| PyHive | 0.7+ | Hive 连接 |

### 前端技术
| 组件 | 版本 | 用途 |
|------|------|------|
| Bootstrap | 5.3 | 响应式 UI |
| ECharts | 5.x | 数据可视化 |
| jQuery | 3.x | DOM 操作 |
| Font Awesome | 6.x | 图标库 |

### 依赖列表 (requirements.txt)
```txt
# Flask 核心
flask>=3.0.0
flask-bootstrap>=5.0.0
flask-wtf>=1.2.0

# 数据库连接
pyhive>=0.7.0
thrift>=0.16.0
sasl>=0.3.1
thrift-sasl>=0.4.3

# Spark 集成
pyspark>=3.5.0

# 工具库
python-dotenv>=1.0.0
gunicorn>=21.0.0
```

---

## 🎯 功能模块设计

### 核心功能需求
1. **可视化数据查询** - 数据统计、图表展示、多维分析
2. **模型学习** - 触发 ALS 模型训练、查看训练状态
3. **推理结果显示** - 展示推荐结果、推荐理由
4. **结果保存** - 保存推荐结果到文件/数据库

---

### 1. 首页 (Dashboard)
**路由**: `/`

**功能**:
- 系统概览统计卡片（用户数、商品数、事件数）
- 热门商品 TOP10 排行榜
- 模型状态显示
- 快速搜索入口

**数据接口**:
```python
@main_bp.route('/')
def index():
    stats = analysis_service.get_overview_stats()
    hot_items = analysis_service.get_hot_items(limit=10)
    return render_template('index.html', stats=stats, hot_items=hot_items)
```

---

### 2. 推荐查询 (Recommend)
**路由**: `/recommend`

**功能**:
- 用户 ID 输入框
- 推荐商品列表展示
- 推荐分数可视化
- 推荐理由说明

**数据接口**:
```python
@recommend_bp.route('/recommend', methods=['GET', 'POST'])
def get_recommendation():
    if request.method == 'POST':
        user_id = request.form.get('user_id')
        recommendations = recommend_service.get_recommendations(user_id, top_n=10)
        user_profile = user_service.get_user_profile(user_id)
        return render_template('recommend.html', 
                             recommendations=recommendations,
                             user_profile=user_profile)
    return render_template('recommend.html')
```

**API 接口** (JSON):
```python
@recommend_bp.route('/api/recommend/<user_id>')
def api_recommend(user_id):
    recommendations = recommend_service.get_recommendations(user_id, top_n=10)
    return jsonify({
        'user_id': user_id,
        'recommendations': recommendations,
        'timestamp': datetime.now().isoformat()
    })
```

---

### 3. 用户画像 (User Profile)
**路由**: `/user/<user_id>`

**功能**:
- 用户基本信息展示
- 行为统计（浏览/加购/购买次数）
- 行为时间分布图
- 偏好类目分析
- 最近浏览记录

**数据结构**:
```python
class UserProfile:
    user_id: str
    total_events: int
    view_count: int
    cart_count: int
    purchase_count: int
    favorite_categories: List[str]
    recent_items: List[dict]
    activity_timeline: List[dict]
```

---

### 4. 数据分析 (Analysis)
**路由**: `/analysis`

**功能**:
- 整体数据统计
- 用户活跃度分布图
- 转化漏斗图
- 热门商品排行
- 类目分布饼图

**可视化图表** (ECharts):
```javascript
// 转化漏斗图
option = {
    title: { text: '用户转化漏斗' },
    series: [{
        type: 'funnel',
        data: [
            { value: 1000000, name: '浏览' },
            { value: 200000, name: '加购' },
            { value: 50000, name: '购买' }
        ]
    }]
};
```

---

### 5. 随机推荐 (Random Recommend)
**路由**: `/random`

**功能**:
- 一键随机选择用户
- 自动展示推荐结果
- 支持刷新重新随机

---

## 📊 核心服务实现

### SparkService - Spark 连接服务

```python
# app/services/spark_service.py
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from contextlib import contextmanager

class SparkService:
    _instance = None
    _spark = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def get_spark(self):
        """获取或创建 SparkSession"""
        if self._spark is None:
            self._spark = SparkSession.builder \
                .appName("EcommerceWebApp") \
                .enableHiveSupport() \
                .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
                .getOrCreate()
            self._spark.sparkContext.setLogLevel("ERROR")
        return self._spark
    
    def execute_query(self, sql: str):
        """执行 Hive 查询"""
        spark = self.get_spark()
        return spark.sql(sql).collect()
    
    def load_als_model(self):
        """加载 ALS 推荐模型"""
        return ALSModel.load("/user/ecommerce/model/als_model")
```

### RecommendService - 推荐服务

```python
# app/services/recommend_service.py
from pyspark.sql.functions import col, explode

class RecommendService:
    def __init__(self, spark_service):
        self.spark_service = spark_service
        self._model = None
        self._item_map = None
    
    @property
    def model(self):
        if self._model is None:
            self._model = self.spark_service.load_als_model()
        return self._model
    
    @property
    def item_map(self):
        if self._item_map is None:
            spark = self.spark_service.get_spark()
            item_df = spark.sql("SELECT item_id, original_item_id FROM ecommerce.item_id_mapping")
            self._item_map = {r['item_id']: r['original_item_id'] for r in item_df.collect()}
        return self._item_map
    
    def get_recommendations(self, visitor_id: str, top_n: int = 10):
        """获取用户推荐"""
        spark = self.spark_service.get_spark()
        
        # 获取映射后的用户ID
        user_map = spark.sql(f"""
            SELECT user_id FROM ecommerce.user_id_mapping
            WHERE original_user_id = '{visitor_id}'
        """).collect()
        
        if not user_map:
            return None
        
        user_id = user_map[0]['user_id']
        
        # 生成推荐
        user_df = spark.createDataFrame([(user_id,)], ["user_id"])
        recs = self.model.recommendForUserSubset(user_df, top_n)
        
        # 解析结果
        rec_list = recs.select(explode(col("recommendations")).alias("rec")).select(
            col("rec.item_id"), col("rec.rating")
        ).collect()
        
        # 转换为原始商品ID
        recommendations = []
        for r in rec_list:
            original_item = self.item_map.get(r['item_id'], r['item_id'])
            recommendations.append({
                'item_id': original_item,
                'score': round(r['rating'], 4)
            })
        
        return recommendations
    
    def get_random_user(self):
        """获取随机用户ID"""
        result = self.spark_service.execute_query("""
            SELECT original_user_id
            FROM ecommerce.user_id_mapping
            ORDER BY RAND()
            LIMIT 1
        """)
        return result[0]['original_user_id'] if result else None
```

### UserService - 用户服务

```python
# app/services/user_service.py

class UserService:
    def __init__(self, spark_service):
        self.spark_service = spark_service
    
    def get_user_profile(self, visitor_id: str):
        """获取用户画像"""
        spark = self.spark_service.get_spark()
        
        # 行为统计
        stats = spark.sql(f"""
            SELECT 
                event_type,
                COUNT(*) as count,
                COUNT(DISTINCT item_id) as unique_items
            FROM ecommerce.user_events
            WHERE visitor_id = '{visitor_id}'
            GROUP BY event_type
        """).collect()
        
        if not stats:
            return None
        
        profile = {
            'user_id': visitor_id,
            'view_count': 0,
            'cart_count': 0,
            'purchase_count': 0,
            'view_items': 0,
            'cart_items': 0,
            'purchase_items': 0
        }
        
        for s in stats:
            if s['event_type'] == 'view':
                profile['view_count'] = s['count']
                profile['view_items'] = s['unique_items']
            elif s['event_type'] == 'addtocart':
                profile['cart_count'] = s['count']
                profile['cart_items'] = s['unique_items']
            elif s['event_type'] == 'transaction':
                profile['purchase_count'] = s['count']
                profile['purchase_items'] = s['unique_items']
        
        profile['total_events'] = profile['view_count'] + profile['cart_count'] + profile['purchase_count']
        
        return profile
    
    def get_recent_items(self, visitor_id: str, limit: int = 10):
        """获取用户最近浏览的商品"""
        result = self.spark_service.execute_query(f"""
            SELECT 
                e.item_id,
                e.event_type,
                e.event_time,
                p.property_value as category_id
            FROM ecommerce.user_events e
            LEFT JOIN ecommerce.item_properties p 
                ON e.item_id = p.item_id AND p.property_name = 'categoryid'
            WHERE e.visitor_id = '{visitor_id}'
            ORDER BY e.event_time DESC
            LIMIT {limit}
        """)
        return result
    
    def search_users(self, keyword: str = None, limit: int = 20):
        """搜索用户"""
        if keyword:
            query = f"""
                SELECT DISTINCT original_user_id
                FROM ecommerce.user_id_mapping
                WHERE original_user_id LIKE '%{keyword}%'
                LIMIT {limit}
            """
        else:
            query = f"""
                SELECT original_user_id
                FROM ecommerce.user_id_mapping
                LIMIT {limit}
            """
        return self.spark_service.execute_query(query)
```

### AnalysisService - 数据分析服务

```python
# app/services/analysis_service.py

class AnalysisService:
    def __init__(self, spark_service):
        self.spark_service = spark_service
    
    def get_overview_stats(self):
        """获取系统概览统计"""
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
        return {}
    
    def get_event_distribution(self):
        """获取事件类型分布"""
        return self.spark_service.execute_query("""
            SELECT 
                event_type,
                COUNT(*) as count
            FROM ecommerce.user_events
            GROUP BY event_type
            ORDER BY count DESC
        """)
    
    def get_hot_items(self, limit: int = 20):
        """获取热门商品"""
        return self.spark_service.execute_query(f"""
            SELECT 
                item_id,
                COUNT(*) as view_count,
                COUNT(DISTINCT visitor_id) as unique_visitors
            FROM ecommerce.user_events
            WHERE event_type = 'view'
            GROUP BY item_id
            ORDER BY view_count DESC
            LIMIT {limit}
        """)
    
    def get_conversion_funnel(self):
        """获取转化漏斗数据"""
        return self.spark_service.execute_query("""
            SELECT 'view' as stage, COUNT(DISTINCT visitor_id) as users
            FROM ecommerce.user_events WHERE event_type = 'view'
            UNION ALL
            SELECT 'addtocart', COUNT(DISTINCT visitor_id)
            FROM ecommerce.user_events WHERE event_type = 'addtocart'
            UNION ALL
            SELECT 'transaction', COUNT(DISTINCT visitor_id)
            FROM ecommerce.user_events WHERE event_type = 'transaction'
        """)
    
    def get_user_activity_distribution(self):
        """获取用户活跃度分布"""
        return self.spark_service.execute_query("""
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
```

---

## 🎨 页面设计规范

### 配色方案
```css
:root {
    --primary-color: #4e73df;      /* 主色调 - 蓝色 */
    --success-color: #1cc88a;      /* 成功 - 绿色 */
    --warning-color: #f6c23e;      /* 警告 - 黄色 */
    --danger-color: #e74a3b;       /* 危险 - 红色 */
    --secondary-color: #858796;    /* 次要 - 灰色 */
    --background-color: #f8f9fc;   /* 背景色 */
    --card-shadow: 0 0.15rem 1.75rem 0 rgba(58, 59, 69, 0.15);
}
```

### 响应式断点
- 桌面端: ≥ 1200px
- 平板端: 768px - 1199px
- 移动端: < 768px

### UI 组件
- 导航栏: 固定顶部，包含 Logo、菜单、搜索框
- 侧边栏: 可折叠，包含功能导航
- 统计卡片: 展示关键指标
- 数据表格: 支持排序、分页
- 图表: ECharts 可视化

---

## 🔌 API 设计

### RESTful API 端点

| 方法 | 端点 | 描述 |
|------|------|------|
| GET | `/api/stats` | 获取系统统计 |
| GET | `/api/recommend/<user_id>` | 获取用户推荐 |
| GET | `/api/user/<user_id>` | 获取用户画像 |
| GET | `/api/user/<user_id>/history` | 获取用户历史 |
| GET | `/api/items/hot` | 获取热门商品 |
| GET | `/api/analysis/funnel` | 获取转化漏斗 |
| GET | `/api/random-user` | 获取随机用户 |

### 响应格式
```json
{
    "success": true,
    "data": { ... },
    "message": "success",
    "timestamp": "2026-01-14T10:30:00"
}
```

---

## ⚙️ 配置管理

### 环境配置 (config.py)
```python
import os

class Config:
    """基础配置"""
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'dev-secret-key'
    
    # Spark 配置
    SPARK_MASTER = os.environ.get('SPARK_MASTER') or 'yarn'
    SPARK_APP_NAME = 'EcommerceWebApp'
    
    # Hive 配置
    HIVE_DATABASE = 'ecommerce'
    
    # 模型路径
    ALS_MODEL_PATH = '/user/ecommerce/model/als_model'
    
    # 缓存配置
    CACHE_TYPE = 'simple'
    CACHE_DEFAULT_TIMEOUT = 300

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
```

---

## 🚀 部署方案

### 开发环境启动
```bash
# 1. 创建虚拟环境
cd webapp
python -m venv venv
source venv/bin/activate

# 2. 安装依赖
pip install -r requirements.txt

# 3. 启动开发服务器
export FLASK_APP=run.py
export FLASK_ENV=development
flask run --host=0.0.0.0 --port=5000
```

### 生产环境部署 (Gunicorn + Nginx)
```bash
# 使用 Gunicorn 启动
gunicorn -w 4 -b 0.0.0.0:5000 run:app

# Nginx 反向代理配置
server {
    listen 80;
    server_name your-domain.com;
    
    location / {
        proxy_pass http://127.0.0.1:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
    
    location /static {
        alias /path/to/webapp/app/static;
    }
}
```

---

## 📋 开发计划

### ✅ 已完成功能

#### 1. 可视化数据查询
- [x] 系统概览统计（用户数、商品数、事件数）
- [x] 事件类型分布饼图
- [x] 用户转化漏斗图
- [x] 用户活跃度分布图
- [x] 热门商品排行榜
- [x] 热门类目分布图

#### 2. 模型学习
- [x] 模型状态显示
- [x] 训练数据统计
- [x] 一键触发模型训练
- [x] 实时训练日志显示
- [x] 训练状态轮询更新
- [x] 模型重新加载

#### 3. 推理结果显示
- [x] 用户ID输入查询
- [x] 推荐商品列表展示
- [x] 推荐分数可视化
- [x] 用户画像展示
- [x] 最近行为记录
- [x] 随机用户推荐
- [x] 活跃用户快速选择

#### 4. 结果保存
- [x] 推荐结果保存为 JSON 文件
- [x] 保存路径显示

---

## 📋 原开发计划

### 第一阶段：基础框架 (2天)
- [ ] 项目结构搭建
- [ ] Flask 应用初始化
- [ ] Spark 服务封装
- [ ] 基础模板创建

### 第二阶段：核心功能 (3天)
- [ ] 推荐查询页面
- [ ] 用户画像页面
- [ ] 数据分析页面
- [ ] API 接口实现

### 第三阶段：优化完善 (2天)
- [ ] UI 美化
- [ ] 响应式适配
- [ ] 性能优化
- [ ] 错误处理

### 第四阶段：测试部署 (1天)
- [ ] 单元测试
- [ ] 集成测试
- [ ] 部署文档

---

## 📝 注意事项

### 性能考虑
1. **Spark 连接池**: 使用单例模式管理 SparkSession，避免重复创建
2. **结果缓存**: 对热门数据使用 Flask-Caching 缓存
3. **分页加载**: 大数据量使用分页，避免一次加载过多
4. **异步查询**: 长时间查询可考虑使用 Celery 异步处理

### 安全考虑
1. **SQL 注入防护**: 使用参数化查询
2. **输入验证**: 验证用户输入的 user_id 格式
3. **错误处理**: 不向用户暴露系统内部错误信息

### 兼容性
1. 确保 PySpark 版本与集群 Spark 版本一致
2. 使用 Python 3.8+ 版本
3. 测试不同浏览器兼容性

---

## 📚 参考资料

- [Flask 官方文档](https://flask.palletsprojects.com/)
- [Bootstrap 5 文档](https://getbootstrap.com/)
- [ECharts 文档](https://echarts.apache.org/)
- [PySpark 文档](https://spark.apache.org/docs/latest/api/python/)
- [Jinja2 模板文档](https://jinja.palletsprojects.com/)

---

**文档版本**: v1.0  
**创建日期**: 2026-01-14  
**作者**: ERecommender Team

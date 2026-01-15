# Model Directory

存放训练好的ALS推荐模型和相关映射表。

## 目录结构

```
model/
├── als_model_YYYYMMDD_HHMMSS/     # ALS模型文件
│   ├── metadata/
│   ├── itemFactors/
│   └── userFactors/
└── mappings/                       # ID映射表
    ├── user_id_mapping/           # 用户ID映射
    └── item_id_mapping/           # 商品ID映射
```

## 下载模型

运行以下脚本从HDFS下载最新训练的模型：

```bash
./scripts/download_model.sh
```

## 使用模型

加载本地模型进行推荐：

```python
from pyspark.ml.recommendation import ALSModel

# 加载模型
model = ALSModel.load("model/als_model_20260115_091234")

# 为用户生成推荐
recommendations = model.recommendForAllUsers(10)
```

## 注意事项

- 模型文件采用Parquet格式存储
- 需要相同版本的Spark才能加载模型
- ID映射表用于将模型的数值ID转换回原始字符串ID

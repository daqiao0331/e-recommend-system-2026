# 推荐系统预测使用指南

## 1. 快速开始

### 方式1: 单用户实时推荐 (推荐)
```bash
spark-submit spark/recommend.py <用户ID>
```

**示例：**
```bash
# 为用户1000640生成推荐
spark-submit spark/recommend.py 1000640

# 输出:
# ======================================================================
#   用户 1000640 的推荐系统
# ======================================================================
# 
# 【用户历史】
#   view        :    5 次 (100.0%)  |  5 个商品
# 
# 【最近浏览】
#   1. 商品 294109  (view)
#   2. 商品 460187  (view)
#   ...
# 
# 【推荐结果】
#   排名       商品ID            推荐分数
#   ----------------------------------------
#   1        231482            0.0571
#   2        455183            0.0529
#   ...
```

### 方式2: 批量测试多个用户
```bash
# 测试3个用户
for uid in 1000640 1001728 1004781; do
    spark-submit spark/recommend.py $uid 2>&1 | grep -v "INFO\|WARN"
done
```

### 方式3: 查看用户画像 (Hive)
```bash
./scripts/recommend.sh <用户ID>
```

---

## 2. 获取可用的用户ID

系统只能为训练集中的用户生成推荐。获取可用用户ID的方法：

### 方法1: 从映射表查询
```bash
hive -e "SELECT original_user_id FROM ecommerce.user_id_mapping LIMIT 20;"
```

### 方法2: 查询活跃用户
```python
# 创建查询脚本
cat > /tmp/active_users.py << 'EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("ActiveUsers").enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 查询有购买行为的用户（推荐质量最好）
users = spark.sql("""
    SELECT u.original_user_id, COUNT(*) as events
    FROM ecommerce.user_events e
    JOIN ecommerce.user_id_mapping u ON e.visitor_id = u.original_user_id
    WHERE e.event_type = 'transaction'
    GROUP BY u.original_user_id
    ORDER BY events DESC
    LIMIT 20
""")
users.show(truncate=False)
spark.stop()
EOF

spark-submit /tmp/active_users.py
```

---

## 3. 推荐系统工作原理

### 模型类型
- **ALS协同过滤** (Alternating Least Squares)
- 基于用户-商品交互矩阵
- 隐式反馈 (浏览、加购、购买)

### 评分权重
- 浏览(view): 1分
- 加购(addtocart): 3分
- 购买(transaction): 5分

### 推荐逻辑
1. **用户画像**: 分析历史行为（浏览、加购、购买）
2. **协同过滤**: 找到相似用户喜欢的商品
3. **排序输出**: 按预测评分从高到低排序

---

## 4. 测试样例

### 示例用户ID（训练集中）
```
1000640  - 5次浏览
1001728  - 9次浏览  
1004781  - 4次浏览
1006721  - 8次浏览
1008732  - 有购买行为的用户
1010247  - 有加购行为的用户
```

### 完整测试脚本
```bash
#!/bin/bash
# 测试推荐系统

echo "=== 推荐系统测试 ==="
echo ""

# 测试5个不同类型的用户
USERS="1000640 1001728 1004781 1006721 1008732"

for USER_ID in $USERS; do
    echo ">>> 测试用户: $USER_ID"
    spark-submit spark/recommend.py $USER_ID 2>&1 | grep -v "INFO\|WARN\|^26/"
    echo ""
    echo "----------------------------------------"
    echo ""
done

echo "=== 测试完成 ==="
```

---

## 5. 推荐质量说明

### 推荐分数含义
- **>0.5**: 强推荐（用户很可能喜欢）
- **0.1-0.5**: 中等推荐（值得尝试）
- **<0.1**: 弱推荐（可能感兴趣）

### 影响推荐质量的因素
1. **用户活跃度**: 交互越多，推荐越准
2. **行为类型**: 购买>加购>浏览
3. **商品热度**: 热门商品推荐分更高
4. **协同效应**: 相似用户越多，推荐越准

### 当前模型性能
```
Precision@10:  1.16%   (推荐命中率)
Recall@10:     7.93%   (召回率)
Hit Rate:      10.92%  (用户命中比例)
NDCG@10:       5.23%   (排序质量)
```

---

## 6. 进阶用法

### 修改推荐数量
编辑 [spark/recommend.py](spark/recommend.py#L82):
```python
# 将10改为你想要的数量
recs = model.recommendForUserSubset(user_df, 20)  # 推荐20个商品
```

### 过滤已购买商品
```python
# 在recommend.py中添加过滤逻辑
purchased = spark.sql(f"""
    SELECT DISTINCT item_id 
    FROM ecommerce.user_events 
    WHERE visitor_id = '{visitor_id}' AND event_type = 'transaction'
""")
# 过滤推荐结果...
```

### 自定义评分权重
修改 [spark/02_data_preprocessing.py](spark/02_data_preprocessing.py) 中的权重：
```python
rating_weight = {
    'view': 1,
    'addtocart': 5,      # 调高加购权重
    'transaction': 10    # 调高购买权重
}
```
然后重新训练模型。

---

## 7. 常见问题

### Q: 提示"用户不在训练集"？
**A:** 该用户交互太少（<5次）被过滤了。尝试其他活跃用户。

### Q: 推荐分数为什么这么低？
**A:** 这是正常的。ALS模型输出的是预测评分，不是概率。关注相对排序，不是绝对值。

### Q: 如何获取新用户的推荐？
**A:** 冷启动问题：
1. 推荐热门商品（基于统计）
2. 推荐同类目商品（基于内容）
3. 收集用户行为后重新训练

### Q: 推荐结果可以保存吗？
**A:** 可以修改脚本保存到文件或数据库：
```python
# 保存到CSV
rec_df.write.csv(f"/tmp/recommendations_{visitor_id}.csv")

# 保存到Hive表
rec_df.write.mode("append").saveAsTable("ecommerce.user_recommendations")
```

---

## 8. 性能优化建议

### 批量推荐（生产环境）
对于大规模用户，使用批量推荐而非单个查询：
```python
# 为所有用户批量生成推荐
user_recs = model.recommendForAllUsers(10)
user_recs.write.saveAsTable("ecommerce.all_recommendations")
```

### 推荐缓存
将推荐结果缓存到Redis/MySQL，避免实时计算：
```python
# 定期（如每天）更新推荐
# 用户查询时直接从缓存读取
```

---

## 相关文件
- **推荐脚本**: [spark/recommend.py](spark/recommend.py)
- **完整预测**: [spark/05_recommend_predict.py](spark/05_recommend_predict.py)
- **Hive查询**: [scripts/recommend.sh](scripts/recommend.sh)
- **训练脚本**: [spark/03_train_als_model.py](spark/03_train_als_model.py)

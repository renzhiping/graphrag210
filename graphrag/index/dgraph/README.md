# GraphRAG DGraph 初始化与数据导入工具

本工具包含两个主要功能：初始化DGraph Schema (`init_schema.py`) 和数据导入 (`import_data.py`)，用于构建和填充知识图谱数据库。

## 环境要求

- Python 3.8+
- 运行中的DGraph服务器 (默认地址: localhost:9080)
- 安装以下依赖库:
  - pydgraph
  - pandas
  - numpy

## 1. 初始化Schema (init_schema.py)

`init_schema.py` 用于在DGraph数据库中创建和设置Schema，定义GraphRAG所需的各类节点和关系。

### 支持的Schema类型

- `text_unit`: 文本单元，知识图谱的基本信息单位
- `document`: 文档，包含多个文本单元的集合
- `entity`: 实体，如人物、组织、地点等
- `relationship`: 实体间的关系
- `community`: 社区，实体的集合
- `community_report`: 社区报告，针对社区的分析结果
- `complete`: 包含所有上述Schema类型

### 命令行使用

```bash
python -m graphrag.index.dgraph.init_schema [options]
```

#### 命令行参数

- `--dgraph-host`: DGraph服务器地址，默认为`localhost:9080`
- `--schema-type`: Schema类型，可选值为上述支持的类型，默认为`complete`
- `--drop-existing`: 是否删除现有数据和Schema，默认为False

#### 使用示例

```bash
# 初始化所有Schema类型(完整初始化)
python -m graphrag.index.dgraph.init_schema

# 初始化单个Schema类型
python -m graphrag.index.dgraph.init_schema --schema-type entity

# 删除现有数据并重新初始化
python -m graphrag.index.dgraph.init_schema --drop-existing

# 指定DGraph服务器地址
python -m graphrag.index.dgraph.init_schema --dgraph-host localhost:8080
```

### 代码中使用

```python
from graphrag.index.dgraph.client import create_client
from graphrag.index.dgraph.init_schema import init_schema

# 创建DGraph客户端
client = create_client("localhost:9080")

# 初始化Schema
init_schema(client, schema_type="complete", drop_existing=True)
```

## 2. 数据导入 (import_data.py)

`import_data.py` 用于将数据导入到DGraph数据库中，支持多种数据类型和导入策略。

### 支持的数据类型

- `text_unit`: 文本单元数据
- `document`: 文档数据
- `entity`: 实体数据
- `relationship`: 关系数据
- `community`: 社区数据
- `community_report`: 社区报告数据
- `all`: 导入所有类型数据

### 命令行使用

```bash
python -m graphrag.index.dgraph.import_data [options]
```

#### 命令行参数

- `--data-dir`: 数据目录路径，默认为`/Users/renzhiping/workspace2/graphrag_root/output`
- `--dgraph-host`: DGraph服务器地址，默认为`localhost:9080`
- `--drop-existing`: 是否删除现有数据，默认为False
- `--init-schema`: 是否初始化Schema，默认为False
- `--batch-size`: 批量导入的批次大小，默认为1000
- `--types`: 要导入的数据类型，可选值为上述支持的类型，默认为all
- `--conflict`: 冲突处理策略，可选值：upsert(更新), insert(插入), skip(跳过)，默认为upsert

#### 使用示例

```bash
# 初始化Schema并导入所有数据
python -m graphrag.index.dgraph.import_data --init-schema --drop-existing

# 只导入实体和关系数据
python -m graphrag.index.dgraph.import_data --types entity relationship

# 从指定目录导入数据，设置冲突处理策略
python -m graphrag.index.dgraph.import_data --data-dir /path/to/data --conflict upsert
```

### 代码中使用

```python
from graphrag.index.dgraph.import_data import DGraphImporter

# 创建导入器
importer = DGraphImporter(
    data_dir="/path/to/data",
    dgraph_host="localhost:9080",
    batch_size=1000,
    conflict_strategy="upsert"
)

# 导入所有类型数据
importer.connect()
results = importer.import_data()
importer.close()

# 导入指定类型数据
results = importer.import_data(data_types=["entity", "relationship"])
```

## 数据目录结构

数据导入器会在指定的数据目录中查找以下文件模式：

- 文本单元: `*text_units*.parquet`
- 文档: `*documents*.parquet`
- 实体: `*entities*.parquet`
- 关系: `*relationships*.parquet`
- 社区: `*communities*.parquet`
- 社区报告: `*community_reports*.parquet`

## 常见问题排除

1. **连接问题**
   - 确保DGraph服务器正在运行，并检查服务器地址和端口是否正确
   - 检查防火墙或网络限制

2. **Schema初始化失败**
   - 检查DGraph服务器版本是否兼容
   - 确认有足够的权限执行schema更改

3. **数据导入错误**
   - 检查数据格式是否正确，特别是必需字段是否存在
   - 对于数组或JSON字段，确保数据结构有效
   - 检查日志以获取详细错误信息

4. **性能问题**
   - 对于大数据集，增加批处理大小以提高性能
   - 确保DGraph服务器有足够的资源（内存、CPU）

## 数据验证与转换

导入过程包含三层验证：

1. **文件层验证**：验证文件格式和必需列
2. **业务层验证**：验证业务规则和数据完整性
3. **存储层验证**：验证数据库约束和唯一性

数据转换逻辑会自动处理：
- JSON字段的序列化/反序列化
- 日期格式的标准化
- 数组和列表的处理
- 数值类型的转换 
# DGraph数据导入模块

该模块提供了一套用于将数据导入到DGraph数据库的工具。使用pydgraph客户端直接与DGraph交互，不需要GraphQL API。已经优化并简化了导入流程，提供了声明式配置和流水线式API。

## 功能特点

- 配置驱动的数据导入，减少重复代码
- 流水线式API，支持链式调用
- 自动类型转换和字段处理
- 智能关系处理和后处理钩子
- 支持多种数据源和批量导入

## 使用方法

### 命令行使用

```bash
# 初始化Schema并导入所有数据
python -m graphrag.index.dgraph.importer.import_data --init-schema --drop-existing

# 只导入实体和关系数据
python -m graphrag.index.dgraph.importer.import_data --types entity relationship

# 从指定目录导入数据，设置冲突处理策略
python -m graphrag.index.dgraph.importer.import_data --data-dir /path/to/data --dgraph-host localhost:9080 --conflict upsert
```

### 命令行参数

- `--data-dir`: 数据目录路径，默认为`/Users/renzhiping/workspace2/graphrag_root/output`
- `--dgraph-host`: DGraph服务器地址，默认为`localhost:9080`
- `--drop-existing`: 是否删除现有数据，默认为False
- `--init-schema`: 是否初始化Schema，默认为False
- `--batch-size`: 批量导入的批次大小，默认为1000
- `--types`: 要导入的数据类型，可选值：text_unit, document, entity, relationship, community, all
- `--conflict`: 冲突处理策略，可选值：upsert, insert, skip，默认为upsert

### 流水线式API使用

```python
from graphrag.index.dgraph.client import create_client
from graphrag.index.dgraph.importer.generic_importer import GenericImporter

# 创建导入器
importer = GenericImporter(hosts="localhost:9080")

# 导入实体数据
importer.use_schema("entity") \
        .from_directory("/path/to/data") \
        .with_batch_size(1000) \
        .on_conflict("upsert") \
        .import_from_directory()

# 导入单个实体
from graphrag.data_model.entity import Entity
entity = Entity(id="123", title="示例实体")
importer.use_schema("entity") \
        .from_models([entity]) \
        .import_data()

# 导入所有类型的数据
importer.from_directory("/path/to/data") \
        .import_all_types()
```

### 配置驱动的数据导入

数据导入配置位于`config.py`文件中，可以根据需要自定义导入规则：

```python
# 实体配置示例
"entity": {
    "type": "Entity",
    "file_pattern": "*entities*.parquet",
    "required_fields": ["id", "title"],
    "list_fields": ["text_unit_ids"],
    "numeric_fields": {
        "frequency": int,
        "degree": int,
        "x": float,
        "y": float
    },
    "relations": {
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"},
        "related_entities": {"field": None, "via_relation": "Relationship"}
    }
}
```

## 数据目录结构

导入器会在指定的数据目录中查找以下文件模式：

- 文本单元: `*text_units*.parquet`
- 文档: `*documents*.parquet`
- 实体: `*entities*.parquet`
- 关系: `*relationships*.parquet`
- 社区: `*communities*.parquet`

## 高级特性

### 冲突处理策略

- `upsert`: 如果记录已存在，删除后重新插入
- `insert`: 直接插入所有记录，不检查是否存在
- `skip`: 如果记录已存在，跳过该记录

### 自定义导入流程

您可以创建自己的导入流程，包括数据转换、字段处理和后处理操作：

```python
# 自定义数据转换
from graphrag.index.dgraph.importer.converter import DgraphDataConverter

# 自定义后处理函数
def my_post_process(client, data_type):
    # 执行自定义操作
    pass

# 使用自定义后处理
importer.with_post_process(my_post_process).import_data()
```

## 注意事项

1. 请确保DGraph服务器已启动并可访问
2. 导入大量数据时，请调整batch_size以优化性能
3. 数据导入顺序已经优化，以确保正确处理数据依赖关系
4. 如果出现内存不足问题，请减小batch_size值

## 故障排除

1. 如果连接DGraph失败，请检查DGraph服务器地址和端口
2. 如果导入数据失败，请检查数据文件格式和路径
3. 如果设置关系或层次结构失败，请确保先导入了相关的实体和社区数据 
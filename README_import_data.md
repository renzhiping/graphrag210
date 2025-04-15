# DGraph数据导入工具

此工具用于将处理后的数据导入到DGraph数据库中。

## 重构说明

代码已重构为模块化结构：

- `graphrag/index/dgraph/import_data.py`：主要导入类和逻辑
- `graphrag/index/dgraph/importer/utils.py`：数据处理工具方法
- `import_data_to_dgraph.py`：命令行入口点（向后兼容）

## 使用方法

你可以通过以下两种方式使用此工具：

### 1. 命令行工具

```bash
python import_data_to_dgraph.py [参数]
```

或者直接使用模块：

```bash
python -m graphrag.index.dgraph.import_data [参数]
```

### 2. 作为Python模块

```python
from graphrag.index.dgraph.import_data import DGraphImporter

importer = DGraphImporter(
    data_dir='/path/to/data',
    dgraph_host='localhost:9080',
    batch_size=1000,
    conflict_strategy='upsert'
)

try:
    results = importer.import_data(['text_unit', 'document'])
    print(results)
finally:
    importer.close()
```

## 参数说明

- `--data-dir`: 数据目录路径，默认为 '/Users/renzhiping/workspace2/graphrag_root/output'
- `--dgraph-host`: DGraph服务器地址，默认为 'localhost:9080'
- `--batch-size`: 批量导入的批次大小，默认为 1000
- `--types`: 要导入的数据类型，支持 text_unit, document, entity, relationship, community 或 all，默认为 ['all']
- `--conflict`: 冲突处理策略，可选值为 upsert, insert, skip，默认为 'upsert'

## 数据类型

数据导入按以下顺序进行：

1. document - 文档数据
2. text_unit - 文本单元数据
3. entity - 实体数据
4. relationship - 关系数据
5. community - 社区数据
6. community_report - 社区报告数据

## 错误处理

导入过程中的错误会被记录到日志中。程序会尽可能地继续处理其他数据类型，除非遇到严重错误。

## 冲突处理策略

- `upsert`: 如果记录已存在，则先删除再插入（更新）
- `insert`: 总是插入新记录，不检查是否存在
- `skip`: 如果记录已存在，则跳过

## 注意事项

1. 导入顺序已经优化，以确保正确处理数据依赖关系
2. 导入大量数据时，请适当调整batch_size以优化性能
3. 确保DGraph服务器有足够的存储空间和内存
4. 导入过程中如遇错误，可以查看日志了解详情

## 故障排除

1. 如果连接DGraph失败，请检查DGraph服务器是否正常运行
2. 如果导入某类数据失败，可以尝试单独导入该类型数据
3. 如果数据转换过程中出现错误，可能是数据格式不符合预期 
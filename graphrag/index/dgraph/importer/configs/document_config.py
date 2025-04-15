# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""文档配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取文档字段定义
document_fields = get_data_type_fields("document")

# 文档配置
DOCUMENT_CONFIG = {
    "type": "Document",
    "file_pattern": "*documents*.parquet",
    "required_fields": document_fields["required_fields"],
    "optional_fields": document_fields["optional_fields"],
    "list_fields": [
        "text_unit_ids"  # 文本单元ID列表
    ],
    "date_fields": ["creation_date"],
    "json_fields": ["metadata"],
    "text_fields": ["title", "text"],
    "relations": {
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"}
    }
}
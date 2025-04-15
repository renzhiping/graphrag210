# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""实体配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取实体字段定义
entity_fields = get_data_type_fields("entity")

# 实体配置
ENTITY_CONFIG = {
    "type": "Entity",
    "file_pattern": "*entities*.parquet",
    "required_fields": entity_fields["required_fields"],
    "optional_fields": entity_fields["optional_fields"],
    "list_fields": [
        "text_unit_ids"  # 文本单元ID列表
    ],
    "numeric_fields": {
        "frequency": int,
        "degree": int,
        "x": float,
        "y": float
    },
    "text_fields": ["title", "description", "type"],
    "relations": {
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"},
        "related_entities": {"field": None, "via_field": "source", "target_type": "Relationship"},
        "communities": {"field": None, "via_field": "entity_ids", "target_type": "Community"}
    }
}
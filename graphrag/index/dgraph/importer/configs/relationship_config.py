# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""关系配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取关系字段定义
relationship_fields = get_data_type_fields("relationship")

# 关系配置
RELATIONSHIP_CONFIG = {
    "type": "Relationship",
    "file_pattern": "*relationships*.parquet",
    "required_fields": relationship_fields["required_fields"],
    "optional_fields": relationship_fields["optional_fields"],
    "list_fields": [
        "text_unit_ids"  # 文本单元ID列表
    ],
    "numeric_fields": {
        "weight": float,
        "combined_degree": int
    },
    "text_fields": ["description"],
    "relations": {
        "source_entity": {"field": "source", "target_type": "Entity"},
        "target_entity": {"field": "target", "target_type": "Entity"},
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"},
        "communities": {"field": None, "via_field": "relationship_ids", "target_type": "Community"}
    },
    "post_import": "setup_entity_relations"
}
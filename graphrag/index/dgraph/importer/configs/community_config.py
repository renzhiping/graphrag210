# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""社区配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取社区字段定义
community_fields = get_data_type_fields("community")

# 社区配置
COMMUNITY_CONFIG = {
    "type": "Community",
    "file_pattern": "*communities*.parquet",
    "required_fields": community_fields["required_fields"],
    "optional_fields": community_fields["optional_fields"],
    "list_fields": [
        "children",     # 子社区ID列表
        "entity_ids",   # 实体ID列表
        "relationship_ids",  # 关系ID列表
        "text_unit_ids"  # 文本单元ID列表
    ],
    "numeric_fields": {
        "community": int,
        "level": int,
        "size": int,
        "parent": int
    },
    "date_fields": ["period"],
    "relations": {
        "entities": {"field": "entity_ids", "target_type": "Entity"},
        "relationships": {"field": "relationship_ids", "target_type": "Relationship"},
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"},
        "child_communities": {"field": "children", "target_type": "Community"}
    },
    "post_import": "setup_hierarchy"
} 
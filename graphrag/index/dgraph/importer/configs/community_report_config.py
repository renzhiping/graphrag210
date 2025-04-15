# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""社区报告配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取社区报告字段定义
community_report_fields = get_data_type_fields("community_report")

# 社区报告配置
COMMUNITY_REPORT_CONFIG = {
    "type": "CommunityReport",
    "file_pattern": "*community_reports*.parquet",
    "required_fields": community_report_fields["required_fields"],
    "optional_fields": community_report_fields["optional_fields"],
    "list_fields": [
        # 社区报告可能包含的列表字段
        "entity_ids",      # 相关实体ID列表
        "text_unit_ids"    # 相关文本单元ID列表
    ],
    "text_fields": [
        "title",
        "summary",
        "explanation",
        "report_content"
    ],
    "json_fields": ["full_content_json", "findings"],
    "numeric_fields": {
        "rating": int,
        "level": int
    },
    "date_fields": ["period", "create_time"],
    "relations": {
        "community": {"field": "community", "target_type": "Community"},
        "entities": {"field": "entity_ids", "target_type": "Entity"},
        "text_units": {"field": "text_unit_ids", "target_type": "TextUnit"}
    }
}
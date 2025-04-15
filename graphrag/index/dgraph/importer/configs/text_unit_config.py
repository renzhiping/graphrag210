# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""文本单元配置定义."""

from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields

# 获取文本单元字段定义
text_unit_fields = get_data_type_fields("text_unit")

# 文本单元配置
TEXT_UNIT_CONFIG = {
    "type": "TextUnit",
    "file_pattern": "*text_units*.parquet",
    "required_fields": text_unit_fields["required_fields"],
    "optional_fields": text_unit_fields["optional_fields"],
    "list_fields": [
        "document_ids",
        "entity_ids",
        "relationship_ids",
        "covariate_ids"
    ],
    "numeric_fields": {"n_tokens": int},
    "relations": {
        "documents": {"field": "document_ids", "target_type": "Document"},
        "entities": {"field": "entity_ids", "target_type": "Entity"},
        "relationships": {"field": "relationship_ids", "target_type": "Relationship"},
        "communities": {"field": None, "via_field": "text_unit_ids", "target_type": "Community"},
        "covariates": {"field": "covariate_ids", "target_type": "Covariate"}
    }
}
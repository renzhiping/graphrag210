#!/usr/bin/env python3
"""配置模块,提供DGraph导入所需的配置信息."""
from graphrag.index.dgraph.importer.configs import CONFIGS, IMPORT_ORDER

# 为了兼容性,保留原始配置
IMPORT_CONFIGS = CONFIGS

# 导入顺序（为了兼容性）
LEGACY_IMPORT_ORDER = IMPORT_ORDER

# 模型映射配置
MODEL_MAPPING = {
    "document": "Document",
    "text_unit": "TextUnit",
    "entity": "Entity",
    "relationship": "Relationship",
    "community": "Community",
    "community_report": "CommunityReport"
}
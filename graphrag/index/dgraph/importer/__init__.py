# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph导入器包，提供将数据导入DGraph的工具."""

from .import_manager import ImportManager
from .configs import CONFIGS, IMPORT_ORDER

# 为了向后兼容，保留原始配置导出
from .config import IMPORT_CONFIGS, IMPORT_ORDER as LEGACY_IMPORT_ORDER, MODEL_MAPPING

__all__ = [
    "ImportManager",
    "CONFIGS", 
    "IMPORT_ORDER",
    "IMPORT_CONFIGS",  # 旧版配置，为了兼容性保留
    "LEGACY_IMPORT_ORDER",  # 旧版导入顺序，为了兼容性保留
    "MODEL_MAPPING"  # 旧版模型映射，为了兼容性保留
]

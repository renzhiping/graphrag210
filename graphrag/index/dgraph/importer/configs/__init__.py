"""配置模块初始化文件,导出所有数据类型的配置."""

from .community_config import COMMUNITY_CONFIG
from .community_report_config import COMMUNITY_REPORT_CONFIG
from .document_config import DOCUMENT_CONFIG
from .entity_config import ENTITY_CONFIG
from .relationship_config import RELATIONSHIP_CONFIG
from .text_unit_config import TEXT_UNIT_CONFIG
from .validation_rules import get_data_type_fields


# 确保所有配置使用一致的必填字段和可选字段
def _update_config_fields(config, data_type):
    """
    更新配置字典中的字段定义,确保与中心化定义一致.
    
    Args:
        config: 配置字典
        data_type: 数据类型
    
    Return:
        更新后的配置字典
    """
    fields = get_data_type_fields(data_type)
    config["required_fields"] = fields["required_fields"]
    config["optional_fields"] = fields["optional_fields"]
    return config

# 将所有配置与中心化字段定义同步
_CONFIGS = {
    "text_unit": TEXT_UNIT_CONFIG,
    "document": DOCUMENT_CONFIG,
    "entity": ENTITY_CONFIG,
    "relationship": RELATIONSHIP_CONFIG,
    "community": COMMUNITY_CONFIG,
    "community_report": COMMUNITY_REPORT_CONFIG,
}

# 导出同步后的所有配置
CONFIGS = {
    data_type: _update_config_fields(config.copy(), data_type)
    for data_type, config in _CONFIGS.items()
}

# 导入顺序定义，确保依赖关系正确处理
IMPORT_ORDER = ["text_unit", "document", "entity", "relationship", "community", "community_report"] 
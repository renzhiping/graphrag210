"""转换器模块初始化文件，导出所有数据类型的转换器."""

from .text_unit_converter import TextUnitConverter
from .document_converter import DocumentConverter
from .entity_converter import EntityConverter
from .relationship_converter import RelationshipConverter
from .community_converter import CommunityConverter
from .community_report_converter import CommunityReportConverter

# 导出所有转换器类
CONVERTERS = {
    "text_unit": TextUnitConverter,
    "document": DocumentConverter,
    "entity": EntityConverter,
    "relationship": RelationshipConverter,
    "community": CommunityConverter,
    "community_report": CommunityReportConverter,
}

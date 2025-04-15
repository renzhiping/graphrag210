# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph转换器基类定义."""

from contextlib import suppress
from typing import Any

import numpy as np
import pandas as pd

# 导入公共工具函数,复用类型转换逻辑
from graphrag.index.dgraph.importer.utils import convert_value
from graphrag.index.dgraph.importer.validation import BusinessValidationError, BusinessValidator


class BaseConverter:
    """DGraph数据转换器基类，提供通用的转换方法."""

    def __init__(self, config: dict[str, Any]):
        """
        初始化转换器.

        Args:
            config: 数据类型配置字典
        """
        self.config = config
        self.type_name = config["type"]
        self.validator = BusinessValidator()

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str, Any]]:
        """
        将DataFrame转换为DGraph可导入的格式.

        Args:
            data_frame: 输入数据帧

        Return:
            转换后的数据列表
            
        Raise:
            BusinessValidationError: 当业务验证失败时
        """
        # 验证数据框必需字段
        self.validate(data_frame)
        
        # 业务转换
        result = []
        data_type = self.type_name.lower()
        
        for _, row in data_frame.iterrows():
            converted_row = self.process_fields(row)
            
            # 验证必需字段
            BusinessValidator.validate_required_fields(converted_row, data_type)
            
            # 进行字段值验证
            for field in converted_row:
                # 验证字段长度限制
                BusinessValidator.validate_field_value(converted_row, field, "min_length", data_type)
                BusinessValidator.validate_field_value(converted_row, field, "max_length", data_type)
                
                # 验证类型值有效性
                if field == "type":
                    BusinessValidator.validate_field_value(converted_row, field, "valid_values", data_type)
            
            result.append(converted_row)
            
        # 后处理
        return self.post_process(result)

    def validate(self, data_frame: pd.DataFrame) -> bool:
        """
        验证数据帧是否符合配置要求.

        Args:
            data_frame: 输入数据帧
            
        Return:
            bool: 验证是否通过
        """
        import logging
        logger = logging.getLogger(__name__)
        
        # 使用数据类型名称
        data_type = self.type_name.lower()
        
        # 记录验证开始
        logger.debug(f"开始验证{data_type}数据帧，列: {data_frame.columns.tolist()}")
        
        # 使用统一的字段定义
        from graphrag.index.dgraph.importer.configs.validation_rules import get_data_type_fields
        fields = get_data_type_fields(data_type)
        required_fields = fields["required_fields"]
        
        logger.debug(f"验证字段 - 必需: {required_fields}")
        
        # 检查DataFrame必需字段
        if required_fields:
            missing_fields = [field for field in required_fields if field not in data_frame.columns]
            if missing_fields:
                columns_str = ", ".join(data_frame.columns.tolist())
                logger.error(f"{data_type}数据帧缺少必需字段: {missing_fields}, 现有列: {columns_str}")
                return False
        
        # 验证通过
        logger.debug(f"{data_type}数据帧验证通过")
        return True

    def convert_to_list(self, value: Any) -> list:
        """将值转换为列表类型."""
        if isinstance(value, list):
            return value
            
        if isinstance(value, np.ndarray):
            return value.tolist()
            
        if hasattr(value, "__iter__") and not isinstance(value, (str|dict|bytes)):
            return list(value)
            
        return [value]

    def process_fields(self, row: pd.Series) -> dict[str, Any]:
        """
        处理数据行的各个字段,根据配置进行业务转换.

        Args:
            row: 数据行

        Return:
            处理后的字段字典
        """
        # 仅进行业务转换,不做技术转换(留给utils处理)
        result = {"type": self.type_name}
        
        # 检查字段是否存在且不为空的辅助函数
        def is_valid_field(field_name):
            """检查字段是否存在且不为空."""
            return (field_name in row and
                    row[field_name] is not None and
                    not (isinstance(row[field_name], float) and pd.isna(row[field_name])))
        
        # 处理基本字段(必需字段和可选字段)
        all_fields = self.config.get("required_fields", []) + self.config.get("optional_fields", [])
        for field in all_fields:
            if is_valid_field(field):
                result[field] = row[field]
        
        # 处理数值字段
        for field, field_type in self.config.get("numeric_fields", {}).items():
            if is_valid_field(field):
                with suppress(ValueError, TypeError):
                    result[field] = field_type(row[field])
                    
        # 其他字段留给子类处理
                    
        return result

    def post_process(self, converted_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        转换后的后处理操作.

        Args:
            converted_data: 转换后的数据列表

        Return:
            后处理的数据列表
        """
        # 对转换后的数据进行业务验证
        self.validate_converted_data(converted_data)
        return converted_data
        
    def validate_converted_data(self, converted_data: list[dict[str, Any]]) -> None:
        """
        验证转换后的数据是否符合业务规则.
        
        Args:
            converted_data: 转换后的数据列表
            
        Raise:
            BusinessValidationError: 当验证失败时
        """
        # 基类提供基本验证,子类可以扩展
        if not converted_data:
            return
            
        # 获取数据类型
        data_type = self.type_name.lower()
        
        # 对所有记录进行采样验证
        for i, record in enumerate(converted_data):
            if i >= 5:  # 只验证前5条记录
                break
                
            # 检查必需字段
            BusinessValidator.validate_required_fields(record, data_type)
            
            # 对每个字段进行值验证
            for field in record:
                # 验证字段长度限制
                BusinessValidator.validate_field_value(record, field, "min_length", data_type)
                BusinessValidator.validate_field_value(record, field, "max_length", data_type)
                
                # 验证类型值有效性
                if field == "type":
                    BusinessValidator.validate_field_value(record, field, "valid_values", data_type)
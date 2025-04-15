# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""文档转换器定义."""

import json
import logging
from typing import Any
from datetime import datetime

import pandas as pd
import numpy as np

from graphrag.index.dgraph.importer.base_converter import BaseConverter
from graphrag.index.dgraph.importer.configs.document_config import DOCUMENT_CONFIG


class DocumentConverter(BaseConverter):
    """文档数据转换器."""

    def __init__(self):
        """初始化文档转换器."""
        super().__init__(DOCUMENT_CONFIG)
        self.logger = logging.getLogger(__name__)

    def validate(self, data_frame: pd.DataFrame) -> bool:
        """
        验证数据帧是否符合配置要求.

        Args:
            data_frame: 输入数据帧
            
        Return:
            bool: 验证是否通过
        """
        # 使用基类的统一验证逻辑
        return super().validate(data_frame)

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str, Any]]:
        """
        将文档数据帧转换为DGraph可导入的格式.

        Args:
            data_frame: 文档数据帧

        Return:
            转换后的文档数据列表
        """
        if not self.validate(data_frame):
            error_message = f"文档数据帧缺少必需字段: {self.config['required_fields']}"
            raise ValueError(error_message)

        result = []
        for _, row in data_frame.iterrows():
            # 基础字段处理由基类完成
            converted_row = self.process_fields(row)
            
            # 处理JSON字段 - 文档特有的业务逻辑
            for field in self.config.get("json_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        if isinstance(field_value, str):
                            try:
                                # 业务逻辑:解析JSON字符串
                                converted_row[field] = json.loads(field_value)
                            except json.JSONDecodeError:
                                # 保留原始字符串
                                converted_row[field] = field_value
                        else:
                            converted_row[field] = field_value
            
            # 处理日期字段 - 确保格式为ISO 8601标准
            for field in self.config.get("date_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        if isinstance(field_value, str):
                            try:
                                # 尝试解析日期字符串，然后重新格式化为标准ISO格式（带T分隔符）
                                date_obj = datetime.fromisoformat(field_value.replace(' ', 'T', 1))
                                converted_row[field] = date_obj.isoformat()
                            except ValueError:
                                # 如果解析失败，尝试其他常见格式
                                try:
                                    # 处理常见的 "YYYY-MM-DD HH:MM:SS" 格式
                                    if ' ' in field_value and 'T' not in field_value:
                                        parts = field_value.split(' ', 1)
                                        if len(parts) == 2:
                                            converted_row[field] = f"{parts[0]}T{parts[1]}"
                                        else:
                                            converted_row[field] = field_value
                                    else:
                                        converted_row[field] = field_value
                                except Exception:
                                    self.logger.warning(f"无法解析日期字段 {field}: {field_value}")
                                    converted_row[field] = field_value
                        elif isinstance(field_value, pd.Timestamp):
                            # Pandas Timestamp可以直接转为ISO格式
                            converted_row[field] = field_value.isoformat()
                        else:
                            converted_row[field] = field_value
            
            result.append(converted_row)

        return self.post_process(result)
        
    def _is_valid_value(self, value: Any) -> bool:
        """
        判断值是否有效（非None，非NaN）.
        
        Args:
            value: 要检查的值
            
        Return:
            bool: 值是否有效
        """
        # 处理标量值
        if value is None:
            return False
            
        # 处理pandas的NA/NaN值
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return False
            
        # 处理数组/迭代器类型
        if hasattr(value, '__iter__') and not isinstance(value, (str, dict)):
            # 检查是否为空数组
            if len(value) == 0:
                return False
            # 对于numpy数组，检查是否全部为NA
            if hasattr(value, 'any') and pd.isna(value).all():
                return False
                
        return True
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""社区转换器定义."""

from datetime import datetime
from typing import Any

import pandas as pd
import numpy as np

from ..base_converter import BaseConverter
from ..configs.community_config import COMMUNITY_CONFIG


class CommunityConverter(BaseConverter):
    """社区数据转换器."""

    def __init__(self):
        """初始化社区转换器."""
        super().__init__(COMMUNITY_CONFIG)

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str,Any]]:
        """
        将社区数据帧转换为DGraph可导入的格式.

        Args:
            data_frame: 社区数据帧

        Return:
            转换后的社区数据列表
        """
        if not self.validate(data_frame):
            error_message = f"社区数据帧缺少必需字段: {self.config['required_fields']}"
            raise ValueError(error_message)

        result = []
        for _, row in data_frame.iterrows():
            converted_row = self.process_fields(row)
            
            # 处理文本字段
            for field in self.config.get("text_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        converted_row[field] = str(field_value)
            
            # 处理日期字段
            for field in self.config.get("date_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        if isinstance(field_value, str):
                            try:
                                converted_row[field] = datetime.fromisoformat(field_value).isoformat()
                            except ValueError:
                                converted_row[field] = field_value
                        elif isinstance(field_value, pd.Timestamp):
                            converted_row[field] = field_value.isoformat()
                        else:
                            converted_row[field] = field_value
            
            result.append(converted_row)

        return self.post_process(result)

    def setup_hierarchy(self, converted_data: list[dict[str,Any]]) -> list[dict[str,Any]]:
        """
        设置社区层次结构.

        Args:
            converted_data: 转换后的社区数据列表

        Return:
            处理后的社区数据列表
        """
        # 在这里实现社区层次结构的特殊处理逻辑
        return converted_data

    def post_process(self, converted_data: list[dict[str,Any]]) -> list[dict[str,Any]]:
        """
        转换后的后处理操作.

        Args:
            converted_data: 转换后的数据列表

        Return:
            后处理的数据列表
        """
        if self.config.get("post_import") == "setup_hierarchy":
            return self.setup_hierarchy(converted_data)
        return converted_data
        
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
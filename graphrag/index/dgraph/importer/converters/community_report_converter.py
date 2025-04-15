# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""社区报告转换器定义."""

import json
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

from ..base_converter import BaseConverter
from ..configs.community_report_config import COMMUNITY_REPORT_CONFIG
from ..utils import generate_fingerprint


class CommunityReportConverter(BaseConverter):
    """社区报告数据转换器."""

    def __init__(self):
        """初始化社区报告转换器."""
        super().__init__(COMMUNITY_REPORT_CONFIG)

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str,Any]]:
        """
        将社区报告数据帧转换为DGraph可导入的格式.

        Args:
            data_frame: 社区报告数据帧

        Return:
            转换后的社区报告数据列表
        """
        # 验证数据帧基本结构
        if not self.validate(data_frame):
            error_message = f"社区报告数据帧缺少必需字段: {self.config['required_fields']}"
            raise ValueError(error_message)

        result = []
        for _, row in data_frame.iterrows():
            # 基本字段处理
            converted_row = self.process_fields(row)
            
            # 处理文本字段
            for field in self.config.get("text_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        # 如果是可迭代对象但不是字符串,转换为文本
                        if hasattr(field_value, "__iter__") and not isinstance(field_value, str):
                            converted_row[field] = "; ".join([str(item) for item in field_value if item is not None])
                        else:
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
            
            # 处理JSON字段
            for field in self.config.get("json_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        if isinstance(field_value, str):
                            try:
                                # 如果已经是字符串,尝试解析确保是有效的JSON
                                json.loads(field_value)
                                converted_row[field] = field_value
                            except json.JSONDecodeError:
                                # 如果不是有效的JSON，则转换为JSON字符串
                                converted_row[field] = json.dumps(field_value)
                        else:
                            # 如果是其他类型（如列表、字典等），则转换为JSON字符串
                            converted_row[field] = json.dumps(field_value)
            
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
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""实体转换器定义."""

from typing import Any

import pandas as pd
import numpy as np

from graphrag.index.dgraph.importer.base_converter import BaseConverter
from graphrag.index.dgraph.importer.configs.entity_config import ENTITY_CONFIG


class EntityConverter(BaseConverter):
    """实体数据转换器."""

    def __init__(self):
        """初始化实体转换器."""
        super().__init__(ENTITY_CONFIG)

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str, Any]]:
        """
        将实体数据帧转换为DGraph可导入的格式.

        Args:
            data_frame: 实体数据帧

        Return:
            转换后的实体数据列表
        """
        if not self.validate(data_frame):
            error_message = f"实体数据帧缺少必需字段: {self.config['required_fields']}"
            raise ValueError(error_message)

        result = []
        for _, row in data_frame.iterrows():
            # 基础字段处理由基类完成
            converted_row = self.process_fields(row)
            
            # 处理实体特有的业务逻辑
            # 处理文本字段,确保它们是字符串
            for field in self.config.get("text_fields", []):
                if field in row:
                    field_value = row[field]
                    if self._is_valid_value(field_value):
                        # 实体特有的业务逻辑,确保文本字段是字符串
                        converted_row[field] = str(field_value)
            
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
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""文本单元转换器定义."""

from typing import Any

import pandas as pd

from graphrag.index.dgraph.importer.base_converter import BaseConverter
from graphrag.index.dgraph.importer.configs.text_unit_config import TEXT_UNIT_CONFIG


class TextUnitConverter(BaseConverter):
    """文本单元数据转换器."""

    def __init__(self):
        """初始化文本单元转换器."""
        super().__init__(TEXT_UNIT_CONFIG)

    def validate(self, data_frame: pd.DataFrame) -> bool:
        """
        验证数据帧是否符合配置要求.

        Args:
            data_frame: 输入数据帧
            
        Return:
            bool: 验证是否通过
        """
        # 确保必需字段存在于数据帧中
        for field in self.config["required_fields"]:
            if field not in data_frame.columns:
                return False
                
        # 验证通过
        return True

    def convert(self, data_frame: pd.DataFrame) -> list[dict[str, Any]]:
        """
        将文本单元数据帧转换为DGraph可导入的格式.

        Args:
            data_frame: 文本单元数据帧

        Return:
            转换后的文本单元数据列表
        """
        if not self.validate(data_frame):
            error_message = f"文本单元数据帧缺少必需字段: {self.config['required_fields']}"
            raise ValueError(error_message)

        result = []
        for _, row in data_frame.iterrows():
            converted_row = self.process_fields(row)
            result.append(converted_row)

        return self.post_process(result)
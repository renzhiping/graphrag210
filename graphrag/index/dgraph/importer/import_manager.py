# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph导入管理器，统一管理所有数据类型的导入过程."""

import glob
import os
import logging
import traceback
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import numpy as np

from .configs import CONFIGS, IMPORT_ORDER
from .converters import CONVERTERS
from .validation import FileValidator, FileValidationError, Validator, ValidationError


class ImportManager:
    """DGraph导入管理器，负责协调各数据类型的转换和导入."""

    def __init__(self, data_dir: str):
        """
        初始化导入管理器.

        Args:
            data_dir: 数据文件所在目录路径
        """
        self.data_dir = data_dir
        self.converters = {data_type: converter_cls() for data_type, converter_cls in CONVERTERS.items()}
        self.configs = CONFIGS
        self.import_order = IMPORT_ORDER
        self.logger = logging.getLogger(__name__)
        self.file_validator = FileValidator()

    def get_supported_data_types(self) -> list[str]:
        """
        获取所有支持的数据类型.

        Return:
            支持的数据类型列表
        """
        return list(self.configs.keys())

    def get_import_order(self) -> list[str]:
        """
        获取推荐的导入顺序.

        Return:
            数据类型按推荐顺序的列表
        """
        return self.import_order.copy()

    def validate_data_type(self, data_type: str) -> bool:
        """
        验证数据类型是否有效.

        Args:
            data_type: 数据类型名称

        Return:
            数据类型是否有效
        """
        return data_type in self.configs

    def find_data_files(self, data_type: str) -> list[str]:
        """
        根据数据类型查找匹配的数据文件.

        Args:
            data_type: 数据类型名称

        Return:
            匹配的文件路径列表

        Raise:
            FileValidationError: 当数据类型未知时
        """
        if not self.validate_data_type(data_type):
            raise FileValidationError("TYPE", f"未知的数据类型: {data_type}")
        
        file_pattern = self.configs[data_type]["file_pattern"]
        return glob.glob(os.path.join(self.data_dir, file_pattern))

    def load_dataframe(self, file_path: str, data_type: str) -> pd.DataFrame:
        """
        加载数据文件为DataFrame.

        Args:
            file_path: 数据文件路径
            data_type: 数据类型

        Return:
            加载的DataFrame

        Raise:
            FileValidationError: 当文件格式不支持时
        """
        # 验证文件存在
        FileValidator.validate_file_exists(file_path)
        
        # 验证文件格式
        FileValidator.validate_file_format(file_path, data_type)
        
        # 根据文件格式加载数据
        if file_path.endswith(".parquet"):
            df = pd.read_parquet(file_path)
        elif file_path.endswith(".csv"):
            df = pd.read_csv(file_path)
        else:
            raise FileValidationError("FORMAT", f"不支持的文件格式: {file_path}")
            
        # 验证必需列
        FileValidator.validate_columns(df, data_type)
        
        return df

    def convert_data(self, data_type: str, data_frame: pd.DataFrame) -> list[dict[str, Any]]:
        """
        使用对应的转换器转换数据.

        Args:
            data_type: 数据类型名称
            data_frame: 输入数据帧

        Return:
            转换后的数据列表

        Raise:
            FileValidationError: 当转换器未找到时
        """
        if data_type not in self.converters:
            raise FileValidationError("CONVERTER", f"未找到数据类型的转换器: {data_type}")
        
        # 业务转换和验证在转换器中处理
        return self.converters[data_type].convert(data_frame)

    def import_all(self) -> dict[str, list[dict[str, Any]]]:
        """
        按照指定顺序导入所有数据类型.

        Return:
            各数据类型的转换结果
        """
        results = {}
        
        for data_type in self.import_order:
            try:
                data = self.import_data_type(data_type)
                if not Validator.is_empty(data):
                    results[data_type] = data
                    self.logger.info(f"已导入{data_type}类型数据: {len(data)}条记录")
            except Exception as e:
                self.logger.error(f"导入{data_type}类型数据时出错: {str(e)}")
        
        return results
        
    def import_data_type(self, data_type: str) -> list[dict[str, Any]]:
        """
        导入单个数据类型的数据.

        Args:
            data_type: 数据类型名称

        Return:
            转换后的数据列表

        Raise:
            FileValidationError: 当数据类型未知或文件不存在时
        """
        # 验证数据类型
        if not self.validate_data_type(data_type):
            raise FileValidationError("TYPE", f"未知的数据类型: {data_type}")
            
        # 查找匹配的文件
        files = self.find_data_files(data_type)
        if not files:
            self.logger.warning(f"未找到{data_type}类型的数据文件")
            return []
            
        self.logger.info(f"开始导入{data_type}类型数据，文件数量: {len(files)}")
            
        all_data = []
        for file_path in files:
            try:
                # 文件层验证和加载
                self.logger.info(f"读取文件: {file_path}")
                df = self.load_dataframe(file_path, data_type)
                self.logger.info(f"读取完成，行数: {len(df)}, 列数: {len(df.columns)}")
                
                # 预处理numpy数组字段，避免后续处理错误
                self._preprocess_arrays(df, data_type)
                
                # 增强日志：记录DataFrame的列名和数据类型
                self.logger.info(f"数据列: {df.columns.tolist()}")
                if not df.empty:
                    # 记录第一行数据类型和不为空的值样例
                    sample_row = df.iloc[0]
                    type_info = {col: type(val).__name__ for col, val in sample_row.items()}
                    self.logger.info(f"首行数据类型: {type_info}")
                    value_samples = {}
                    for col, val in sample_row.items():
                        try:
                            if val is not None and not pd.isna(val):
                                # 安全处理数组类型值
                                str_val = str(list(val)) if isinstance(val, np.ndarray) else str(val)
                                value_samples[col] = str_val[:50] + ('...' if len(str_val) > 50 else '')
                        except Exception as e:
                            self.logger.warning(f"字段[{col}]值记录失败: {str(e)}")
                            value_samples[col] = "<VALUE LOGGING ERROR>"
                    self.logger.debug(f"首行数据样例: {value_samples}")
                
                # 业务转换（转换器内部会进行业务验证）
                self.logger.info(f"开始转换{data_type}数据")
                
                try:
                    converted_data = self.convert_data(data_type, df)
                    self.logger.info(f"转换完成，得到{len(converted_data)}条记录")
                    
                    # 添加转换后的数据
                    all_data.extend(converted_data)
                    self.logger.info(f"累计导入{len(all_data)}条{data_type}记录")
                except ValueError as e:
                    self.logger.error(f"{data_type}数据转换失败: {str(e)}")
                    # 获取所需字段配置信息，帮助诊断
                    config = self.configs.get(data_type, {})
                    required_fields = config.get("required_fields", [])
                    missing_fields = [field for field in required_fields if field not in df.columns]
                    if missing_fields:
                        self.logger.error(f"确实缺少字段: {missing_fields}")
                    else:
                        self.logger.error(f"字段存在但可能有其他问题，所需字段: {required_fields}")
                        # 检查字段值是否为空
                        for field in required_fields:
                            empty_count = df[field].isna().sum()
                            if empty_count > 0:
                                self.logger.error(f"字段'{field}'有{empty_count}个空值")
                    raise
                
            except ValidationError as e:
                self.logger.error(f"验证失败: {str(e)}")
                # 继续处理其他文件
            except Exception as e:
                error_msg = f"处理{file_path}时出错: {str(e)}"
                self.logger.error(error_msg)
                self.logger.error(f"错误详情: {traceback.format_exc()}")
                # 继续处理其他文件
        
        self.logger.info(f"已导入{data_type}类型数据: {len(all_data)}条记录")
        return all_data
        
    def _preprocess_arrays(self, df: pd.DataFrame, data_type: str) -> None:
        """
        预处理数据帧中的numpy数组.
        
        Args:
            df: 数据帧
            data_type: 数据类型
        """
        array_columns = []
        for col in df.columns:
            if df[col].size > 0:
                # 尝试获取第一个非空值作为样本
                try:
                    sample = next((x for x in df[col] if x is not None), None)
                    if isinstance(sample, np.ndarray):
                        array_columns.append(col)
                        # 转换numpy数组为列表
                        self.logger.debug(f"转换numpy数组列: {col}, 示例值类型: {type(sample)}, 形状: {sample.shape if hasattr(sample, 'shape') else '未知'}")
                        df[col] = df[col].apply(
                            lambda x: x.tolist() if isinstance(x, np.ndarray) else x
                        )
                except (TypeError, ValueError) as e:
                    # 跳过无法处理的列
                    self.logger.warning(f"处理列 {col} 时出错: {str(e)}")
        
        if array_columns:
            self.logger.info(f"预处理{data_type}数据中的数组列: {', '.join(array_columns)}") 
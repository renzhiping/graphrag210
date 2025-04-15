# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph数据验证模块，提供分层验证框架."""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union
import json

import pandas as pd

# 导入验证规则配置
from graphrag.index.dgraph.importer.configs.validation_rules import get_validation_rules

logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """验证异常基类"""
    layer: str  # 所属验证层(file/business/storage)
    code: str   # 错误代码
    
    def __init__(self, message: str, layer: str, code: str):
        self.layer = layer
        self.code = code
        super().__init__(f"[{layer}.{code}] {message}")


class FileValidationError(ValidationError):
    """文件层验证异常"""
    def __init__(self, code: str, message: str):
        super().__init__(message, "file", code)


class BusinessValidationError(ValidationError):
    """业务层验证异常"""
    def __init__(self, code: str, message: str):
        super().__init__(message, "business", code)


class StorageValidationError(ValidationError):
    """存储层验证异常"""
    def __init__(self, code: str, message: str):
        super().__init__(message, "storage", code)


class Validator:
    """数据验证器基类"""
    
    @staticmethod
    def is_empty(data: Any) -> bool:
        """
        检查数据是否为空.
        
        Args:
            data: 要检查的数据
            
        Return:
            bool: 数据是否为空
        """
        if data is None:
            return True
        elif isinstance(data, pd.DataFrame):
            return data.empty
        elif isinstance(data, (list, tuple)):
            return len(data) == 0
        
        # 对于其他类型，尝试使用len或bool判断
        try:
            return len(data) == 0
        except (TypeError, AttributeError):
            try:
                return not bool(data)
            except (ValueError, TypeError):
                return False


class FileValidator(Validator):
    """文件层验证器，负责验证文件存在性和格式"""
    
    @staticmethod
    def validate_file_exists(file_path: str) -> None:
        """
        验证文件是否存在.
        
        Args:
            file_path: 文件路径
            
        Raises:
            FileValidationError: 当文件不存在时
        """
        import os
        if not os.path.exists(file_path):
            raise FileValidationError("NOT_FOUND", f"文件不存在: {file_path}")
    
    @staticmethod
    def validate_file_format(file_path: str, data_type: str) -> None:
        """
        验证文件格式是否支持.
        
        Args:
            file_path: 文件路径
            data_type: 数据类型
            
        Raises:
            FileValidationError: 当文件格式不支持时
        """
        file_ext = file_path.split('.')[-1].lower()
        
        # 获取支持的格式
        rules = get_validation_rules(data_type, "file_layer")
        supported_formats = rules.get("supported_formats", ["csv", "parquet"])
        
        if file_ext not in supported_formats:
            raise FileValidationError("FORMAT", f"不支持的文件格式: {file_ext}，支持的格式: {supported_formats}")
    
    @staticmethod
    def validate_columns(df: pd.DataFrame, data_type: str) -> None:
        """
        验证数据帧是否包含必需列.
        
        Args:
            df: 数据帧
            data_type: 数据类型
            
        Raises:
            FileValidationError: 当缺少必需列时
        """
        # 获取必需列
        rules = get_validation_rules(data_type, "file_layer")
        required_columns = rules.get("required_columns", [])
        
        if not required_columns:
            return
            
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise FileValidationError("MISSING_COLUMNS", f"缺少必需列: {missing_columns}")


class BusinessValidator(Validator):
    """业务层验证器，负责验证数据的业务规则"""
    
    @staticmethod
    def validate_required_fields(data: Union[pd.DataFrame, Dict[str, Any]], data_type: str) -> None:
        """
        验证数据是否包含必需字段.
        
        Args:
            data: 要验证的数据
            data_type: 数据类型
            
        Raises:
            BusinessValidationError: 当缺少必需字段时
        """
        # 获取必需字段
        rules = get_validation_rules(data_type, "business_layer")
        required_fields = rules.get("required_fields", [])
        
        if not required_fields:
            return
        
        logger.debug(f"验证{data_type}必需字段: {required_fields}")
            
        if isinstance(data, pd.DataFrame):
            # 验证DataFrame
            missing_fields = [field for field in required_fields if field not in data.columns]
            if missing_fields:
                columns_str = ", ".join(data.columns.tolist())
                logger.error(f"{data_type}数据帧缺少必需字段: {missing_fields}, 现有列: {columns_str}")
                raise BusinessValidationError("MISSING_FIELDS", f"缺少必需字段: {missing_fields}")
        elif isinstance(data, dict):
            # 验证字典
            missing_fields = [field for field in required_fields if field not in data]
            if missing_fields:
                keys_str = ", ".join(list(data.keys()))
                logger.error(f"{data_type}数据记录缺少必需字段: {missing_fields}, 现有字段: {keys_str}")
                raise BusinessValidationError("MISSING_FIELDS", f"缺少必需字段: {missing_fields}")
        else:
            type_name = type(data).__name__
            logger.error(f"不支持的数据类型: {type_name}, 期望DataFrame或dict")
            raise BusinessValidationError("TYPE", f"不支持的数据类型: {type_name}")
    
    @staticmethod
    def validate_field_value(data: Dict[str, Any], field: str, rule_name: str, data_type: str) -> None:
        """
        验证字段值是否符合规则.
        
        Args:
            data: 要验证的数据
            field: 字段名
            rule_name: 规则名称
            data_type: 数据类型
            
        Raises:
            BusinessValidationError: 当字段值不符合规则时
        """
        if field not in data:
            return
            
        value = data[field]
        rules = get_validation_rules(data_type, "business_layer")
        
        # 验证长度
        if rule_name == "min_length" and rules.get(f"min_{field}_length"):
            min_length = rules[f"min_{field}_length"]
            if len(str(value)) < min_length:
                raise BusinessValidationError(
                    "MIN_LENGTH", 
                    f"字段{field}长度小于最小长度{min_length}: {len(str(value))}"
                )
                
        elif rule_name == "max_length" and rules.get(f"max_{field}_length"):
            max_length = rules[f"max_{field}_length"]
            if len(str(value)) > max_length:
                raise BusinessValidationError(
                    "MAX_LENGTH", 
                    f"字段{field}长度大于最大长度{max_length}: {len(str(value))}"
                )
                
        # 验证类型枚举值
        elif rule_name == "valid_values" and rules.get("valid_types") and field == "type":
            valid_types = rules["valid_types"]
            if value not in valid_types:
                raise BusinessValidationError(
                    "INVALID_TYPE", 
                    f"字段{field}值不在有效范围: {value}，有效值: {valid_types}"
                )
        
        # 验证数值范围
        elif rule_name == "min_value" and rules.get(f"min_{field}"):
            min_value = rules[f"min_{field}"]
            if float(value) < min_value:
                raise BusinessValidationError(
                    "MIN_VALUE", 
                    f"字段{field}值小于最小值{min_value}: {value}"
                )
                
        elif rule_name == "max_value" and rules.get(f"max_{field}"):
            max_value = rules[f"max_{field}"]
            if float(value) > max_value:
                raise BusinessValidationError(
                    "MAX_VALUE", 
                    f"字段{field}值大于最大值{max_value}: {value}"
                )


class StorageValidator(Validator):
    """存储层验证器，负责验证数据库约束"""
    
    @staticmethod
    def validate_id_format(id_value: str, data_type: str) -> None:
        """
        验证ID格式是否符合要求.
        
        Args:
            id_value: ID值
            data_type: 数据类型
            
        Raises:
            StorageValidationError: 当ID格式不符合要求时
        """
        # 获取ID格式规则
        rules = get_validation_rules(data_type, "storage_layer")
        format_type = rules.get("id_format")
        
        if not format_type:
            return
            
        if format_type == "uuid":
            import uuid
            try:
                uuid.UUID(id_value)
            except ValueError:
                raise StorageValidationError("ID_FORMAT", f"ID格式不是有效的UUID: {id_value}")
        elif format_type == "int":
            try:
                int(id_value)
            except ValueError:
                raise StorageValidationError("ID_FORMAT", f"ID格式不是有效的整数: {id_value}")
        elif format_type == "hash":
            # 哈希值格式，一般为十六进制，允许长字符串
            import re
            if not re.match(r'^[0-9a-fA-F]{32,}$', id_value):
                raise StorageValidationError("ID_FORMAT", f"ID格式不是有效的哈希值: {id_value}")

    @staticmethod
    def validate_unique_constraint(txn, id_value: str, data_type: str) -> None:
        """
        验证唯一约束是否满足.
        
        Args:
            txn: 数据库事务
            id_value: 数据ID
            data_type: 数据类型
            
        Raises:
            StorageValidationError: 当违反唯一约束时
        """
        # 确保ID是字符串类型
        id_value = str(id_value)
        
        query = f"""
        {{
          exists(func: eq(id, "{id_value}")) {{
            uid
          }}
        }}
        """
        response = txn.query(query)
        
        # 兼容pydgraph 24.2.1版本
        if isinstance(response, bytes):
            result = json.loads(response.decode('utf-8'))
        else:
            result = json.loads(response.json)
        
        if result and result.get("exists"):
            raise StorageValidationError("UNIQUE", f"ID已存在: {id_value}, 类型: {data_type}")
    
    @staticmethod
    def validate_composite_unique(txn, data: Dict[str, Any], data_type: str) -> None:
        """
        验证复合唯一约束是否满足.
        
        Args:
            txn: 数据库事务
            data: 数据
            data_type: 数据类型
            
        Raises:
            StorageValidationError: 当违反复合唯一约束时
        """
        # 获取复合唯一约束
        rules = get_validation_rules(data_type, "storage_layer")
        composite_fields = rules.get("composite_unique")
        
        if not composite_fields:
            return
            
        # 检查数据是否包含所有复合字段
        if not all(field in data for field in composite_fields):
            return
            
        # 构建查询条件
        conditions = []
        for field in composite_fields:
            if field in data and data[field] is not None:
                conditions.append(f'eq({field}, "{data[field]}")')
                
        if not conditions:
            return
            
        query = f"""
        {{
          exists(func: type({data_type})) @filter({" AND ".join(conditions)}) {{
            uid
          }}
        }}
        """
        
        response = txn.query(query)
        
        # 兼容pydgraph 24.2.1版本
        if isinstance(response, bytes):
            result = json.loads(response.decode('utf-8'))
        else:
            result = json.loads(response.json)
        
        if result and result.get("exists"):
            field_values = ", ".join([f"{field}={data[field]}" for field in composite_fields if field in data])
            raise StorageValidationError("COMPOSITE_UNIQUE", f"记录已存在: {field_values}, 类型: {data_type}") 
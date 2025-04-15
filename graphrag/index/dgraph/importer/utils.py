#!/usr/bin/env python3
"""
工具方法模块，为DGraph数据导入提供辅助功能。
"""
import base64
import hashlib
import json
import logging
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import pandas as pd

from graphrag.index.dgraph.importer.configs import CONFIGS as IMPORT_CONFIGS

logger = logging.getLogger(__name__)

# 数据类型转换部分 - 增强并统一所有类型转换功能
def convert_value(value: Any) -> Any:
    """
    转换单个值为DGraph兼容格式.
    
    Args:
        value: 要转换的值
        
    Return:
        转换后的值
    """
    # 处理None值
    if value is None:
        return None
        
    # 处理日期和时间类型
    if isinstance(value,(datetime | date)):
        return value.isoformat()

    # 处理字节类型
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return base64.b64encode(value).decode("utf-8")
    
    # 处理numpy数组
    if isinstance(value, np.ndarray):
        return value.tolist()
        
    # 处理numpy标量
    if isinstance(value, np.generic):
        return value.item()
        
    # 处理集合类型
    if isinstance(value, (set | tuple)):
        return list(value)
    
    # 处理其他可迭代类型(但不是字符串、字典或字节)
    if hasattr(value, "__iter__") and not isinstance(value, (str | dict | bytes)):
        return [convert_value(item) for item in value]
        
    # 处理字典类型 - 递归转换
    if isinstance(value, dict):
        return {k: convert_value(v) for k, v in value.items() if v is not None}
        
    # 其他基本类型直接返回
    return value

# 生成数据指纹
def generate_fingerprint(data: Union[dict[str,Any], list, str, bytes]) -> str:
    """
    生成数据的指纹用于唯一标识.
    
    Args:
        data: 要生成指纹的数据
        
    Return:
        str: 十六进制字符串指纹
    """
    try:
        # 序列化为JSON字符串
        if isinstance(data, (dict | list)):
            data_str = json.dumps(data, sort_keys=True, default=str)
        elif isinstance(data, bytes):
            data_str = data.hex()
        else:
            data_str = str(data)
            
        # 计算SHA-256哈希
        return hashlib.sha256(data_str.encode("utf-8")).hexdigest()
    except Exception as e:
        logger.error(f"生成指纹时出错: {str(e)}")
        # 发生错误时使用对象的字符串表示
        backup_str = str(data)
        return hashlib.sha256(backup_str.encode("utf-8")).hexdigest()

# 统一验证框架
def validate_data(data: Union[pd.DataFrame, List[Dict[str, Any]]], data_type: str, strict: bool = False) -> Tuple[bool, str]:
    """
    验证数据是否包含必需字段.
    
    Args:
        data: 要验证的数据
        data_type: 数据类型
        strict: 是否严格验证所有字段
    
    Return:
        Tuple[bool, str]: (验证是否通过, 错误信息)
    """
    logger.info(f"开始验证数据类型: {data_type}")
    
    if data is None or (isinstance(data, pd.DataFrame) and data.empty) or (isinstance(data, list) and len(data) == 0):
        return False, "数据为空"
    
    # 对所有数据类型执行相同的验证逻辑
    required_fields = IMPORT_CONFIGS.get(data_type, {}).get('required_fields', ['id'])
    logger.info(f"数据类型 {data_type} 的必需字段: {required_fields}")
    
    if isinstance(data, pd.DataFrame):
        missing_fields = [field for field in required_fields if field not in data.columns]
        if missing_fields and (strict or len(missing_fields) == len(required_fields)):
            logger.error(f"{data_type}数据帧缺少必需字段: {missing_fields}")
            return False, f"{data_type}数据帧缺少必需字段: {missing_fields}"
    elif isinstance(data, list) and data:
        missing_fields = [field for field in required_fields if field not in data[0]]
        if missing_fields and (strict or len(missing_fields) == len(required_fields)):
            logger.error(f"{data_type}数据记录缺少必需字段: {missing_fields}")
            return False, f"{data_type}数据记录缺少必需字段: {missing_fields}"
    
    logger.info(f"数据验证通过: {data_type}")
    return True, ""

# 数据类型检查方法
def is_empty_data(data: Any) -> bool:
    """
    智能检查不同类型数据是否为空.
    
    Args:
        data: 要检查的数据
        
    Return:
        bool: 数据是否为空
    """
    if data is None:
        logger.info("数据为None")
        return True
    elif isinstance(data, pd.DataFrame):
        logger.info(f"检查DataFrame是否为空: {data.empty}, 行数: {len(data)}")
        return data.empty
    elif isinstance(data, np.ndarray):
        # 特别处理NumPy数组，避免布尔判断错误
        try:
            is_empty = data.size == 0
            logger.info(f"检查NumPy数组是否为空: {is_empty}, 大小: {data.size}")
            return is_empty
        except:
            # 对于复杂数组，如果无法直接判断size，我们检查shape
            try:
                is_empty = len(data) == 0 or all(d == 0 for d in data.shape)
                logger.info(f"检查NumPy数组(使用shape)是否为空: {is_empty}, 形状: {data.shape}")
                return is_empty
            except:
                # 如果无法判断，我们安全地返回False（假设不为空）
                logger.warning("无法判断NumPy数组是否为空，假定不为空")
                return False
    elif isinstance(data, (list, tuple)):
        logger.info(f"检查列表/元组是否为空: {len(data) == 0}, 长度: {len(data)}")
        return len(data) == 0
    
    # 对于可能的特殊类型，如DataFrame的GroupBy对象或Series，使用更安全的检查
    try:
        # 尝试使用len判断
        is_empty = len(data) == 0
        logger.info(f"使用len()检查数据是否为空: {is_empty}, 类型: {type(data)}")
        return is_empty
    except (TypeError, AttributeError):
        try:
            # 尝试转换为bool
            is_empty = not bool(data)
            logger.info(f"使用bool()检查数据是否为空: {is_empty}, 类型: {type(data)}")
            return is_empty
        except (ValueError, TypeError):
            # 如果以上都失败，我们假设数据不为空
            logger.warning(f"无法判断{type(data)}类型数据是否为空，假定不为空")
            return False

# 增强的DGraph格式转换方法 - 明确区分技术转换职责
def convert_to_dgraph_format(item, data_type=None):
    """
    将业务对象转换为DGraph可接受的格式.
    注意：
    - 所有的key应该是snake_case格式
    - id字段应该被保留
    - dgraph需要特定的格式，如日期、时间戳等

    Args:
        item: 要转换的项目(已经过业务转换)
        data_type: 数据类型

    Return:
        转换后的项目(技术层面)
    """
    # 跳过None值
    if item is None:
        return None

    # 应该排除的字段
    exclude_fields = []
    
    result = {}
    
    # 确保type字段标识DGraph类型
    if data_type and 'type' not in item:
        type_name = IMPORT_CONFIGS.get(data_type, {}).get('type')
        if type_name:
            result['dgraph.type'] = type_name
    elif 'type' in item:
        # 确保类型字段使用dgraph.type格式
        result['dgraph.type'] = item['type']
        
    # 处理每个字段 - 使用统一的convert_value函数
    for field, value in item.items():
        # 跳过无需的字段
        if field in exclude_fields or field == 'type':
            continue
            
        # 跳过None值和空字符串
        if value is None or (isinstance(value, str) and not value.strip()):
            continue
            
        # 统一转换值
        result[field] = convert_value(value)
            
    return result

# 冲突处理方法
def handle_conflict(txn, item: Dict[str, Any], data_type: str, conflict_strategy: str) -> Tuple[bool, Optional[str]]:
    """
    处理数据导入冲突.
    
    Args:
        txn: DGraph事务
        item: 数据项
        data_type: 数据类型
        conflict_strategy: 冲突处理策略
        
    Return:
        Tuple[bool, Optional[str]]: (是否需要创建新记录, 已存在记录的UID)
    """
    # 如果是insert模式或者没有ID，直接创建新记录
    if conflict_strategy == 'insert' or 'id' not in item:
        return True, None
        
    # 确保ID是字符串类型
    item_id = str(item['id'])
    
    # 查询是否已存在
    query_str = f'{{ q(func: eq(id, "{item_id}")) {{ uid }} }}'
    
    try:
        # 发送查询请求
        res = txn.query(query_str.encode('utf-8'))
        
        # 解析JSON结果
        if isinstance(res, bytes):
            json_data = json.loads(res.decode('utf-8'))
        else:
            json_data = json.loads(res.json)
            
        existing = json_data and 'q' in json_data and len(json_data['q']) > 0
        
        if not existing:
            return True, None  # 不存在，创建新记录
            
        # 记录已存在的uid
        uid = json_data['q'][0]['uid']
        
        if conflict_strategy == 'upsert':
            # 删除已存在的记录
            del_mutation = {
                'uid': uid,
                'dgraph.type': IMPORT_CONFIGS[data_type]['type'],
                'id': item_id
            }
            txn.mutate(del_obj=del_mutation)
            return True, uid  # 删除后创建新记录
        else:  # skip模式
            return False, uid  # 不创建新记录
            
    except Exception as e:
        logger.error(f"处理冲突时出错: {str(e)}")
        logger.error(f"异常详情: {str(e)}")
        
        # 默认创建新记录，避免导致批处理失败
        return True, None

# 事务执行方法
def execute_mutation(txn, mutations: List[Dict[str, Any]]) -> Tuple[int, List[str]]:
    """
    执行DGraph mutation.
    
    Args:
        txn: DGraph事务
        mutations: 要执行的mutation列表
        
    Return:
        Tuple[int, List[str]]: (成功数量, 错误信息列表)
    """
    if not mutations:
        return 0, []
        
    errors = []
    try:
        txn.mutate(set_obj=mutations)
        txn.commit()
        return len(mutations), errors
    except Exception as e:
        txn.discard()
        errors.append(f"执行mutation时出错: {str(e)}")
        return 0, errors

# 检查记录是否存在
def exists(txn, id_value: str, data_type: str) -> bool:
    """
    检查数据是否已存在.
    
    Args:
        txn: DGraph事务
        id_value: 数据ID
        data_type: 数据类型
        
    Return:
        bool: 数据是否存在
    """
    # 确保ID是字符串类型
    id_value = str(id_value)
    
    # 简化查询
    query = f'{{ q(func: eq(id, "{id_value}")) {{ uid }} }}'
    
    try:
        # 发送查询请求
        res = txn.query(query.encode('utf-8'))
        
        # 解析JSON结果 - 兼容pydgraph 24.2.1版本
        if isinstance(res, bytes):
            json_data = json.loads(res.decode('utf-8'))
        else:
            json_data = json.loads(res.json)
            
        return json_data and 'q' in json_data and len(json_data['q']) > 0
    except Exception as e:
        logger.error(f"检查记录是否存在时出错: {str(e)}")
        return False 
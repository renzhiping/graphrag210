#!/usr/bin/env python3
"""
DGraph数据导入模块，提供将数据导入到DGraph的功能。
"""
import argparse
import logging
import sys
import time
import os
from typing import Any, Dict, List, Optional, Union, IO, BinaryIO, TextIO

import numpy as np
import pandas as pd

from graphrag.index.dgraph.client import create_client
from graphrag.index.dgraph.importer.config import IMPORT_CONFIGS
from graphrag.index.dgraph.importer.configs import IMPORT_ORDER
from graphrag.index.dgraph.importer.import_manager import ImportManager
from graphrag.index.dgraph.importer.utils import convert_to_dgraph_format, execute_mutation
from graphrag.index.dgraph.importer.validation import (
    StorageValidator, StorageValidationError, Validator
)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# DGraph导入类
class DGraphImporter:
    """DGraph数据导入器，负责将数据导入到DGraph数据库中."""
    
    def __init__(
        self,
        client=None,
        data_dir: str = '/Users/renzhiping/workspace2/graphrag_root/output',
        batch_size: int = 1000,
        conflict_strategy: str = 'upsert'
    ):
        """
        初始化导入器.
        
        Args:
            client: DGraph客户端对象
            data_dir: 数据目录路径
            batch_size: 批量导入的批次大小
            conflict_strategy: 冲突处理策略(upsert, insert, skip)
        """
        self.client = client
        self.data_dir = data_dir
        self.batch_size = batch_size
        self.conflict_strategy = conflict_strategy
        self.storage_validator = StorageValidator()
        
    def import_data(self, data_types: Optional[list[str]] = None) -> dict[str, int]:
        """
        基于目录导入数据到DGraph.
        
        Args:
            data_types: 要导入的数据类型列表,None表示导入所有类型
            
        Return:
            dict[str, int]: 每种类型的导入数量
        """
        if self.client is None:
            raise ValueError("DGraph客户端未初始化，请在初始化导入器时提供客户端")
            
        results = {}
        start_time = time.time()
        
        # 创建导入管理器
        import_manager = ImportManager(self.data_dir)
        
        # 确定要导入的数据类型
        types_to_import = data_types or IMPORT_ORDER
        if 'all' in types_to_import:
            types_to_import = IMPORT_ORDER
        
        # 按顺序导入数据
        for data_type in types_to_import:
            if data_type not in IMPORT_CONFIGS:
                logger.warning(f"跳过未知数据类型: {data_type}")
                continue
                
            logger.info(f"正在导入 {data_type} 数据...")
            
            # 导入数据
            try:
                # 获取业务转换后的数据 (ImportManager负责文件层和业务层验证)
                converted_data = import_manager.import_data_type(data_type)
                
                # 检查数据是否为空
                if Validator.is_empty(converted_data):
                    logger.warning(f"没有找到 {data_type} 类型的数据")
                    results[data_type] = 0
                    continue
                
                # 批量导入数据到DGraph (进行存储层验证和转换)
                imported_count = self._batch_import(data_type, converted_data)
                results[data_type] = imported_count
                
                logger.info(f"导入 {data_type} 数据完成，共导入 {imported_count} 条记录")
                
            except Exception as e:
                logger.error(f"导入 {data_type} 数据时出错: {str(e)}")
                import traceback
                logger.error(f"错误详情: {traceback.format_exc()}")
                results[data_type] = 0
        
        end_time = time.time()
        logger.info(f"所有数据导入完成，共耗时 {end_time - start_time:.2f} 秒")
        
        return results
    
    def import_from_file(self, file: Union[str, IO, BinaryIO], data_type: str) -> int:
        """
        从Parquet文件对象导入数据到DGraph.
        
        Args:
            file: Parquet文件对象或文件路径
            data_type: 数据类型
            
        Return:
            int: 导入的数据条数
        """
        if self.client is None:
            raise ValueError("DGraph客户端未初始化，请在初始化导入器时提供客户端")
        
        if data_type not in IMPORT_CONFIGS:
            raise ValueError(f"未知数据类型: {data_type}")
        
        logger.info(f"正在导入Parquet格式的{data_type}数据...")
        
        try:
            # 读取Parquet文件
            data = pd.read_parquet(file)
            
            # 使用DataFrame导入方法处理数据
            return self.import_from_dataframe(data, data_type)
            
        except Exception as e:
            logger.error(f"导入Parquet格式的{data_type}数据时出错: {str(e)}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
            return 0
    
    def import_from_dataframe(self, df: pd.DataFrame, data_type: str) -> int:
        """
        从DataFrame导入数据到DGraph.
        
        Args:
            df: 包含要导入数据的DataFrame
            data_type: 数据类型
            
        Return:
            int: 导入的数据条数
        """
        if self.client is None:
            raise ValueError("DGraph客户端未初始化，请在初始化导入器时提供客户端")
        
        if data_type not in IMPORT_CONFIGS:
            raise ValueError(f"未知数据类型: {data_type}")
        
        logger.info(f"正在从DataFrame导入 {data_type} 数据...")
        
        # 检查数据是否为空
        if Validator.is_empty(df):
            logger.warning(f"没有找到 {data_type} 类型的数据")
            return 0
        
        try:
            # 直接批量导入数据到DGraph
            imported_count = self._batch_import(data_type, df)
            logger.info(f"导入 {data_type} 数据完成，共导入 {imported_count} 条记录")
            return imported_count
        except Exception as e:
            logger.error(f"从DataFrame导入 {data_type} 数据时出错: {str(e)}")
            import traceback
            logger.error(f"错误详情: {traceback.format_exc()}")
            return 0
    
    def _batch_import(self, data_type: str, data: Union[pd.DataFrame, List[Dict[str, Any]]]) -> int:
        """
        批量导入数据到DGraph.
        
        Args:
            data_type: 数据类型
            data: 要导入的数据列表
            
        Return:
            int: 导入的数据条数
        """
        # 将DataFrame转换为列表
        if isinstance(data, pd.DataFrame):
            # 处理NaN值
            data = data.replace({np.nan: None})
            
            # 转换为字典列表
            data_list = data.to_dict('records')
        else:
            # 如果已经是列表，不做特殊处理
            data_list = data
            
        if not data_list:
            return 0
            
        total = len(data_list)
        imported = 0
        
        # 分批导入
        for i in range(0, total, self.batch_size):
            batch = data_list[i:i+self.batch_size]
            batch_size = len(batch)
            
            try:
                # 构建并执行DGraph mutation
                txn = self.client.txn()
                try:
                    # 构建mutation数据
                    mutations = []
                    
                    for item in batch:
                        # 确保ID字段是字符串类型
                        if 'id' in item and item['id'] is not None:
                            item['id'] = str(item['id'])
                            
                            # 存储层验证：ID格式验证
                            try:
                                StorageValidator.validate_id_format(item['id'], data_type)
                            except StorageValidationError as e:
                                logger.warning(f"ID格式验证失败: {str(e)}")
                                continue
                            
                            # 存储层验证：冲突处理
                            try:
                                # 检查记录是否已存在
                                if self.conflict_strategy != 'insert':
                                    StorageValidator.validate_unique_constraint(txn, item['id'], data_type)
                                    # 如果没有引发异常，则记录不存在
                                    should_create = True
                                else:
                                    # insert模式总是创建新记录
                                    should_create = True
                            except StorageValidationError:
                                # 记录存在，根据冲突策略决定是否跳过
                                if self.conflict_strategy == 'skip':
                                    logger.debug(f"跳过已存在记录: {item['id']}")
                                    should_create = False
                                else:  # 'upsert'模式
                                    should_create = True
                        else:
                            # 无ID记录，总是创建
                            should_create = True
                        
                        # 验证复合唯一约束
                        try:
                            StorageValidator.validate_composite_unique(txn, item, data_type)
                        except StorageValidationError as e:
                            if self.conflict_strategy == 'skip':
                                logger.debug(f"跳过违反复合唯一约束的记录: {str(e)}")
                                should_create = False
                            # upsert和insert模式继续创建
                        
                        if should_create:
                            # 技术转换 - 将业务对象转换为DGraph格式
                            mutation = convert_to_dgraph_format(item, data_type)
                            mutations.append(mutation)
                    
                    # 执行mutation
                    if mutations:
                        success_count, errors = execute_mutation(txn, mutations)
                        imported += success_count
                        
                        if errors:
                            for error in errors:
                                logger.error(f"批量导入 {data_type} 数据时出错: {error}")
                    
                    # 提交事务
                    txn.commit()
                    
                except Exception as e:
                    # 回滚事务
                    txn.discard()
                    logger.error(f"批量导入 {data_type} 数据时出错: {str(e)}")
                    
            except Exception as e:
                logger.error(f"创建事务时出错: {str(e)}")
            
            logger.info(f"已导入 {data_type} 数据: {imported}/{total}，当前批次: {batch_size}")
            
        return imported


def main():
    """主函数，处理命令行参数并执行导入."""
    parser = argparse.ArgumentParser(description='DGraph数据导入工具')
    parser.add_argument('--data-dir', type=str, default='/Users/renzhiping/workspace2/graphrag_root/output',
                       help='数据目录路径')
    parser.add_argument('--dgraph-host', type=str, default='localhost:9080',
                       help='DGraph服务器地址')
    parser.add_argument('--batch-size', type=int, default=1000,
                       help='批量导入的批次大小')
    parser.add_argument('--types', type=str, nargs='+', default=['all'],
                       help='要导入的数据类型，支持 text_unit, document, entity, relationship, community 或 all')
    parser.add_argument('--conflict', type=str, choices=['upsert', 'insert', 'skip'], default='upsert',
                       help='冲突处理策略')
    parser.add_argument('--file', type=str, default=None,
                       help='单个文件路径（可选，与--type一起使用）')
    parser.add_argument('--type', type=str, default=None,
                       help='单个数据类型（与--file一起使用）')
    
    args = parser.parse_args()
    
    # 创建客户端
    client = create_client(args.dgraph_host)
    
    try:
        # 创建导入器
        importer = DGraphImporter(
            client=client,
            data_dir=args.data_dir,
            batch_size=args.batch_size,
            conflict_strategy=args.conflict
        )
        
        # 导入数据
        if args.file and args.type:
            # 从单个文件导入
            count = importer.import_from_file(args.file, args.type)
            logger.info(f"从文件导入完成: {args.type}: {count}条记录")
        else:
            # 从目录导入
            results = importer.import_data(args.types)
            
            # 输出导入结果
            logger.info("导入结果摘要:")
            for data_type, count in results.items():
                logger.info(f"{data_type}: {count}条记录")
            
    finally:
        # 关闭连接
        client.close()


if __name__ == '__main__':
    main() 
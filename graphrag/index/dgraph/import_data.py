#!/usr/bin/env python3
"""
DGraph数据导入模块，提供将数据导入到DGraph的功能。
"""
import argparse
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Union

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
        data_dir: str = '/Users/renzhiping/workspace2/graphrag_root/output',
        dgraph_host: str = 'localhost:9080',
        batch_size: int = 1000,
        conflict_strategy: str = 'upsert'
    ):
        """
        初始化导入器.
        
        Args:
            data_dir: 数据目录路径
            dgraph_host: DGraph服务器地址
            batch_size: 批量导入的批次大小
            conflict_strategy: 冲突处理策略(upsert, insert, skip)
        """
        self.data_dir = data_dir
        self.dgraph_host = dgraph_host
        self.batch_size = batch_size
        self.conflict_strategy = conflict_strategy
        self.client = None
        self.storage_validator = StorageValidator()
        
    def connect(self):
        """连接到DGraph数据库."""
        logger.info(f"正在连接到DGraph服务器: {self.dgraph_host}")
        self.client = create_client(self.dgraph_host)
            
    def import_data(self, data_types: Optional[list[str]] = None) -> dict[str, int]:
        """
        导入数据到DGraph.
        
        Args:
            data_types: 要导入的数据类型列表，None表示导入所有类型
            
        Return:
            dict[str, int]: 每种类型的导入数量
        """
        if self.client is None:
            self.connect()
            
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
    
    def close(self):
        """关闭DGraph客户端连接."""
        if self.client:
            self.client.close()
            self.client = None


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
    
    args = parser.parse_args()
    
    # 创建导入器
    importer = DGraphImporter(
        data_dir=args.data_dir,
        dgraph_host=args.dgraph_host,
        batch_size=args.batch_size,
        conflict_strategy=args.conflict
    )
    
    try:
        # 导入数据
        results = importer.import_data(args.types)
        
        # 输出导入结果
        logger.info("导入结果摘要:")
        for data_type, count in results.items():
            logger.info(f"{data_type}: {count}条记录")
            
    finally:
        # 关闭连接
        importer.close()


if __name__ == '__main__':
    main() 
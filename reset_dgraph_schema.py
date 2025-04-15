#!/usr/bin/env python3
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""删除DGraph数据库中的所有数据和schema，然后重新初始化schema。"""

import logging
import time

from graphrag.index.dgraph.client import create_client
from graphrag.index.dgraph.init_schema import init_schema

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def reset_dgraph_schema():
    """删除现有DGraph schema并重新初始化。"""
    logger.info("开始重置DGraph数据库schema...")
    
    try:
        # 创建DGraph客户端
        client = create_client()
        logger.info("已连接到DGraph服务器")
        
        # 初始化schema，设置drop_existing=True以删除现有数据和schema
        logger.info("正在删除现有的数据和schema...")
        init_schema(client, schema_type="complete", drop_existing=True)
        
        logger.info("DGraph数据库schema已成功重置")
        return True
    except Exception as e:
        logger.error(f"重置DGraph schema时出错: {str(e)}")
        return False

if __name__ == "__main__":
    start_time = time.time()
    success = reset_dgraph_schema()
    elapsed_time = time.time() - start_time
    
    if success:
        logger.info(f"操作完成，耗时: {elapsed_time:.2f}秒")
    else:
        logger.error("操作失败") 
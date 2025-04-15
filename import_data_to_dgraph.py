#!/usr/bin/env python3
"""
DGraph数据导入工具命令行入口点。
此文件是对graphrag.index.dgraph.import_data模块的包装，
提供向后兼容性和方便的命令行入口。
"""
import sys

# 导入重构后的模块
from graphrag.index.dgraph.import_data import main

if __name__ == '__main__':
    # 直接调用重构后的主函数
    main()



#!/usr/bin/env python3
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""初始化DGraph数据库的schema,支持批量或单独导入各种类型的schema."""

import pydgraph

from graphrag.index.dgraph.schema_definitions import (
    define_community_report_schema,
    define_community_schema,
    define_document_schema,
    define_entity_schema,
    define_relationship_schema,
    define_text_unit_schema,
)


def set_schema(client: pydgraph.DgraphClient, schema: str) -> None:
    """
    设置DGraph数据库的schema.
    
    Args:
        client: DGraph客户端
        schema: schema定义字符串
    """
    try:
        op = pydgraph.Operation(schema=schema)
        client.alter(op)
    except Exception as e:  # noqa: BLE001
        error = f"设置schema时出错: {e}"
        raise ValueError(error)  # noqa: B904


def get_schema_by_type(schema_type: str) -> str:
    """
    获取指定类型的DGraph schema定义.
    
    Args:
        schema_type: Schema类型（text_unit, document, entity, relationship, 
                   community, community_report, complete）
        
    Return:
        str: Schema定义字符串
    """
    schema_map = {
        "text_unit": define_text_unit_schema(),
        "document": define_document_schema(),
        "entity": define_entity_schema(),
        "relationship": define_relationship_schema(),
        "community": define_community_schema(),
        "community_report": define_community_report_schema(),
    }
    
    if schema_type not in schema_map and schema_type != "complete":
        error = f"未知的schema类型: {schema_type}. "
        error += f"可用类型: {', '.join(list(schema_map.keys()) + ['complete'])}"
        raise ValueError(error)
    
    return schema_map[schema_type]


def drop_all(client: pydgraph.DgraphClient) -> None:
    """
    删除DGraph数据库中的所有数据和schema.
    
    Args:
        client: DGraph客户端
    """
    try:
        op = pydgraph.Operation(drop_all=True)
        client.alter(op)
    except Exception as e:  # noqa: BLE001
        error = f"删除数据时出错: {e}"
        raise ValueError(error)  # noqa: B904


def init_schema(
    client: pydgraph.DgraphClient,
    schema_type: str = "complete",
    drop_existing: bool = False
) -> None:
    """初始化DGraph数据库schema.

    Args:
        client: DGraph客户端
        schema_type: Schema类型
        drop_existing: 是否删除现有数据和schema
    """
    try:
        # 如果需要,删除所有现有数据和schema
        if drop_existing:
            drop_all(client)
        
        if schema_type == "complete":
            # 依次执行所有schema定义并应用到数据库
            schema_types = [
                "text_unit", 
                "document", 
                "entity", 
                "relationship", 
                "community", 
                "community_report"
            ]
            for type_name in schema_types:
                schema = get_schema_by_type(type_name)
                set_schema(client, schema)
                print(f"Schema '{type_name}' 初始化成功")
        else:
            # 获取schema定义
            schema = get_schema_by_type(schema_type)
            # 设置schema
            set_schema(client, schema)
            print(f"Schema '{schema_type}' 初始化成功")
    finally:
        # 关闭客户端连接
        client.close()



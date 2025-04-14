#!/usr/bin/env python3
# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph客户端工具，提供创建和管理DGraph连接的功能."""

from typing import Any, Optional, Union

import grpc
import pydgraph


class DGraphClient:
    """DGraph客户端管理类，提供创建和管理DGraph连接的功能."""
    
    def __init__(
        self, 
        hosts: Union[list[str], str] = "localhost:9080",
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[dict[str, Any]] = None
    ):
        """
        初始化DGraph客户端.
        
        Args:
            hosts: DGraph服务器地址，可以是单个字符串或地址列表
                  默认为"localhost:9080"
            credentials: gRPC证书，用于安全连接
            options: gRPC连接选项
        """
        self.hosts = [hosts] if isinstance(hosts, str) else hosts
        self.credentials = credentials
        self.options = options or {}
        self._client = None
    
    def connect(self) -> pydgraph.DgraphClient:
        """
        创建并返回DGraph客户端连接.
        
        Return:
            pydgraph.DgraphClient: DGraph客户端对象
        
        Raise:
            ConnectionError: 当连接失败时抛出
        """
        try:
            # 创建客户端存根(client_stubs)
            stubs = []
            for host in self.hosts:
                if self.credentials:
                    stub = pydgraph.DgraphClientStub(host, self.credentials, self.options)
                else:
                    stub = pydgraph.DgraphClientStub(host, options=self.options)
                stubs.append(stub)
            
            # 创建客户端
            return pydgraph.DgraphClient(*stubs)
            
        except Exception as e:
            error = f"连接DGraph失败: {e}"
            raise ConnectionError(error) from e
    
    def get_client(self) -> pydgraph.DgraphClient:
        """获取已创建的DGraph客户端，如果客户端不存在，将自动创建."""
        """
        Return:
            pydgraph.DgraphClient: DGraph客户端对象
        """
        if self._client is None:
            return self.connect()
        return self._client
    
    def close(self) -> None:
        """关闭所有连接."""
        if self._client:
            self._client.close()
            self._client = None


def create_client(
    hosts: Union[list[str], str] = "localhost:9080",
    credentials: Optional[grpc.ChannelCredentials] = None,
    options: Optional[dict[str, Any]] = None
) -> pydgraph.DgraphClient:
    """
    创建并返回DGraph客户端的便捷函数.
    
    Args:
        hosts: DGraph服务器地址，可以是单个字符串或地址列表
              默认为"localhost:9080"
        credentials: gRPC证书，用于安全连接
        options: gRPC连接选项
    
    Return:
        pydgraph.DgraphClient: DGraph客户端对象
    """
    client_manager = DGraphClient(hosts, credentials, options)
    return client_manager.connect()

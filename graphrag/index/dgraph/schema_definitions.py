# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""DGraph schema定义，基于Graphene模型."""

# 文本单元(TextUnit)的schema定义
def define_text_unit_schema() -> str:
    """
    定义与Graphene的TextUnit模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    text: string @index(fulltext) .
    n_tokens: int .
    
    # 关系ID列表
    document_ids: [string] @index(exact) .
    entity_ids: [string] @index(exact) .
    relationship_ids: [string] @index(exact) .
    covariate_ids: [string] @index(exact) .
    
    # 关系定义(未来扩展)
    documents: [uid] @reverse .
    entities: [uid] @reverse .
    relationships: [uid] @reverse .
    communities: [uid] @reverse .
    
    # TextUnit类型定义
    type TextUnit {
        id
        human_readable_id
        text
        n_tokens
        document_ids
        entity_ids
        relationship_ids
        covariate_ids
        documents
        entities
        relationships
        communities
    }
    """
    return schema


# 文档(Document)的schema定义
def define_document_schema() -> str:
    """
    定义与Graphene的Document模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    title: string @index(term) .
    text: string @index(fulltext) .
    
    # 关系
    text_unit_ids: [string] @index(exact) .
    text_units: [uid] @reverse .
    
    # 其他属性
    creation_date: datetime .
    metadata: string .
    
    # Document类型定义
    type Document {
        id
        human_readable_id
        title
        text
        text_unit_ids
        creation_date
        metadata
        text_units
    }
    """
    return schema


# 实体(Entity)的schema定义
def define_entity_schema() -> str:
    """
    定义与Graphene的Entity模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    title: string @index(term) .
    type: string @index(exact) .
    description: string @index(fulltext) .
    
    # 关系
    text_unit_ids: [string] @index(exact) .
    text_units: [uid] @reverse .
    related_entities: [uid] @reverse .
    
    # 可视化属性
    frequency: int .
    degree: int .
    x: float .
    y: float .
    
    # Entity类型定义
    type Entity {
        id
        human_readable_id
        title
        type
        description
        text_unit_ids
        frequency
        degree
        x
        y
        text_units
        related_entities
    }
    """
    return schema  # noqa: RET504


# 关系(Relationship)的schema定义
def define_relationship_schema() -> str:
    """
    定义与Graphene的Relationship模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    source: string @index(exact) .
    target: string @index(exact) .
    description: string @index(fulltext) .
    
    # 关系
    text_unit_ids: [string] @index(exact) .
    text_units: [uid] @reverse .
    source_entity: uid .
    target_entity: uid .
    
    # 其他属性
    weight: float .
    combined_degree: int .
    
    # Relationship类型定义
    type Relationship {
        id
        human_readable_id
        source
        target
        description
        weight
        combined_degree
        text_unit_ids
        source_entity
        target_entity
        text_units
    }
    """
    return schema  # noqa: RET504


# 社区(Community)的schema定义
def define_community_schema() -> str:
    """
    定义与Graphene的Community模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    title: string @index(term) .
    community: int .
    level: int .
    
    # 层次关系
    parent: int .
    children: [int] .
    child_communities: [uid] @reverse .
    
    # 包含内容
    entity_ids: [string] @index(exact) .
    relationship_ids: [string] @index(exact) .
    text_unit_ids: [string] @index(exact) .
    
    # 关系
    entities: [uid] @reverse .
    relationships: [uid] @reverse .
    text_units: [uid] @reverse .
    reports: [uid] @reverse .
    
    # 其他属性
    period: datetime .
    size: int .
    
    # Community类型定义
    type Community {
        id
        human_readable_id
        community
        level
        parent
        children
        title
        entity_ids
        relationship_ids
        text_unit_ids
        period
        size
        entities
        relationships
        text_units
        child_communities
        reports
    }
    """
    return schema


# 社区报告(CommunityReport)的schema定义
def define_community_report_schema() -> str:
    """
    定义与Graphene的CommunityReport模型一致的DGraph schema.
    
    Return:
        str: DGraph的schema定义字符串
    """
    schema = """
    # 基本属性
    id: string @index(exact) .
    human_readable_id: string .
    community: int .
    title: string @index(term) .
    report_content: string @index(fulltext) .
    summary: string @index(fulltext) .
    findings: string @index(fulltext) .
    explanation: string @index(fulltext) .
    rating: int .
    level: int .
    
    # 日期相关
    period: datetime .
    create_time: datetime .
    
    # 关系ID列表
    entity_ids: [string] @index(exact) .
    text_unit_ids: [string] @index(exact) .
    
    # 关系
    community_rel: uid .
    entities: [uid] @reverse .
    text_units: [uid] @reverse .
    
    # JSON字段
    full_content_json: string .
    
    # CommunityReport类型定义
    type CommunityReport {
        id
        human_readable_id
        community
        title
        report_content
        summary
        findings
        explanation
        rating
        level
        period
        create_time
        entity_ids
        text_unit_ids
        community_rel
        entities
        text_units
        full_content_json
    }
    """
    return schema  # noqa: RET504


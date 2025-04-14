# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""GraphRAG data model for DGraph using Graphene."""


import graphene


class Document(graphene.ObjectType):
    """Document model representing documents.parquet."""
    
    id = graphene.ID(required=True, description="Document ID")
    human_readable_id = graphene.String(description="Human readable ID")
    title = graphene.String(description="Document title")
    text = graphene.String(description="Document content text")
    text_unit_ids = graphene.List(graphene.ID, description="IDs of text units in this document")
    creation_date = graphene.DateTime(description="Document creation date")
    metadata = graphene.JSONString(description="Document metadata")

    # Relationships
    text_units = graphene.List(lambda: TextUnit, description="Text units in this document")


class Entity(graphene.ObjectType):
    """Entity model representing entities.parquet."""

    id = graphene.ID(required=True, description="Entity ID")
    human_readable_id = graphene.String(description="Human readable ID")
    title = graphene.String(description="Entity title")
    type = graphene.String(description="Entity type (e.g., PERSON, ORGANIZATION, GEO)")
    description = graphene.String(description="Entity description")
    text_unit_ids = graphene.List(graphene.ID, description="IDs of text units mentioning this entity")
    frequency = graphene.Int(description="Entity frequency")
    degree = graphene.Int(description="Entity connectivity degree")
    x = graphene.Float(description="X coordinate for visualization")
    y = graphene.Float(description="Y coordinate for visualization")

    # Relationships
    text_units = graphene.List(lambda: TextUnit, description="Text units mentioning this entity")
    related_entities = graphene.List(lambda: Relationship, description="Relationships to other entities")
    communities = graphene.List(lambda: Community, description="Communities this entity belongs to")


class Relationship(graphene.ObjectType):
    """Relationship model representing relationships.parquet."""

    id = graphene.ID(required=True, description="Relationship ID")
    human_readable_id = graphene.String(description="Human readable ID")
    source = graphene.ID(description="Source entity ID")
    target = graphene.ID(description="Target entity ID")
    description = graphene.String(description="Relationship description")
    weight = graphene.Float(description="Relationship weight")
    combined_degree = graphene.Int(description="Combined degree of the connected entities")
    text_unit_ids = graphene.List(graphene.ID, description="IDs of text units containing this relationship")

    # Relationships
    source_entity = graphene.Field(Entity, description="Source entity")
    target_entity = graphene.Field(Entity, description="Target entity")
    text_units = graphene.List(lambda: TextUnit, description="Text units containing this relationship")


class TextUnit(graphene.ObjectType):
    """TextUnit model representing text_units.parquet."""

    id = graphene.ID(required=True, description="Text unit ID")
    human_readable_id = graphene.String(description="Human readable ID")
    text = graphene.String(description="Text unit content")
    n_tokens = graphene.Int(description="Number of tokens in the text unit")
    document_ids = graphene.List(graphene.ID, description="IDs of documents containing this text unit")
    entity_ids = graphene.List(graphene.ID, description="IDs of entities mentioned in this text unit")
    relationship_ids = graphene.List(graphene.ID, description="IDs of relationships in this text unit")
    covariate_ids = graphene.List(graphene.ID, description="IDs of covariates in this text unit")
    
    # Relationships
    documents = graphene.List(Document, description="Documents containing this text unit")
    entities = graphene.List(Entity, description="Entities mentioned in this text unit")
    relationships = graphene.List(Relationship, description="Relationships in this text unit")
    communities = graphene.List(lambda: Community, description="Communities this text unit belongs to")


class Community(graphene.ObjectType):
    """Community model representing communities.parquet."""

    id = graphene.ID(required=True, description="Community ID")
    human_readable_id = graphene.String(description="Human readable ID")
    community = graphene.Int(description="Community number")
    level = graphene.Int(description="Community hierarchical level")
    parent = graphene.ID(description="Parent community ID")
    children = graphene.List(graphene.ID, description="Child community IDs")
    title = graphene.String(description="Community title")
    entity_ids = graphene.List(graphene.ID, description="Entity IDs in this community")
    relationship_ids = graphene.List(graphene.ID, description="Relationship IDs in this community")
    text_unit_ids = graphene.List(graphene.ID, description="Text unit IDs in this community")
    period = graphene.DateTime(description="Community time period")
    size = graphene.Int(description="Community size")

    # Relationships
    entities = graphene.List(Entity, description="Entities in this community")
    relationships = graphene.List(Relationship, description="Relationships in this community")
    text_units = graphene.List(TextUnit, description="Text units in this community")
    parent_community = graphene.Field(lambda: Community, description="Parent community")
    child_communities = graphene.List(lambda: Community, description="Child communities")
    reports = graphene.List(lambda: CommunityReport, description="Reports for this community")


class CommunityReport(graphene.ObjectType):
    """CommunityReport model representing community_reports.parquet."""

    id = graphene.ID(required=True, description="Community report ID")
    community_id = graphene.ID(description="Community ID this report belongs to")
    title = graphene.String(description="Report title")
    content = graphene.String(description="Report content")
    summary = graphene.String(description="Report summary")
    keywords = graphene.List(graphene.String, description="Report keywords")
    level = graphene.Int(description="Community level of this report")
    
    # Relationships
    community = graphene.Field(Community, description="Community this report belongs to") 
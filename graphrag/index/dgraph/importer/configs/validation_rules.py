# Copyright (c) 2024 Microsoft Corporation.
# Licensed under the MIT License

"""验证规则配置文件，定义不同数据类型在不同层的验证规则."""

# 数据类型字段定义 - 中心化定义所有数据类型的字段
DATA_TYPE_FIELDS = {
    "document": {
        "required_fields": ["id", "title", "text"],
        "optional_fields": ["human_readable_id", "creation_date", "metadata", "text_unit_ids"],
    },
    "text_unit": {
        "required_fields": ["id", "text"],
        "optional_fields": ["human_readable_id", "n_tokens", "document_ids", "entity_ids", 
                           "relationship_ids", "covariate_ids"],
    },
    "entity": {
        "required_fields": ["id", "title", "type","description"],
        "optional_fields": ["human_readable_id", "frequency", "degree", "x", "y",
                           "text_unit_ids"],
    },
    "relationship": {
        "required_fields": ["id", "source", "target"],
        "optional_fields": ["human_readable_id", "description", "weight", "combined_degree", "text_unit_ids", "type"],
    },
    "community": {
        "required_fields": ["id", "community", "level", "title"],
        "optional_fields": ["human_readable_id", "parent", "size", "period",
                           "children", "entity_ids", "relationship_ids", "text_unit_ids", "name", "members"],
    },
    "community_report": {
        "required_fields": ["id", "community", "full_content_json"],
        "optional_fields": ["human_readable_id", "summary", "title", "findings", "rating",
                           "explanation", "create_time", "period", "level",
                           "entity_ids", "text_unit_ids", "data", "created_at"],
    }
}

# 验证规则配置
VALIDATION_RULES = {
    "document": {
        # 文件层验证规则
        "file_layer": {
            "supported_formats": ["csv", "parquet"],
            "required_columns": DATA_TYPE_FIELDS["document"]["required_fields"],
        },
        
        # 业务层验证规则
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["document"]["required_fields"],
            "min_content_length": 1,
        },
        
        # 存储层验证规则
        "storage_layer": {
            "id_format": "hash",
            "unique_constraints": ["id"]
        }
    },
    
    "text_unit": {
        "file_layer": {
            "supported_formats": ["csv", "parquet"],
            "required_columns": DATA_TYPE_FIELDS["text_unit"]["required_fields"],
        },
        
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["text_unit"]["required_fields"],
            "max_text_length": 10000
        },
        
        "storage_layer": {
            "id_format": "hash",
            "unique_constraints": ["id"]
        }
    },
    
    "entity": {
        "file_layer": {
            "supported_formats": ["csv", "parquet"],
            "required_columns": DATA_TYPE_FIELDS["entity"]["required_fields"],
        },
        
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["entity"]["required_fields"]
            # "valid_types": ["PERSON", "ORGANIZATION", "location", "concept", "other"]
        },
        
        "storage_layer": {
            "id_format": "uuid",
            "unique_constraints": ["id"]
        }
    },
    
    "relationship": {
        "file_layer": {
            "supported_formats": ["csv", "parquet"],
            "required_columns": DATA_TYPE_FIELDS["relationship"]["required_fields"],
        },
        
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["relationship"]["required_fields"]
            #"valid_types": ["related", "contains", "same_document", "belongs_to", "created_by"]
        },
        
        "storage_layer": {
            "composite_unique": ["source", "target"]
        }
    },
    
    "community": {
        "file_layer": {
            "supported_formats": ["csv", "parquet"],
            "required_columns": DATA_TYPE_FIELDS["community"]["required_fields"],
        },
        
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["community"]["required_fields"],
            "min_members": 1
        },
        
        "storage_layer": {
            "id_format": "uuid",
            "unique_constraints": ["id"]
        }
    },
    
    "community_report": {
        "file_layer": {
            "supported_formats": ["csv", "parquet", "json"],
            "required_columns": DATA_TYPE_FIELDS["community_report"]["required_fields"],
        },
        
        "business_layer": {
            "required_fields": DATA_TYPE_FIELDS["community_report"]["required_fields"],
        },
        
        "storage_layer": {
            "id_format": "uuid",
            "unique_constraints": ["id"]
        }
    }
}

# 导出验证规则
def get_validation_rules(data_type, layer=None):
    """
    获取指定数据类型和层的验证规则.
    
    Args:
        data_type: 数据类型
        layer: 验证层名称(file_layer, business_layer, storage_layer)，None表示返回所有层
        
    Return:
        dict: 验证规则字典
    """
    if data_type not in VALIDATION_RULES:
        return {}
        
    if layer:
        return VALIDATION_RULES[data_type].get(layer, {})
        
    return VALIDATION_RULES[data_type]

# 获取数据类型字段定义
def get_data_type_fields(data_type):
    """
    获取指定数据类型的字段定义.
    
    Args:
        data_type: 数据类型
        
    Return:
        dict: 字段定义字典，包含required_fields和optional_fields
    """
    return DATA_TYPE_FIELDS.get(data_type, {"required_fields": [], "optional_fields": []})
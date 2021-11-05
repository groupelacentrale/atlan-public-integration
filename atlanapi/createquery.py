import json

class AtlanQuery:
    def __init__(self, qualified_name, query="*", asset_type="AtlanTable"):
        self.qualified_name = qualified_name
        self.query = query
        self.asset_type = asset_type


class AtlanQuerySerializer:
    """
    Returns a query request based on an asset type and its qualified name.
    """
    def serialize(self, atlanquery):
        query = {
            "searchType": "BASIC",
            "typeName": "AtlanAsset",
            "excludeDeletedEntities": True,
            "includeClassificationAttributes": True,
            "includeSubClassifications": True,
            "includeSubTypes": True,
            "limit": 20,
            "offset": 0,
            "attributes": [
                "description",
                "name",
                "displayName",
                "rowCount",
                "colCount",
                "source",
                "sourceType",
                "integration",
                "integrationType",
                "status",
                "statusMeta",
                "type",
                "typeName",
                "updatedAt",
                "updatedBy",
                "lastSyncedAt",
                "lastSyncedBy",
                "dataType",
                "experts",
                "owners",
                "previewImageId",
                "previewImageId",
                "url"
            ],
            "query": atlanquery.query,
            "entityFilters": {
                "condition": "AND",
                "criterion": [
                    {
                        "attributeName": "typeName",
                        "attributeValue": atlanquery.asset_type,
                        "operator": "eq"
                    },
                    {
                        "attributeName": "qualifiedName",
                        "attributeValue": atlanquery.qualified_name,
                        "operator": "ENDS_WITH"
                    }
                ]
            }
        }
        return json.dumps(query)
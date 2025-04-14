from graphrag.index.dgraph.client import create_client  # noqa: D100
from graphrag.index.dgraph.init_schema import init_schema

client = create_client()

init_schema(client, schema_type="complete", drop_existing=True)
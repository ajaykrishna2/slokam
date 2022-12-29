import json
from pyspark.sql.types import StructType

with open("dataflow_schema.json", "r") as fp:
    json_schema_str = fp.read()
    my_schema = StructType.fromJson(json.loads(json_schema_str))
    print(my_schema)
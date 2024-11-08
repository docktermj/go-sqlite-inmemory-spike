#! /usr/bin/env python3

import json
from urllib.parse import urlparse

engine_config_json = '{"PIPELINE":{"CONFIGPATH":"/etc/opt/senzing","RESOURCEPATH":"/opt/senzing/er/resources","SUPPORTPATH":"/opt/senzing/data"},"SQL":{"CONNECTION": "sqlite3://na:na@/MYPRIVATE_DB?mode=memory&cache=shared"}}'


uri = json.loads(engine_config_json)["SQL"]["CONNECTION"]

print(uri)
parsed = urlparse(uri)
print(parsed)
connectionString = "file:" + parsed.path[1:] + "?mode=memory&cache=shared"

print(connectionString)

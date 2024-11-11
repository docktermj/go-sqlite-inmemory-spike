#! /usr/bin/env python3

import sqlite3
from urllib.parse import urlparse

from senzing import SzAbstractFactory

DATABASE_URL = "sqlite3://na:na@/MYPRIVATE_DB?mode=memory&cache=shared"
SQL_FILE = "/opt/senzing/er/resources/schema/szcore-schema-sqlite-create.sql"
FACTORY_PARAMETERS = {
    "instance_name": "Example",
    "settings": {
        "PIPELINE": {
            "CONFIGPATH": "/etc/opt/senzing",
            "RESOURCEPATH": "/opt/senzing/er/resources",
            "SUPPORTPATH": "/opt/senzing/data",
        },
        "SQL": {"CONNECTION": DATABASE_URL},
    },
}

# Create database connection.

database_url_parsed = urlparse(DATABASE_URL)
connection_string = "file:" + database_url_parsed.path[1:] + "?mode=memory&cache=shared"
database_connection = sqlite3.connect(connection_string, autocommit=True)

# Create Senzing schema in database.

database_cursor = database_connection.cursor()
with open(SQL_FILE) as schema_file:
    for line in schema_file:
        line = line.strip()
        if not line:
            continue
        database_cursor.execute(line)

# Create Senzing objects.

sz_abstract_factory = SzAbstractFactory(**FACTORY_PARAMETERS)
sz_config = sz_abstract_factory.create_sz_config()
sz_config_manager = sz_abstract_factory.create_sz_configmanager()

# Install default Senzing configuration.

config_handle = sz_config.create_config()
config_string = sz_config.export_config(config_handle)
config_id = sz_config_manager.add_config(config_string, "Test comment")
sz_config_manager.set_default_config_id(config_id)

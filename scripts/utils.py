"""
File : /scripts/utils.py

Description:
1. Setup a connection to the PostgreSQL database
"""
import os
import json
from configparser import ConfigParser
import psycopg2
from elasticsearch import Elasticsearch
from datetime import datetime
from psycopg2.extras import Json
from psycopg2 import extensions

class CustomJSONEncoder(json.JSONEncoder):
    """
    This is a custom class that extends the `json.JSONEncoder` class. It overrides the `default()` method
    to handle datetime objects in a JSON-serializable format.
    """
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

def getConfig():
    """
    This function reads a configuration file named 'config.ini' located in the parent directory of the current script.
    
    Returns:
        ConfigParser object that can be used to access the configuration values.
    """
    _cwd = os.getcwd()

    _abspath = os.path.abspath(__file__)
    _dname = os.path.dirname(_abspath)
    os.chdir(_dname)

    _config = ConfigParser()
    _config.read('../config.ini')

    os.chdir(_cwd)

    return _config


def connSQL():
    """
    This function creates a connection to a PostgreSQL database using the configuration values read from a 'config.ini' file.
    
    Returns:
        psycopg2 connection object that can be used to interact with the database.
    """
    config = getConfig()
    
    _connection = psycopg2.connect(
        **dict(config.items('postgres'))
    )

    return _connection

def connNoSQL():
    """
    This function creates a connection to an Elasticsearch database using the configuration values read from a 'config.ini' file.
    
    Returns:
        Elasticsearch client object that can be used to interact with the database.
    """
    config = getConfig()

    esConfig = dict(config.items('elasticsearch'))
    
    _client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password']),
        max_retries=3,
        retry_on_timeout=True
    )

    return _client

async def pushLogs(key, result, response_time):
    """
    This function inserts a new row into a PostgreSQL database table called 'logs' using the provided cursor object.
    The row contains the following values:
    - A JSON-serialized representation of the key (if it is a dictionary), or the key value (if it is not)
    - The response time (in seconds)
    - The response as a JSONB column in the database (as psycopg2.extras.Json)

    Parameters:
    - key: the key associated with the response (can be a dictionary or a simple value)
    - result: the response to be logged (any JSON-serializable object)
    - response_time: the response time in seconds (can be None)
    """
    _connection = connSQL()
    _cursor = _connection.cursor()

    if isinstance(key, dict):
        key = json.dumps(key)
    
    created_at = datetime.now()
    created_at = json.dumps(created_at,cls=CustomJSONEncoder)



    if bool(key):
        key = extensions.adapt(key).getquoted().decode('utf-8')
        key = key.replace('"', '').replace("%", "%%")
    
    sql_query=f"""
            INSERT INTO
                logs (query, created_at, time_taken)
            VALUES
                ({key},{Json(created_at)},{response_time})
        """
    
    print(sql_query)

    _cursor.execute(
        sql_query
    )

    _connection.commit()
    _cursor.close()
    _connection.close()

def removeNoSQLData():
    """
    This function connects to Elasticsearch using credentials from the `config.ini` file,
    and then attempts to delete all indices using the `_all` parameter.
    """
    config = getConfig()

    esConfig = dict(config.items('elasticsearch'))
    
    _client = Elasticsearch(
        f"http://{esConfig['host']}:{esConfig['port']}",
        basic_auth = (esConfig['elastic_username'], esConfig['elastic_password'])
    )

    try:
        _client.indices.delete(index='_all')
        print('Elastic Search: *** Index Data Delete Successful ***')
    except Exception as e:
        print(f'Elastic Search: *** Index Data Delete Unsuccessful as {e} ***')

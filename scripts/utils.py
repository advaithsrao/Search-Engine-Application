"""
File : /scripts/utils.py

Description:
1. Setup a connection to the PostgreSQL database
"""
from configparser import ConfigParser
import psycopg2

config = ConfigParser()
config.read('config.ini')

def connSQL():
    connection = psycopg2.connect(
        **dict(config.items('postgres'))
    )

    return connection
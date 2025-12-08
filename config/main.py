from configparser import ConfigParser
import os
import psycopg2

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def load_config(filename=None, section="postgresql"):
    if filename is None:
        filename = os.path.join(BASE_DIR, "config", "database.ini")
    
    parser = ConfigParser()
    parser.read(filename)

    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(
            "Section {0} not found in the {1} file".format(section, filename)
        )

    return config


def connect():
    """Connect to PostgreSQL and return a live connection."""
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        print("Connected to the PostgreSQL server.")
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Database connection error: {error}")
        raise

from ast import Str
import psycopg2
from dotenv import load_dotenv, dotenv_values
import json
import logging

logging.basicConfig(level=logging.NOTSET)

load_dotenv()
settings = dotenv_values("aws/.env")
settings_dict = json.loads(json.dumps(settings))

REDSHIFT_USERNAME=settings_dict["REDSHIFT_USERNAME"]
REDSHIFT_PASSWORD=settings_dict["REDSHIFT_PASSWORD"]
REDSHIFT_ENDPOINT=settings_dict["REDSHIFT_ENDPOINT"]
REDSHIFT_PORT=settings_dict["REDSHIFT_PORT"]
REGION_NAME=settings_dict["AWS_REGION"]

config_dict = { 
    'user': REDSHIFT_USERNAME, 
    'password': REDSHIFT_PASSWORD,
    'host' : REDSHIFT_ENDPOINT, 
    'port': REDSHIFT_PORT, 
}


def create_conn(dbname:str):
    try:
        conn=psycopg2.connect(
            user = config_dict['user'],
            password = config_dict['password'],
            host = config_dict['host'],
            port = config_dict['port'],
            dbname = dbname,
        )
    except Exception as err:
        print(err)
    return conn

def default_query():
    return """
        SELECT *    
        FROM pg_table_def    
        WHERE tablename = 'sales';    
        """

def execute_sql_command(dbname:str, command):
    try:
        client = create_conn(dbname)
        cursor = client.cursor()
        cursor.execute(command)
        rows = cursor.fetchall()
        for row in rows:
            logging.info(row)
    except Exception as e:
        logging.error(e)
    finally:
        cursor.close()
        client.close()

# OLD
# def select(cursor, query):
#     try:       
#         cursor.execute(query)
#     except Exception as err:
#             print(err.code,err)
 
#     rows = cursor.fetchall()
#     for row in rows:
#         print(row)
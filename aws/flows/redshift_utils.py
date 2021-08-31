import datetime
import psycopg2
from dotenv import load_dotenv, dotenv_values
import json
import logging

logging.basicConfig(level=logging.NOTSET)

def load_settings():
    import prefect
    if prefect.context.get("flow_run_id"):
        # know we're in cloud
        return prefect.client.Secret("REDSHIFT CHAMBER OF SECRETS").get()
    else:
        # locally
        # put all of your load_dotenv logic here and return
        load_dotenv()
        settings = dotenv_values("aws/.env")
        settings_dict = json.loads(json.dumps(settings))
        return settings_dict

settings_dict = load_settings()

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
        return conn
    except Exception as err:
        print(err)

def default_query():
    return """
        SELECT *    
        FROM pg_table_def    
        WHERE tablename = 'sales';    
        """

def execute_sql_command(client, command):
    try:
        cursor = client.cursor()
        cursor.execute(command)
        rows = cursor.fetchall()
        for row in rows:
            logging.info(row)
        cursor.close()
    except Exception as e:
        logging.error(e)
    finally:
        if client is not None:
            client.close()

def execute_many_commands(client, commands):
    """ 
        Execute multiple SQL commands
        Takes:
        - client object
        - commands, iterable
    """
    try:
        cur = client.cursor()
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        client.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if client is not None:
            client.close()

def insert_df(client, df, table):
    cursor = client.cursor()
    try:
        cursor = client.cursor()
        for i in df.index:
            cols  = ','.join(list(df.columns))
            value_data  = [df.at[i,col] for col in list(df.columns)]
            # value_str = ",".join([str(item) for item in value_data])
            # query = f"INSERT INTO {table}({cols}) VALUES({str(value_str)})"
            value1, value2 = str(value_data[0]), value_data[1]
            query = f"INSERT INTO {table}({cols}) VALUES({value1},\'{value2}\',\'{datetime.datetime.now().isoformat()}\');"
            cursor.execute(query)
            print(f"Query: {query}\n")
        cursor.close()
        # commit the changes
        client.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if client is not None:
            client.close()
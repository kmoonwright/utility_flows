import utils.redshift as rs
import logging

logging.basicConfig(level=logging.NOTSET)

def main():
    logging.info("Connection starting....")
    conn = rs.create_conn()
    logging.info("Connection complete")

    logging.info("Initiating query 1...")
    cursor = conn.cursor()
    query = rs.default_query()
    rs.select(cursor, query)

    logging.info("Initiating query 2....")
    query = """
        SELECT *    
        FROM pg_table_def    
        WHERE tablename = 'sales';    
    """
    rs.execute_sql(query)
    logging.info("Query complete.")

    logging.info("Initiating shutdown....")

    logging.info("\n<<<<--->>>>")
    logging.info("\n<<<<DONEZO>>>>")
    logging.info("\n<<<<------------>>>>")

if __name__ == "__main__":
    main()
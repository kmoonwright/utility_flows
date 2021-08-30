import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.redshift as rs
import logging

logging.basicConfig(level=logging.NOTSET)

def main():
    logging.info("Connection starting....")

    conn = rs.create_conn("dev")

    logging.info("Connection complete")
    logging.info("Initiating query 1...")

    command = """
        SELECT *    
        FROM pg_table_def    
        WHERE tablename = 'sales';    
    """
    rs.execute_sql_command(conn, command)

    logging.info("Query complete.")
    logging.info("Initiating shutdown....")

    logging.info("\n<<<<DONEZO>>>>")

if __name__ == "__main__":
    main()
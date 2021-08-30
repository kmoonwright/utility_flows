from typing import Iterable
import logging
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utils.redshift as rs

def generate_create_commands() -> Iterable:
    return (
        "DROP TABLE users",
        "DROP TABLE event",
        """
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            user_name VARCHAR(255) NOT NULL,
            timestamp DATE NOT NULL
        )
        """,
        """ 
        CREATE TABLE event (
            event_id INTEGER PRIMARY KEY,
            event_name VARCHAR(255) NOT NULL,
            timestamp DATE NOT NULL
            )
        """,
    )

if __name__ == '__main__':
    client = rs.create_conn("suppliers")
    create_commands = generate_create_commands()
    rs.execute_many_commands(client, create_commands)
    logging.info("WE DONE HERE Y'ALL")
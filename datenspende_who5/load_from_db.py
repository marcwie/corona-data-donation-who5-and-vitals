from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import numpy as np


def connector():
    """
    Establish connection to the ROCS data base.

    Requires that all environment variables are set in .env in the root of this repository.

    Returns:
        connection: The data base connector.
    """
    load_dotenv()
    
    conn = psycopg2.connect(**{
        "database": os.getenv("DBNAME"),
        "user": os.getenv("DBUSER"),
        "port": os.getenv("PORT"),
        "host": os.getenv("HOST"),
        "password": os.getenv("PASSWORD")
        }
    )
    
    return conn


def run_query(query):
    """
    Run an SQL query against the ROCS postgres database.

    Args:
        query (str): the SQL query to execute.

    Returns:
        pandas.DataFrame: The query results.
    """
    conn = connector()
    df = pd.read_sql_query(query, conn)
    conn.close()

    return df


def tuple_of_user_ids(user_ids):
    """
    Converts a given user id or list of user id's to a format that can be
    inserted into IN statements of an SQL query.

    Ensures that the IN-condition for SQL queries either takes the form
    '(userid)' in the case of a single requested user id or '(userid1, userid2,
    ..., useridN)' in the case of multiple requested user ids.

    Args:
        user_ids (int, list, or array): User ids that are inserted into the SQL
        queries.

    Returns:
        tuple or string: Tuple containing all user ids.
    """
    if isinstance(user_ids, int) or isinstance(user_ids, np.int64):
        formatter = f'({user_ids})'
    elif len(user_ids) == 1:
        formatter = f'({user_ids[0]})'
    else:
        formatter = tuple(user_ids)
    
    return formatter
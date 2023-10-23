from dotenv import load_dotenv
import os
import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path

OUTPUT_PATH = Path("data/01a_raw")
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)


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


def load_who5_responses():

    query = """
    SELECT 
        a.user_id, a.created_at, a.question, c.choice_id, q.description
    FROM 
        datenspende.answers a, datenspende.choice c, datenspende.questions q 
    WHERE 
        a.question IN (49, 50, 54, 55, 56) AND 
        a.element = c.element AND
        q.id = a.question
    """

    df = run_query(query)

    return df


def get_vitals(user_ids, min_date="2021-09-01"):
    """
    Get vital data from the data base. 

    Loads data for sleep duration, resting heart rate and step count up to a given date.

    Args:
        user_ids (int or list/array of int): User ids for which to retrieve the vital data.
        min_date (str, optional): The minimum allowed data of vital data. Defaults to "2022-09-01".

    Returns:
        pandas.DataFrame: The vital data.
    """    
    user_ids = tuple_of_user_ids(user_ids)

    query = f"""
    SELECT 
        user_id AS userid, date, type AS vitalid, value, source AS deviceid
    FROM 
        datenspende.vitaldata
    WHERE 
        vitaldata.user_id IN {user_ids}
    AND
        vitaldata.type IN (9, 65, 43, 52, 53)
    AND
        vitaldata.date >= '{min_date}'
    """

    vitals = run_query(query)
    vitals.date = pd.to_datetime(vitals.date)    

    return vitals


def get_users(user_ids):

    user_ids = tuple_of_user_ids(user_ids)

    query = f"""
    SELECT 
        *
    FROM 
        marc.preprocessed_users
    WHERE 
        preprocessed_users.user_id IN {user_ids}
    """

    users = run_query(query)
    
    return users


if __name__ == "__main__":

    print('Downloading survey data from ROCS DB...')
    survey_data = load_who5_responses()
    survey_data.to_feather(OUTPUT_PATH / 'who5_responses.feather')

    print('Downloading vital data from ROCS DB...')
    user_ids = survey_data.user_id.unique()
    vitals = get_vitals(user_ids)
    vitals.to_feather(OUTPUT_PATH / 'vitals.feather')

    print('Downloading user data from ROCS DB...')
    users = get_users(user_ids)
    users.to_feather(OUTPUT_PATH / 'users.feather')

    print('Done!')
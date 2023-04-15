from datenspende_who5 import load_from_db
from pathlib import Path
import pandas as pd

Path("data/01_raw").mkdir(parents=True, exist_ok=True)


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

    df = load_from_db.run_query(query)

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
    user_ids = load_from_db.tuple_of_user_ids(user_ids)

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

    vitals = load_from_db.run_query(query)
    vitals.date = pd.to_datetime(vitals.date)    

    return vitals


if __name__ == "__main__":

    survey_data = load_who5_responses()
    survey_data.to_feather('data/01_raw/who5_responses.feather')

    user_ids = survey_data.user_id.unique()
    vitals = get_vitals(user_ids)
    vitals.to_feather('data/01_raw/vitals.feather')

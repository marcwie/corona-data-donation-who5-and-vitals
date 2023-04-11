from datenspende_who5 import load_from_db
from pathlib import Path

Path("data/01_raw").mkdir(parents=True, exist_ok=True)

def load_who5_responses():

    query = """
    SELECT 
        a.user_id, a.question, c.choice_id, q.description
    FROM 
        datenspende.answers a, datenspende.choice c, datenspende.questions q 
    WHERE 
        a.question IN (49, 50, 54, 55, 56) AND 
        a.element = c.element AND
        q.id = a.question
    """

    df = load_from_db.run_query(query)
    df.to_feather('data/01_raw/who5_responses.feather')

if __name__ == "__main__":

    load_who5_responses()

import pandas as pd
from pathlib import Path
from datetime import date, datetime


Path("data/02_processed").mkdir(parents=True, exist_ok=True)


def preprocess_survey_data(input_file):

    # Load raw data
    df = pd.read_feather(input_file)

    # Create date column
    df['date'] = pd.to_datetime(df.created_at, unit='ms').dt.date
    df.drop(columns='created_at', inplace=True)

    # Drop entries from before the launch of the surveys
    df = df[df.date >= date(2021, 10, 21)]

    # Drop sessions where users did not respons to all five wellbeing questions or answered multiple times
    g = df.groupby(['user_id', 'date']).question.count().reset_index()
    g = g.query('question != 5').drop(columns='question')

    df = pd.merge(df, g, on=['user_id', 'date'], how="outer", indicator=True)
    df = df.query('_merge=="left_only"').drop(columns='_merge')

    # Compute and add average wellbeing
    total = df.groupby(['user_id', 'date']).choice_id.mean().reset_index()
    total['question'] = 60
    total['description'] = 'Mittleres Wohlbefinden'

    df = pd.concat([df, total], ignore_index=True)

    # Clean up the data frame
    df = df[['user_id', 'date', 'question', 'choice_id', 'description']]
    df.sort_values(by=['user_id', 'date'], inplace=True)
    df.reset_index(inplace=True, drop=True)
    
    df.to_feather("data/02_processed/who5_responses.feather")


def preprocess_vitals(input_file):
    
    # Load data
    df = pd.read_feather(input_file)
    df.rename(columns={'userid': 'user_id'}, inplace=True)

    # Compute sleep onset and offset
    mask = df.vitalid.isin([52, 53])
    values = (pd.to_datetime(df[mask].value, unit='s') - df[mask].date) / pd.Timedelta(hours=1)
    df.loc[mask, 'value'] = values
    
    # Remove implausible values for onset and offset
    invalid = (df.vitalid == 52) & (~df.value.between(-12, 12))
    df = df[~invalid]

    invalid = (df.vitalid == 53) & (~df.value.between(0, 18))
    df = df[~invalid]
    
    # Correct for DST
    dst_2021 = df.vitalid.isin([52, 53]) & (df.date < datetime(2021, 10, 31))
    dst_2022 = df.vitalid.isin([52, 53]) & (df.date > datetime(2022, 3, 27)) & (df.date < datetime(2022, 10, 30))

    df.loc[dst_2021 | dst_2022, 'value'] += 1
    
    # Compute 28-day rolling average
    df = df.set_index('date').groupby(['user_id', 'vitalid']).rolling('28D', min_periods=7).mean().dropna()
    df.reset_index(inplace=True)
    
    # Clean up the data frame
    df.date = pd.to_datetime(df.date).dt.date
    
    df.to_feather("data/02_processed/vitals.feather")


def preprocess_users(input_file):

    df = pd.read_feather(input_file)
    df.drop(columns='creation_timestamp', inplace=True)

    df.to_feather("data/02_processed/users.feather")

    
def main():

    preprocess_survey_data('data/01_raw/who5_responses.feather')
    preprocess_vitals('data/01_raw/vitals.feather')
    preprocess_users('data/01_raw/users.feather')

if __name__ == "__main__":
    main()
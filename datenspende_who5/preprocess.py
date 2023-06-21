import pandas as pd
from pathlib import Path
import numpy as np


Path("data/02_processed").mkdir(parents=True, exist_ok=True)
ZIP_TO_NUTS = 'data/00_external/pc2020_DE_NUTS-2021_v3.0.csv'


def add_date_column(df):
    df['date'] = pd.to_datetime(df.created_at // 1000 // 60 // 60 // 24, unit='D')
    
def drop_duplicate_entries(df):
    df.drop_duplicates(subset=['user_id', 'date', 'question', 'choice_id'], inplace=True)
    df.drop_duplicates(subset=['user_id', 'date', 'question'], keep=False, inplace=True)
    
def drop_creation_time_and_description(df):
    df.drop(columns=['created_at', 'description'], inplace=True)

def put_response_to_each_question_as_column(df):
    df = df.set_index(['user_id', 'date', 'question']).unstack()
    return df

def remove_incomplete_responses(df):
    df.dropna(inplace=True)

def simplify_column_names(df):
    df.columns = df.columns.droplevel(0)
    df.columns.names = [None]
    df.rename(columns={49: 'q49', 50: 'q50', 54: 'q54', 55: 'q55', 56: 'q56'}, inplace=True)
    
def set_user_and_date_as_column(df):
    df.reset_index(inplace=True)
    df.rename(columns={'user_id': 'userid'}, inplace=True)
    
def preprocess_survey_data(input_file):
    
    df = pd.read_feather(input_file)

    add_date_column(df)
    drop_duplicate_entries(df)
    drop_creation_time_and_description(df)

    df = put_response_to_each_question_as_column(df)

    remove_incomplete_responses(df)
    simplify_column_names(df)
    set_user_and_date_as_column(df)    
    
    df['total_wellbeing'] = df[['q49', 'q50', 'q54', 'q55', 'q56']].mean(axis=1)
    
    df.to_feather("data/02_processed/surveys.feather")


def preprocess_vital_data(input_file):
    
    df = pd.read_feather(input_file)
    
    # Put vital data as columns
    df = df.set_index(['userid', 'date', 'deviceid', 'vitalid']).unstack()
    df.columns = df.columns.droplevel(0)
    df.columns.names = [None]
    df.columns = [f'v{entry}' for entry in df.columns]
    
    df.reset_index(inplace=True)

    # Compute onset and offset
    df['v52'] = (pd.to_datetime(df['v52'], unit='s') - df['date']) / pd.Timedelta(hours=1)
    df['v53'] = (pd.to_datetime(df['v53'], unit='s') - df['date']) / pd.Timedelta(hours=1)
    
    # Correct for DST
    df.loc[(df.date <= '2021-10-31') | df.date.between('2022-03-28', '2022-10-30'), 'v52'] += 1 
    df.loc[(df.date <= '2021-11-01') | df.date.between('2022-03-28', '2022-10-31'), 'v53'] += 1 
    
    # Remove dates past the end of Datenspende
    df = df[df.date <= '2022-12-31']
    
    # Remove Apple sleep data
    df.loc[df.deviceid == 6, ['v43', 'v52', 'v53']] = np.nan
    
    # Remove outliers
    vmin = df.quantile(.025)
    vmax = df.quantile(.975)

    for vital in ['v65', 'v9', 'v43', 'v52', 'v53']:
        df.loc[~df[vital].between(vmin[vital], vmax[vital]), vital] = np.nan
    
    # Add boolean variable for weekends
    df['weekend'] = df.date.dt.dayofweek >= 5
    
    df.reset_index(drop=True, inplace=True)
    df.to_feather("data/02_processed/vitals.feather")


def preprocess_users(input_file):
    
    df = pd.read_feather(input_file)
    plz = pd.read_csv(ZIP_TO_NUTS, sep=';')

    plz.NUTS3 = plz.NUTS3.str.replace('\'', '')
    plz.CODE = plz.CODE.str.replace('\'', '')

    df = pd.merge(df, plz, left_on='zip_5digit', right_on='CODE', how='left')
    df.drop(columns=['creation_timestamp', 'CODE'], inplace=True)

    df.to_feather("data/02_processed/users.feather")

    
def main():

    preprocess_survey_data('data/01_raw/who5_responses.feather')
    preprocess_vital_data('data/01_raw/vitals.feather')
    preprocess_users('data/01_raw/users.feather')

if __name__ == "__main__":
    main()
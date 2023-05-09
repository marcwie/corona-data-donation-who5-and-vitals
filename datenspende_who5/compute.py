import pandas as pd
import numpy as np
from pathlib import Path
from datenspende_who5 import utils

Path("data/03_derived").mkdir(parents=True, exist_ok=True)

MIN_DAYS = 14
MIN_WEEKDAYS = 10
MIN_WEEKENDS = 4

def rolling_vitals():
    
    # Load vital data
    vitals = pd.read_feather('data/02_processed/vitals.feather')
    vitals.rename(columns={'userid': 'user_id'}, inplace=True)
    vitals.date = pd.to_datetime(vitals.date)

    # Load survey data to get all dates when surveys where answered
    dates = pd.read_feather('data/02_processed/who5_responses.feather')
    dates.drop(columns=['question', 'choice_id', 'description'], inplace=True)
    dates.drop_duplicates(inplace=True)
    dates.date = pd.to_datetime(dates.date)

    # Insert vital ids at each date  
    dates['vitalid'] = 65
    _df = dates.copy()

    for vital in (9, 43, 52, 53):
        dates['vitalid'] = vital
        _df = pd.concat([_df, dates])
        
    # Outer merge of vitals and dates to get the dates at which surveys were taken considered
    data = pd.merge(vitals, _df, how='outer', on=['user_id', 'date', 'vitalid'])

    # Discriminate weekends and weekdays
    data['weekend'] = data.date.dt.dayofweek >= 5

    data.loc[data['weekend'], 'value_weekend'] = data['value'] 
    data.loc[~data['weekend'], 'value_weekday'] = data['value'] 

    # Compute rolling averages of all days, weekends and weekdays
    df = data.set_index('date').sort_index().groupby(['user_id', 'vitalid']).rolling('28D')
    df = df['value', 'value_weekday', 'value_weekend'].agg(['mean', 'count'])

    # Remove means with too few data points
    df.loc[df['value', 'count'] < MIN_DAYS, ('value', 'mean')] = np.nan
    df.loc[df['value_weekend', 'count'] < MIN_WEEKENDS, ('value_weekend', 'mean')] = np.nan
    df.loc[df['value_weekday', 'count'] < MIN_WEEKDAYS, ('value_weekday', 'mean')] = np.nan

    # Clean up the data frame
    df.drop(columns=df.columns[1::2], inplace=True)
    df.columns = df.columns.droplevel(1)
    df.dropna(axis=0, how='all', inplace=True)
    df.reset_index(inplace=True)

    df.to_feather("data/03_derived/rolling_average_vitals.feather")


def create_dataset():
    
    answers = pd.read_feather('data/02_processed/who5_responses.feather')
    vitals = pd.read_feather('data/03_derived/rolling_average_vitals.feather')
    users = pd.read_feather("data/02_processed/users.feather")
    
    df = pd.merge(answers, vitals, on=['user_id', 'date'])
    df = pd.merge(df, users, on='user_id')
    
    utils.bin_data(df)
    utils.remove_implausible(df)

    df.to_feather('data/03_derived/full_data_binned.feather')


if __name__ == "__main__":
    
    rolling_vitals()
    create_dataset()
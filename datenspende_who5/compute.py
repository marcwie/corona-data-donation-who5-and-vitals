import pandas as pd
import numpy as np
from pathlib import Path
from datenspende_who5 import utils

Path("data/03_derived").mkdir(parents=True, exist_ok=True)

MIN_DAYS = 14
MIN_WEEKDAYS = 10
MIN_WEEKENDS = 4

def get_dummy_entries(surveys, vitals):

    # Get all combination of userid and date for which we have survey responses
    entries = surveys[['userid', 'date']]
    
    # Get the list of all device types
    devices = vitals.deviceid.unique()

    # Create a combination of userid and date for each device
    dummy_entries = pd.concat([entries] * len(devices))
    
    # Add each device type to each combination of users and date. 
    dummy_entries['deviceid'] = np.repeat(devices, len(surveys))
    
    return dummy_entries
    
    
def select_subset(vitals, subset):
    
    if subset == 'weekend':
        return vitals[vitals.weekend]
    elif subset == 'weekday':
        return vitals[~vitals.weekend]
        
    return vitals


def expand_vitals(vitals, dummy_entries):
    
    # Add the dummy entries to the list of vital data
    vitals = pd.concat([vitals, dummy_entries]).reset_index(drop=True)
    
    # This ensures that if an entry of the dummy entries already exists in the vital data, that entry is removed.
    vitals.drop_duplicates(subset=['userid', 'date', 'deviceid'], keep='first', inplace=True)

    return vitals
    
    
def compute(surveys, vitals, min_periods=14, subset=''):
    
    dummy_entries = get_dummy_entries(surveys, vitals)
    vitals = select_subset(vitals, subset)
    vitals = expand_vitals(vitals, dummy_entries)
    
    df = vitals.set_index('date').sort_index().groupby(['userid', 'deviceid']).rolling('28D',  min_periods=min_periods)
    df = df['v9', 'v43', 'v65', 'v52', 'v53'].mean()
    
    df.columns = [f'{column}{subset}' for column in df.columns]
    df.reset_index(inplace=True)
    
    df[f'midsleep{subset}'] = 0.5 * (df[f'v53{subset}'] + df[f'v52{subset}'])
    
    return df


def create_dataset():
    
    surveys = pd.read_feather('data/02_processed/surveys.feather')
    vitals = pd.read_feather('data/02_processed/vitals.feather')
    users = pd.read_feather('data/02_processed/users.feather')

    # Compute average vitals for all valid 28-day periods
    df = compute(surveys, vitals, MIN_DAYS)

    # Append average vitals for weekends and weekdays during each 28-day period
    for subset, min_periods in (('weekend', MIN_WEEKENDS), ('weekday', MIN_WEEKDAYS)):
        df_subset = compute(surveys, vitals, min_periods, subset)
        df = pd.merge(df, df_subset, on=['userid', 'deviceid', 'date'])

    # Compute weekend/weekday differences
    for vital in ('v9', 'v65', 'v43', 'v52', 'v53', 'midsleep'):
        df[f'{vital}difference'] = df[f'{vital}weekend'] - df[f'{vital}weekday']
        
    df.rename(columns={'midsleepdifference': 'social_jetlag'}, inplace=True)

    df = pd.merge(users, df, left_on='user_id', right_on='userid')
    df.reset_index(drop=True, inplace=True)

    df.to_feather('data/03_derived/input_data_users_surveys_rolling_vitals.feather')


if __name__ == "__main__":
    
    create_dataset()
from pathlib import Path
import pandas as pd
import numpy as np
import hydra


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

    # This ensures that if an entry of the dummy entries already exists in the vital data, that
    # entry is removed.
    vitals.drop_duplicates(subset=['userid', 'date', 'deviceid'], keep='first', inplace=True)

    return vitals


def compute(surveys, vitals, min_periods, subset):

    print('Compute 28-day rolling average of vitals for subset:', subset)

    print('Create dummy table...')
    dummy_entries = get_dummy_entries(surveys, vitals)

    print('Expand vitals with dummy table...')
    vitals = select_subset(vitals, subset)
    vitals = expand_vitals(vitals, dummy_entries)

    # Set midsleep before computing the rolling averages
    vitals['midsleep'] = 0.5 * (vitals['v53'] + vitals['v52'])

    print('Compute rolling mean and std...')
    df = vitals.set_index('date').sort_index()
    df =df.groupby(['userid', 'deviceid']).rolling('28D',  min_periods=min_periods)
    df = df['v9', 'v43', 'v65', 'v52', 'v53', 'midsleep'].agg(['mean', 'std'])

    df.columns = [f'{column[0]}{column[1]}{subset}'.replace('mean', '') for column in df.columns]
    df.reset_index(inplace=True)

    print('Done!')

    return df


@hydra.main(version_base=None, config_path='../config', config_name='main.yaml')
def main(config):

    input_path = Path(config.data.interim)
    output_path = Path(config.data.processed)
    output_path.mkdir(parents=True, exist_ok=True)

    surveys = pd.read_feather(input_path / config.data.filenames.surveys)
    vitals = pd.read_feather(input_path / config.data.filenames.vitals)
    users = pd.read_feather(input_path / config.data.filenames.users)

    # Compute average vitals for all valid 28-day periods
    df = compute(surveys, vitals, config.process.min_days_for_averaging_vitals, subset='total')

    # Append average vitals for weekends and weekdays during each 28-day period
    settings = (
        ('weekend', config.process.min_weekenddays_for_averaging_vitals),
        ('weekday', config.process.min_weekdays_for_averaging_vitals)
    )

    for subset, min_periods in settings:
        df_subset = compute(surveys, vitals, min_periods, subset)
        df = pd.merge(df, df_subset, on=['userid', 'deviceid', 'date'])

    # Compute weekend/weekday differences
    for vital in ('v9', 'v65', 'v43', 'v52', 'v53', 'midsleep'):
        df[f'{vital}difference'] = df[f'{vital}weekend'] - df[f'{vital}weekday']

    df.rename(columns={'midsleepdifference': 'social_jetlag'}, inplace=True)

    df = pd.merge(surveys, df, on=['userid', 'date'])
    df = pd.merge(users, df, left_on='user_id', right_on='userid')
    df.reset_index(drop=True, inplace=True)

    df.to_feather(output_path / config.data.filenames.merged_data)


if __name__ == "__main__":
    main() # pylint: disable=E1120

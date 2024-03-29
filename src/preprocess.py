"""
Preprocess the raw vital, user and survey data for further analysis.
"""
from pathlib import Path
import pandas as pd
import numpy as np
import hydra
from omegaconf import DictConfig


def add_date_column(df):
    """
    Add a date column (inplace) from a creation time stamp to a given DataFrame

    Args:
        df (pandas.DataFrame): The data frame containing a 'created_at' column.
    """
    df['date'] = pd.to_datetime(
        df.created_at // 1000 // 60 // 60 // 24, unit='D')


def drop_duplicate_entries(df):
    """
    Drop duplicate entries (inplace) from the DataFrame containing the survey data.

    Args:
        df (pandas.DataFrame): The DataFrame containing survey data.
    """
    # First drop duplicate responses when users gave the same answer more than once
    df.drop_duplicates(
        subset=['user_id', 'date', 'question', 'choice_id'], inplace=True)

    # Then drop all cases where the same question was answered more than once
    df.drop_duplicates(
        subset=['user_id', 'date', 'question'], keep=False, inplace=True)


def drop_creation_time_and_description(df):
    """
    Drop the two columns 'created_at' and 'description from a DataFrame (inplace).

    Args:
        df (pandas.DataFrame): The DataFrame to be modified.
    """
    df.drop(columns=['created_at', 'description'], inplace=True)


def put_response_to_each_question_as_column(df):
    """
    Reformat the DataFrame of survey responses so that all five WHO-5 responses form one column
    each in a single row.

    This transforms from a DataFrame with the following structure:

            user_id  question  choice_id       date
    0       1211028        49          4 2021-10-29
    1       1211028        50          3 2021-10-29
    ...         ...       ...        ...        ...

    To one with the following structure:

                       choice_id
    question                  49   50   54   55   56
    user_id date
    239     2021-11-13       3.0  2.0  2.0  1.0  2.0
    250     2021-10-31       4.0  3.0  3.0  3.0  4.0
            2021-12-01       3.0  2.0  4.0  1.0  2.0
    ...                      ...  ...  ...  ...  ...

    Args:
        df (pandas.DataFrame): DataFrame with structure given above.

    Returns:
        pandas.DataFrame: DataFrame with transformed structure.
    """
    df = df.set_index(['user_id', 'date', 'question']).unstack()

    return df


def remove_incomplete_responses(df):
    """
    Drop rows from the survey DataFrame where at least one response is missing (inplace).

    Args:
        df (pandas.DataFrame): The DataFrame containing the surveys responses as rows.
    """
    df.dropna(inplace=True)


def simplify_column_names(df):
    """
    Removes unneccessary levels in column names from the transformed survey DataFrame (inplace).

    Creates a DataFrame with the following structure:

                        q49  q50  q54  q55  q56
    user_id date
    239     2021-11-13  3.0  2.0  2.0  1.0  2.0
    250     2021-10-31  4.0  3.0  3.0  3.0  4.0
            2021-12-01  3.0  2.0  4.0  1.0  2.0
    ...                 ...  ...  ...  ...  ...

    Args:
        df (pandas.DataFrame): The DataFrame of survey responses with one full response
                               (5 questions) per row.
    """
    df.columns = df.columns.droplevel(0)
    df.columns.names = [None]
    df.rename(columns={49: 'q49', 50: 'q50', 54: 'q54', 55: 'q55', 56: 'q56'}, inplace=True)


def set_user_and_date_as_column(df):
    """
    Sets user_id and date as column instead of index.

    Takes the DataFrame created by simplify_column_names() as input.

    Transforms a DataFrame with the following structure:

                        q49  q50  q54  q55  q56
    user_id date
    239     2021-11-13  3.0  2.0  2.0  1.0  2.0
    250     2021-10-31  4.0  3.0  3.0  3.0  4.0
            2021-12-01  3.0  2.0  4.0  1.0  2.0
    ...                 ...  ...  ...  ...  ...

    To one with this structure:

             userid       date  q49  q50  q54  q55  q56
    0           239 2021-11-13  3.0  2.0  2.0  1.0  2.0
    1           250 2021-10-31  4.0  3.0  3.0  3.0  4.0
    ...         ...        ...  ...  ...  ...  ...  ...

    Args:
        df (pandas.DataFrame): Survey responses with user_id and date as index.
    """
    df.reset_index(inplace=True)
    df.rename(columns={'user_id': 'userid'}, inplace=True)


def preprocess_survey_data(input_file, output_file):
    """
    Preprocess the raw survey data.

    Creates a final DataFrame with the following structure:

             userid       date  q49  q50  q54  q55  q56  total_wellbeing
    0           239 2021-11-13  3.0  2.0  2.0  1.0  2.0              2.0
    1           250 2021-10-31  4.0  3.0  3.0  3.0  4.0              3.4
    ...         ...        ...  ...  ...  ...  ...  ...              ...

    The final DataFrame is stored in 'data/02_processed' for further analysis.

    Args:
        input_file (str): Path to the raw survey data. Typically stored in 'data/01_raw'.
        output_file (str): Path to the desired output file. Typically stored in 'data/02_interim'.
    """

    df = pd.read_feather(input_file)

    add_date_column(df)
    drop_duplicate_entries(df)
    drop_creation_time_and_description(df)
    df = put_response_to_each_question_as_column(df)
    remove_incomplete_responses(df)
    simplify_column_names(df)
    set_user_and_date_as_column(df)

    df['total_wellbeing'] = df[['q49', 'q50', 'q54', 'q55', 'q56']].mean(axis=1)

    df.to_feather(output_file)


def preprocess_vital_data(input_file, output_file):
    """
    Preprocess the raw vital data.

    In particular this function takes the following steps:
    - Compute sleep onset and offset in hours relative to midnight
    - Correct for DST by adding one hour to sleep onset/offset when DST is in place
    - Remove invalid data (invalid dates, apple users, outliers)

    Creates a final DataFrame with the following structure:

               userid       date  deviceid       v9  v43  v52  v53   v65  weekend
    0             239 2021-09-01         6   4793.0  NaN  NaN  NaN   NaN    False
    1             239 2021-09-02         6   4594.0  NaN  NaN  NaN   NaN    False
    2             239 2021-09-03         6   4780.0  NaN  NaN  NaN   NaN    False
    3             239 2021-09-04         6  10501.0  NaN  NaN  NaN   NaN     True
    ...           ...        ...       ...      ...  ...  ...  ...   ...      ...

    The final DataFrame is stored in 'data/02_processed' for further analysis.

    Args:
        input_file (str): Path to the raw vital data. Typically stored in 'data/01_raw'.
        output_file (str): Path to the desired output file. Typically stored in 'data/02_interim'.
    """
    df = pd.read_feather(input_file)

    df['date'] = pd.to_datetime(df['date'])

    # Correct sleep timing for correct timezone
    sleep_timing = df.vitalid.isin([52, 53])
    df.loc[sleep_timing, 'value'] += df.loc[sleep_timing, 'timezone_offset'] * 60
    df.drop(columns='timezone_offset', inplace=True)

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
    #df.loc[(df.date <= '2021-10-31') | df.date.between('2022-03-28', '2022-10-30'), 'v52'] += 1
    #df.loc[(df.date <= '2021-11-01') | df.date.between('2022-03-28', '2022-10-31'), 'v53'] += 1

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
    df.to_feather(output_file)


def preprocess_users(input_file, output_file, zip_to_nuts_mapping_file, age_level1, age_level2):
    """
    Add NUTS3 codes to user data and drop unnecessary columns.

    The final DataFrame is stored in 'data/02_processed' for further analysis.

    Args:
        input_file (str): Path to the raw user data. Typically stored in 'data/01_raw'.
        output_file (str): Path to the desired output file. Typically stored in 'data/02_interim'.
        zip_to_nuts_mapping_file (str): Path to a .csv file containing the mapping of zip codes to \
            NUTS3. Typically stored in 'data/00_external'.
    """
    df = pd.read_feather(input_file)

    # Compute age and define age groups
    df['age'] = 2020 - df.birth_date + 2.5
    df.loc[df['age'].between(0, age_level1, inclusive='left'), 'age_group'] = 0
    df.loc[df['age'].between(age_level1, age_level2, inclusive='left'), 'age_group'] = 1
    df.loc[df['age'].between(age_level2, 100, inclusive='left'), 'age_group'] = 2

    # Drop users with survey reponses that are too early
    df = df[~df.user_id.isin([1143114, 1143193, 1144681, 1147298, 1144157, 1155559])]

    # Drop users with unreasonable birth dates
    df = df[~df.birth_date.isin([2004, 1984, 2005])]

    # Drop users with salution 'D' due to the low sample size
    df = df[df.salutation != 'D']

    plz = pd.read_csv(zip_to_nuts_mapping_file, sep=';')

    plz.NUTS3 = plz.NUTS3.str.replace('\'', '')
    plz.CODE = plz.CODE.str.replace('\'', '')

    df = pd.merge(df, plz, left_on='zip_5digit', right_on='CODE', how='left')
    df.drop(columns=['creation_timestamp', 'CODE'], inplace=True)

    df.to_feather(output_file)


@hydra.main(version_base=None, config_path='../config', config_name='main.yaml')
def main(config: DictConfig):
    """
    Preprocess survey, vital and user data for further analysis.
    """
    external_path = Path(config.data.external)
    input_path = Path(config.data.raw)
    output_path = Path(config.data.interim)
    output_path.mkdir(parents=True, exist_ok=True)

    print('Preprocess survey data...')
    preprocess_survey_data(
        input_file=input_path / config.data.filenames.surveys,
        output_file=output_path / config.data.filenames.surveys
    )

    print('Preprocess vital data...')
    preprocess_vital_data(
        input_file=input_path / config.data.filenames.vitals,
        output_file=output_path / config.data.filenames.vitals
    )

    print('Preprocess user data...')
    preprocess_users(
        input_file=input_path / config.data.filenames.users,
        output_file=output_path / config.data.filenames.users,
        zip_to_nuts_mapping_file=external_path / config.data.filenames.zip_to_nuts,
        age_level1=config.process.users.age_level1,
        age_level2=config.process.users.age_level2
    )

    print('Done!')


if __name__ == "__main__":
    main() # pylint: disable=E1120

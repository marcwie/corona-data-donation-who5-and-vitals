import numpy as np


def bin_data(df, intervals={9: 1000, 65: 1, 43: 15, 52: 0.2, 53: 0.2}, 
             columns=('value', 'value_weekend', 'value_weekday')):
    
    for column in columns:
        for vital_id, interval in intervals.items():
            
            mask = df.vitalid == vital_id
            new_column = f'{column}_binned'
            
            df.loc[mask, new_column] = np.round(df.loc[mask, column] / interval) * interval
            
            
def remove_implausible(df, ranges={9: (1000, 20000), 65: (45, 90), 43: (120, 600), 52: (-3, 2), 53: (4.5, 10)}, 
                       columns=('value_binned', 'value_weekend_binned', 'value_weekday_binned')):
    
    for column in columns:
        for vital_id, valid_range in ranges.items():
            
            vmin, vmax = valid_range
            mask = (df.vitalid == vital_id) & ((df[column] > vmax) | (df[column] < vmin))
            df.loc[mask, column] = np.nan


def average(df, value, by):
    
    df = df.groupby(by=by)[value].agg(['mean', 'std', 'count'])
    df['err'] = df['std'] / np.sqrt(df['count'])
    
    return df

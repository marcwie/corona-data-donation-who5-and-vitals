from scipy.stats import pearsonr


def corrcoef(group, question_key, vital_key):

    x = group[vital_key]
    y = group[question_key]

    mask = np.isfinite(x) & np.isfinite(y)
    n = mask.sum()
    x = x[mask]
    y = y[mask]

    if (n < 2) or (len(np.unique(x)) == 1) or (len(np.unique(y)) == 1):
        corr = np.nan
        p_value = np.nan
    else:
        #corr = np.corrcoef(x[mask], y[mask])[0, 1]
        corr, p_value = pearsonr(x, y)

    return corr, p_value, n


def compute_pearson_correlation():

    df = pd.read_feather('data/03_derived/input_data_users_surveys_rolling_vitals.feather')
    g = df.groupby(['userid', 'deviceid'])

    corr = g.size().reset_index().drop(columns=0)

    for question_key in ['q49', 'q50', 'q54', 'q55', 'q56', 'total_wellbeing', ]:
        for vital_key in ['v9', 'v65', 'v43', 'v52', 'v53']:

            print('Computing correlation:', question_key, vital_key)

            _corr = g.apply(corrcoef, question_key, vital_key).reset_index()

            _corr[f'{question_key}_{vital_key}_corr'] = _corr[0].apply(lambda x: x[0])
            _corr[f'{question_key}_{vital_key}_pvalue'] = _corr[0].apply(lambda x: x[1])
            _corr[f'{question_key}_{vital_key}_N'] = _corr[0].apply(lambda x: x[2])
            _corr.drop(columns=0, inplace=True)

            corr = pd.merge(corr, _corr, on=['userid', 'deviceid'])

    corr.reset_index(inplace=True, drop=True)
    corr.to_feather('data/03_derived/correlation_coefficients.feather')



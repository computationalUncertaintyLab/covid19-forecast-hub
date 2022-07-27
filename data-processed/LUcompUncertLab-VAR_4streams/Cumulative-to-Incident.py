#parth,mcandrew

import sys
import numpy as np
import pandas as pd
from sklearn.model_selection import validation_curve
from itertools import chain

if __name__ == "__main__":
    _4streams_df = pd.read_csv('_4streams.csv.gz')
    cumulative_vac_count = _4streams_df[['date','location_name','vac_count']]
    list_values = []
    for x in cumulative_vac_count['location_name'].unique():
        temp1 = cumulative_vac_count[cumulative_vac_count['location_name'] == x]
        vac_count_1 = temp1['vac_count']
        vac_count_1 = vac_count_1.reset_index(drop = True)
        c = np.diff(vac_count_1)
        a = vac_count_1[0]
        d = list(c)
        final_list = list(chain.from_iterable([[a], d]))
        list_values = list_values + final_list

_4streams_df.drop('vac_count', axis = 1, inplace = True)
_4streams_df['vac_count'] = list_values
_4streams_df.to_csv('_4streams.csv.gz')

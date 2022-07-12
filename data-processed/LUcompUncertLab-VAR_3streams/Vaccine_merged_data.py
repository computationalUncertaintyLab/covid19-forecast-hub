 #concating the two scripts
from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":
    
    df1_county = pd.read_csv('fourstreamscounty.csv')
    df2_state = pd.read_csv('fourstreamsstate.csv')
    print(df1_county.nunique())
    print(df1_county.info())
    print(len(df1_county))
    print(df1_county.isna().sum())
    print(df1_county.head())
    print(df2_state.head())
    print(df2_state.nunique())
    print(df2_state.info())
    print(len(df2_state))
    print(df2_state.isna().sum())
    #now we concatenate the two scripts
    fourstreams = pd.concat([df2_state, df1_county], ignore_index=True, sort=False)
    print(fourstreams.nunique())
    print(fourstreams.info())
    print(fourstreams.isna().sum())
    print(len(fourstreams))
    print(fourstreams.head())
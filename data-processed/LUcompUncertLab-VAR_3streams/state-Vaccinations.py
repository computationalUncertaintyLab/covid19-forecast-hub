 #working with the state level data
from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":

    vaccination_df= pd.read_csv('allVaccinationData.csv.gz')
    vaccination_df = vaccination_df.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]]
    vaccination_df['state_fip'] = vaccination_df['fips'].str[:2]
    vaccination_df['date'] = pd.to_datetime(vaccination_df['date'])
    vaccination_df.rename(columns = {'date':'date','Recip_County':'location_name', 'fips':'location','series_complete_yes':'vac_count'}, inplace=True)
    vaccination_df = vaccination_df[(vaccination_df['date'] >= '2020-12-13') & (vaccination_df['date'] <= '2022-04-29')]
    lis1 = ['UN','66',nan]
    vaccination_df_1 = vaccination_df[~vaccination_df['state_fip'].isin(lis1)]
    vaccination_df_1.rename(columns = {'date':'date','recip_county':'location_name', 'FIPS':'location','Series_Complete_Yes':'Vacc_count','recip_state':'state_ab'}, inplace=True)
    vaccination_df_state = vaccination_df_1.groupby(['state_ab','date'], as_index=False)['vac_count'].sum()
    #so we have the vaccination count for each state on daily basis
    #now we will be working with the locations
    location_df = pd.read_csv('locations.csv') 
    location_df_state = location_df[location_df['abbreviation'].notna()]
    location_df_state['state_fip'] = location_df_state['location'].str[:2]
    location_df_state = location_df_state[location_df_state['location_name']!='US']
    merge1 = pd.merge(vaccination_df_state, location_df_state, how='left', left_on=['state_ab'], right_on=['abbreviation'])
    merge1 = merge1.loc[:,['state_ab','date','vac_count','location_name','population','state_fip']]
    print(merge1.head())
    #now we need the threestreams 
    threestreams_s = pd.read_csv('threestreams__state.csv.gz')
    threestreams_s['date'] = pd.to_datetime(threestreams_s['date'])
    threestreams_s = threestreams_s[(threestreams_s['date'] >= '2020-12-13') & (threestreams_s['date'] <= '2022-04-29')]
    print(threestreams_s)
    fourstreams_s = pd.merge(threestreams_s, merge1, how = 'left', left_on=['date','location','location_name'], right_on=['date','state_fip','location_name'])
    #we dont have values for American Samoa and United states(i.e. it is on a national level)
    #for american Samoa we replace the value by 1 and the est of the values were googled
    fourstreams_s['vac_count'] = np.where((fourstreams_s['location_name'] == 'American Samoa'),1,fourstreams_s['vac_count'])
    fourstreams_s['state_ab'] = np.where((fourstreams_s['location_name'] == 'American Samoa'),'AS',fourstreams_s['state_ab'])
    fourstreams_s['state_fip'] = np.where((fourstreams_s['location_name'] == 'American Samoa'),60,fourstreams_s['state_fip'])
    fourstreams_s['population'] = np.where((fourstreams_s['location_name'] == 'American Samoa'),55197.0,fourstreams_s['population'])   
    #we need to work on everyday vac count at a daily scale
    vaccination_df_country = vaccination_df_1.groupby('date', as_index=False)['vac_count'].sum()
    fourstreams_s['population'] = np.where((fourstreams_s['location_name'] == 'United States'),332915073.0,fourstreams_s['population']) 
    fourstreams_s['state_ab'] = np.where((fourstreams_s['location_name'] == 'United States'),'US',fourstreams_s['state_ab'])
    fourstreams_s['state_fip'] = np.where((fourstreams_s['location_name'] == 'United States'),00,fourstreams_s['state_fip'])
    #merging with the national daily vaccination count
    fourstreams_s = pd.merge(fourstreams_s, vaccination_df_country, how='left', left_on=['date'], right_on=['date'])
    fourstreams_s.rename(columns = {'vac_count_x':'vac_count','vac_count_y':'country_vac_count'}, inplace=True)
    fourstreams_s['vac_count'] = np.where((fourstreams_s['location_name'] == 'United States'),fourstreams_s['country_vac_count'],fourstreams_s['vac_count'])
    fourstreams_s.drop(columns=['country_vac_count'], inplace=True)
    print(fourstreams_s.head())
    print(fourstreams_s.nunique())
    print(fourstreams_s.info())
    print(len(fourstreams_s))
    print(fourstreams_s.isna().sum())
    #so our fourstream_s data is now ready to be added and it has no nan values
    fourstreams_s.drop("population", axis = 1, inplace  = True)
    print(fourstreams_s.head())
    print(fourstreams_s.nunique())
    print(fourstreams_s.info())
    print(len(fourstreams_s))
    print(fourstreams_s.isna().sum())
    #we export it as a csv
    fourstreams_s.to_csv('fourstreamsstate.csv', index=False)


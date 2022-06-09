from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import column

if __name__ == "__main__":

    threestreamsdf__county = pd.read_csv('threestreams__county.csv.gz') 
    threestreamsdf__state  = pd.read_csv('threestreams__state.csv.gz')

    threestreamsdf = threestreamsdf__state.append(threestreamsdf__county)
    
    threestreamsdf1 = threestreamsdf
    threestreamsdf1['location'] = threestreamsdf1['location'].astype(str)
    threestreamsdf1['state_fip'] = threestreamsdf1['location'].str[:2]
    threestreamsdf1['date'] = pd.to_datetime(threestreamsdf1['date'])
    threestreamsdf1 = threestreamsdf1[(threestreamsdf1['date'] >= '2022-02-13') & (threestreamsdf1['date'] <= '2022-04-29')]
    
    threestreamsdf3 = threestreamsdf1.groupby(['state_fip','date']).sum()

    vaccination_df= pd.read_csv('allVaccinationData.csv.gz')
    vaccination_df = vaccination_df.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]]
    vaccination_df = vaccination_df[vaccination_df['fips']!= 'UNK']
    vaccination_df['fips'] = vaccination_df['fips'].astype(int)

    vaccination_df['state_fip'] = [str(x)[:2] for x in vaccination_df['fips']]
    vaccination_df['date'] = pd.to_datetime(vaccination_df['date'])
    
    #We will have to change the names of the columns in vaccination_df_5 so that we have a single column
    vaccination_df.rename(columns = {'date':'date','Recip_County':'location_name', 'fips':'location','series_complete_yes':'vac_count'}, inplace=True)
    vaccination_df = vaccination_df[(vaccination_df['date'] >= '2022-02-13') & (vaccination_df['date'] <= '2022-04-29')]

    # Note here. The below FIPS are likely not contained in the threestreams dataset.
    # They will be removed upong merging the vaccine data with ThreeStreams (parth,tom)
    #lis1 = ['UN','72','66',nan]
    #vaccination_df = vaccination_df[~vaccination_df['state_fip'].isin(lis1)]

    #importing the location csv
    location_df =pd.read_csv('../../data-locations/locations.csv')  

    location_df_state  = location_df[location_df['abbreviation'].notna()]
    location_df_county = location_df[location_df['abbreviation'].isna()]
    location_df_state = location_df_state[location_df_state['location']!= 'US']
    
    
    location_df_county['location'] = location_df_county['location'].astype(int)
    location_df_state['location'] = location_df_state['location'].astype(int)
    location_df_county['state_fip'] = [str(x)[:2] for x in location_df_county['location']]
    location_df_state['state_fip'] = [str(x)[:2] for x in location_df_state['location']]

    location_final = pd.merge(location_df_county, location_df_state, how  = 'left', left_on=['state_fip'], right_on=['state_fip'])
    location_final.rename(columns = {'location_x':'location','location_name_x':'location_name','abbreviation_y':'state_ab','location_name_y':'state','population_y':'state_pop','population_x':'county_pop'}, inplace  = True)

    location_final.drop(['abbreviation_x','location_y'], inplace = True, axis = 1)
    location_final['per_state_pop'] = location_final['county_pop']/location_final['state_pop']

    # HARD CODE FOR VI.
    # The Virgin Islands only has a single county and so we are hardocding in a row to location_final (parth,tom).
    # VI_row = location_df.loc[location_df.location=="78"]
    
    temp1 = location_final[['state_ab','state_pop']]
    print(temp1.groupby('state_pop')['state_ab'].unique())

    vaccination_df_merge = vaccination_df
    vaccination_df_merge['date'] = pd.to_datetime(vaccination_df_merge['date'])
    vaccination_df_merge.rename(columns = {'date':'date','recip_county':'location_name', 'FIPS':'location','Series_Complete_Yes':'Vacc_count','recip_state':'state_ab'}, inplace=True)
    vaccination_df_merge = vaccination_df_merge[(vaccination_df_merge['date'] >= '2022-02-13') & (vaccination_df_merge['date'] <= '2022-04-29')]

    vaccination_df_merge = vaccination_df_merge.merge( location_final, on = ["location", "location_name", "state_ab","state_fip"] )

    #print(vaccination_df_merge[vaccination_df_merge.isna().any(axis=1)])
    #print(vaccination_df_merge[vaccination_df_merge['vac_count'].isna()])

    vaccination_df_state = vaccination_df_merge.groupby(['state_ab','date'], as_index=False)['vac_count'].sum()
    
    vaccination_data_and_census_data_at_state_level = pd.merge(vaccination_df_state, location_final, how = 'left',left_on=['state_ab'], right_on=['state_ab'])
    vaccination_data_and_census_data_at_state_level = vaccination_data_and_census_data_at_state_level.rename(columns = {'vac_count':'state_vac_count'})

    vaccination_data_and_census_data_at_state_level = vaccination_data_and_census_data_at_state_level[ ["date","location","state_vac_count"] ]

    vaccination_date_at_state_and_county = vaccination_df_merge.merge( vaccination_data_and_census_data_at_state_level, on  = ["date","location"] )

    merge_4 = vaccination_date_at_state_and_county
    
    merge_4['cal_vac'] = merge_4['state_vac_count']*(merge_4['county_pop']/merge_4['state_pop'])
    merge_4['vac_count'].fillna(merge_4['cal_vac'], inplace = True)

    #after this we will merge with the threestreams data
    threestreamsdf_c = pd.read_csv('threestreams__county.csv.gz')
    threestreamsdf_c['location'] = threestreamsdf_c['location'].astype(str)
    threestreamsdf_c['state_fip'] = threestreamsdf_c['location'].str[:2]
    threestreamsdf_c['date'] = pd.to_datetime(threestreamsdf_c['date'])
    threestreamsdf_c = threestreamsdf_c[(threestreamsdf_c['date'] >= '2022-02-13') & (threestreamsdf_c['date'] <= '2022-04-29')]


    #now we merge
    threestreamsdf_c['location'] = threestreamsdf_c['location'].astype(int)
    fourstreams = pd.merge(threestreamsdf_c, merge_4, how = 'left',left_on=['date','location','state_fip'], right_on=['date','location','state_fip'])
    
    #print(fourstreams.head())
    #print(fourstreams.info())
    #print(fourstreams.nunique())
    #print(len(fourstreams))
    #print(fourstreams.isna().sum())
    #checking for the state fips
    #print(threestreamsdf_c['state_fip'].unique())
    #print(merge_4['state_fip'].unique())
    #there are no fips in merge_4 for fips 60, 61,80, 81 and 90 in threestreams
    #maybe we can check for nan values after removing them
    lis1 = ['60','61','80','81','90']
    fourstreams = fourstreams[~fourstreams['state_fip'].isin(lis1)]
    print(fourstreams.head())
    print(fourstreams.info())
    print(fourstreams.nunique())
    print(len(fourstreams))
    print(fourstreams.isna().sum())

    #please note that we still have a lot of nan values that needs to be populated and inspite of
    #taking on 2021 values we are still getting 40k Nan Values 
    #Now we remove all the unnecassary set of columns
    #trying to work on the Naan Values
    #post chaning the date to 2021

    #
    fourstreams = fourstreams.loc[:,["date","location","location_name_x","county_cases","state_deaths","state_hosps","state_ab","vac_count","state_fip"]]
    fourstreams.rename(columns = {'county_cases':'cases','state_deaths':'deaths','state_hosps':'hosps'}, inplace  = True)
    print(fourstreams.head())
    print(fourstreams.info())
    print(fourstreams.nunique())
    print(len(fourstreams))
    print(fourstreams.isna().sum())
    #store the fourstreams as a csv file
    fourstreams.to_csv('fourstreamscounty.csv', index=False)
    #

from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":

    threestreamsdf = pd.read_csv('threestreams__county.csv.gz')
    
    threestreamsdf1 = threestreamsdf
    threestreamsdf1['location']   = threestreamsdf1['location'].astype(str)
    threestreamsdf1['state_fip']  = threestreamsdf1['location'].str[:2]
    threestreamsdf1['date']       = pd.to_datetime(threestreamsdf1['date'])
    threestreamsdf1 = threestreamsdf1[(threestreamsdf1['date'] >= '2020-12-13') & (threestreamsdf1['date'] <= '2022-04-29')]
    
    threestreamsdf3 = threestreamsdf1.groupby(['state_fip','date']).sum()

    vaccination_df= pd.read_csv('allVaccinationData.csv')
    vaccination_df = vaccination_df.loc[:, [ "date","fips","recip_county", "recip_state", "series_complete_yes"]]

    vaccination_df['state_fip'] = vaccination_df['fips'].str[:2]
    vaccination_df_5 = vaccination_df[vaccination_df['date'] == '04/29/2022']
    vaccination_df_6 = vaccination_df_5.groupby('state_fip').sum()
    
    #we dont know what to do with the state fip 60 and 61 in threestreams dataframe and what to do with state fip 66 and UN in the vaccination dataframe
    #merged_df = pd.merge(threestreamsdf3, vaccination_df_6)
    #vaccination_df_2 = vaccination_df.groupby('state_fip').sum()
    #print(vaccination_df_2)
    #merging the vaccination dataset of a single latest updated date with the threestreams dataset so that we have only the latest updated count of
    #vaccinations done
    vaccination_df_5['Date'] = pd.to_datetime(vaccination_df_5['Date'])
    #We will have to change the names of the columns in vaccination_df_5 so that we have a single column
    vaccination_df_5.rename(columns = {'Date':'date','Recip_County':'location_name', 'FIPS':'location'}, inplace=True)
    #importing the location csv
    location_df = pd.read_csv('locations.csv') 
    #why are there duplicate values in location_name
    #assuming the same name is because of the fact that there are many counties that have the same name
    #we will instead try to merge the vaccination and the location csv
    #merged_df_2 = pd.merge(vaccination_df_5, location_df, how = 'left', left_on=['location','location_name'], right_on=['location','location_name'])
    #merged_df_2.drop(['abbreviation','Recip_State'], inplace = True, axis = 1)
    #we have now merged the location csv file and the vaccinatin dataframe. Our next step is to merge it with the three streams file
    #merged_df_final = pd.merge(threestreamsdf1, merged_df_2, how = 'left', left_on = ['date','location_name','location','state_fip'], right_on=['date','location_name','location','state_fip'])
    #print(merged_df_final.loc[merged_df_final['date'] == '2022-04-29',:])
    #just consider the locaion dataframe, we will split it into two types
    print(location_df.info())
    location_df_state = location_df[location_df['abbreviation'].notna()]
    location_df_county = location_df[location_df['abbreviation'].isna()]
    location_df_state['state_fip'] = location_df_state['location'].str[:2]
    location_df_county['state_fip'] = location_df_county['location'].str[:2]
    location_final = pd.merge(location_df_county, location_df_state, how  = 'left', left_on=['state_fip'], right_on=['state_fip'])
    location_final.rename(columns = {'location_x':'location','location_name_x':'location_name','abbreviation_y':'state_ab','location_name_y':'state','population_y':'state_pop','population_x':'county_pop'}, inplace  = True)
    location_final.drop(['abbreviation_x','location_y'], inplace = True, axis = 1)
    location_final['per_state_pop'] = location_final['county_pop']/location_final['state_pop']   
    vaccination_df_merge = vaccination_df
    vaccination_df_merge['Date'] = pd.to_datetime(vaccination_df_merge['Date'])
    vaccination_df_merge.rename(columns = {'Date':'date','Recip_County':'location_name', 'FIPS':'location','Series_Complete_Yes':'Vacc_count','Recip_State':'state_ab'}, inplace=True)
    vaccination_df_merge = vaccination_df_merge[(vaccination_df_merge['date'] >= '2020-12-13') & (vaccination_df_merge['date'] <= '2022-04-29')]
    print(vaccination_df_merge.head())
    print(location_final.head())
    #now we will merge all the three dataframes
    merge_1 = pd.merge(vaccination_df_merge, location_final, how = 'left', left_on=['location','location_name','state_ab','state_fip'], right_on=['location','location_name','state_ab','state_fip'])
    merge_df_final = pd.merge(threestreamsdf1, merge_1, how = 'left', left_on = ['date','location','location_name','state_fip'], right_on=['date','location','location_name','state_fip'])
    #now fix ot for the state as well
    

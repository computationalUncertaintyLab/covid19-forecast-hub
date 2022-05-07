from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":
    threestreamsdf = pd.read_csv('s1695RizU6ih.csv')
    threestreamsdf1 = threestreamsdf
    threestreamsdf1['location'] = threestreamsdf1['location'].astype(str)
    threestreamsdf1['state_fip'] = threestreamsdf1['location'].str[:2]
    threestreamsdf1['date'] = pd.to_datetime(threestreamsdf1['date'])
    threestreamsdf1 = threestreamsdf1[(threestreamsdf1['date'] >= '2020-12-13') & (threestreamsdf1['date'] <= '2022-04-29')]
    threestreamsdf3 = threestreamsdf1.groupby('state_fip').sum()
    print(threestreamsdf3)
    vaccination_df= pd.read_csv('COVID-19_Vaccinations_in_the_United_States_County.csv')
    vaccination_df.drop(['Series_Complete_Pop_Pct','Completeness_pct','MMWR_week','Administered_Dose1_Recip_5Plus','Administered_Dose1_Recip_5PlusPop_Pct','Administered_Dose1_Recip_12Plus','Administered_Dose1_Recip_12PlusPop_Pct','Administered_Dose1_Recip_18Plus','Administered_Dose1_Recip_18PlusPop_Pct','Administered_Dose1_Recip_65Plus','Administered_Dose1_Recip_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_SVI','Series_Complete_5to17Pop_Pct_SVI','Series_Complete_12PlusPop_Pct_SVI','Series_Complete_18PlusPop_Pct_SVI','Series_Complete_65PlusPop_Pct_SVI','Booster_Doses_12Plus','Booster_Doses_12Plus_Vax_Pct','Booster_Doses_18Plus','Booster_Doses_18Plus_Vax_Pct','Booster_Doses_50Plus','Booster_Doses_50Plus_Vax_Pct','Booster_Doses_65Plus','Booster_Doses_65Plus_Vax_Pct','Series_Complete_5Plus','Series_Complete_5PlusPop_Pct','Series_Complete_5to17','Series_Complete_5to17Pop_Pct','Series_Complete_12Plus','Series_Complete_12PlusPop_Pct','Series_Complete_18Plus','Series_Complete_18PlusPop_Pct','Series_Complete_65Plus','Series_Complete_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_UR_Equity','Series_Complete_5to17Pop_Pct_UR_Equity','Series_Complete_12PlusPop_Pct_UR_Equity','Series_Complete_18PlusPop_Pct_UR_Equity','Series_Complete_65PlusPop_Pct_UR_Equity','Booster_Doses_12PlusVax_Pct_SVI','Booster_Doses_18PlusVax_Pct_SVI','Booster_Doses_65PlusVax_Pct_SVI','Booster_Doses_12PlusVax_Pct_UR_Equity','Booster_Doses_18PlusVax_Pct_UR_Equity','Booster_Doses_65PlusVax_Pct_UR_Equity','Census2019_5PlusPop','Census2019_5to17Pop','Census2019_12PlusPop','Census2019_18PlusPop','Census2019_65PlusPop','Metro_status','Census2019','Booster_Doses_Vax_Pct_UR_Equity','Booster_Doses_Vax_Pct_SVI','Series_Complete_Pop_Pct_UR_Equity','Series_Complete_Pop_Pct_SVI','SVI_CTGY','Administered_Dose1_Recip','Administered_Dose1_Pop_Pct','Booster_Doses','Booster_Doses_Vax_Pct'], inplace=True, axis = 1)
    vaccination_df['state_fip'] = vaccination_df['FIPS'].str[:2]
    vaccination_df_5 = vaccination_df[vaccination_df['Date'] == '04/29/2022']
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
    

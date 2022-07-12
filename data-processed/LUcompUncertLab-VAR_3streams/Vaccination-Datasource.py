from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":

    vacdf = pd.read_csv("COVID-19_Vaccinations_in_the_United_States_County.csv")
    print(vacdf.info())
    #We need to get range of dates and hence convert to datetime
    vacdf['Date'] = pd.to_datetime(vacdf['Date'])
    print(vacdf['Date'].min())
    print(vacdf['Date'].max())
    #the range of date is from 2020-12-13 to 2022-04-13
    vacdf.drop(['Administered_Dose1_Recip_5Plus','Administered_Dose1_Recip_5PlusPop_Pct','Administered_Dose1_Recip_12Plus','Administered_Dose1_Recip_12PlusPop_Pct','Administered_Dose1_Recip_18Plus','Administered_Dose1_Recip_18PlusPop_Pct','Administered_Dose1_Recip_65Plus','Administered_Dose1_Recip_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_SVI','Series_Complete_5to17Pop_Pct_SVI','Series_Complete_12PlusPop_Pct_SVI','Series_Complete_18PlusPop_Pct_SVI','Series_Complete_65PlusPop_Pct_SVI','Booster_Doses_12Plus','Booster_Doses_12Plus_Vax_Pct','Booster_Doses_18Plus','Booster_Doses_18Plus_Vax_Pct','Booster_Doses_50Plus','Booster_Doses_50Plus_Vax_Pct','Booster_Doses_65Plus','Booster_Doses_65Plus_Vax_Pct','Series_Complete_5Plus','Series_Complete_5PlusPop_Pct','Series_Complete_5to17','Series_Complete_5to17Pop_Pct','Series_Complete_12Plus','Series_Complete_12PlusPop_Pct','Series_Complete_18Plus','Series_Complete_18PlusPop_Pct','Series_Complete_65Plus','Series_Complete_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_UR_Equity','Series_Complete_5to17Pop_Pct_UR_Equity','Series_Complete_12PlusPop_Pct_UR_Equity','Series_Complete_18PlusPop_Pct_UR_Equity','Series_Complete_65PlusPop_Pct_UR_Equity','Booster_Doses_12PlusVax_Pct_SVI','Booster_Doses_18PlusVax_Pct_SVI','Booster_Doses_65PlusVax_Pct_SVI','Booster_Doses_12PlusVax_Pct_UR_Equity','Booster_Doses_18PlusVax_Pct_UR_Equity','Booster_Doses_65PlusVax_Pct_UR_Equity','Census2019_5PlusPop','Census2019_5to17Pop','Census2019_12PlusPop','Census2019_18PlusPop','Census2019_65PlusPop','Metro_status','Census2019','Booster_Doses_Vax_Pct_UR_Equity','Booster_Doses_Vax_Pct_SVI','Series_Complete_Pop_Pct_UR_Equity','Series_Complete_Pop_Pct_SVI','SVI_CTGY','Administered_Dose1_Recip','Administered_Dose1_Pop_Pct','Booster_Doses','Booster_Doses_Vax_Pct'], inplace=True, axis = 1)
    threestreamsdf = pd.read_csv('threestreams__county.csv')
    print(threestreamsdf.head())
    print(threestreamsdf.info())
    #We need to get an idea about the range of dates of this dataframe
    threestreamsdf['date'] = pd.to_datetime(threestreamsdf['date'])
    print(threestreamsdf['date'].min())
    print(threestreamsdf['date'].max())
    ####METHOD 
    #Just trying on a merge without the for loop since the for loop will run on it
    #1.First we need to cut short the dataframe of vaccination from 2022-03-12
    #2.Next we need to make sure that the dates in the vaccination dataframe are in the same format as the threestreams dataframe
    #3.Next we need to cut short the the three streams dataframe so that it will run from 2020-12-13
    #4.Then merge
    print(vacdf.info())
    #There are a total of 1598297 rows on the dataframe 
    vacdf_merge = vacdf
    vacdf_merge = vacdf_merge[(vacdf_merge['Date'] >= '2020-12-13')&(vacdf_merge['Date'] <= '2022-03-12')]
    print(vacdf_merge['Date'].min())
    print(vacdf_merge['Date'].max())
    #step 2
    print(threestreamsdf.info())
    #convert the Data to date and location from object to int in the vacdf_merged
    vacdf_merge.rename(columns = {'Date':'date', 'FIPS':'location'}, inplace=True)
    pd.options.mode.chained_assignment = None
    print(vacdf_merge[vacdf_merge['location'] == 'UNK']) 
    #fix this and check if it is weekly dates or daily in threestreams
    #Since the location is unknown for this, we eliminate all rows that location UNK
    vacdf_merge = vacdf_merge[(vacdf_merge['location'] != 'UNK')]
    vacdf_merge['location'] = vacdf_merge['location'].astype(int)
    #Now the datatype is int
    #We note the final count of rows of vacdf_merge dates column
    print(len(vacdf_merge))
    print(vacdf_merge['date'].nunique())
    #note the count of threestreamsdf
    print(len(threestreamsdf))
    print(threestreamsdf['date'].nunique())
    #clearly we can see that their are more rows in threestreamsdf than vacdf_merge and the difference is 1916962-1466920 = 450042
    #Also the number of unique values in dates column are 712 for threestreams whereas they are only 455 for mergedf
    #Now we change date format of vacdf_merge and make it like threestreams
    print(threestreamsdf.info())
    print(threestreamsdf['date'].head())
    #we have to make sure that the date measured from in the dataframe is the same
    threestreamsdf = threestreamsdf[(threestreamsdf['date'] >= '2020-12-13')&(threestreamsdf['date'] <= '2022-03-12')]
    #now we merge
    merged_df = pd.merge(threestreamsdf, vacdf_merge, how = 'left', left_on=['date','location'], right_on=['date','location'])
    merged_df.drop(['Recip_State','Series_Complete_Pop_Pct','MMWR_week','Completeness_pct'], inplace = True, axis = 1)
    print(merged_df['Series_Complete_Yes'].isna().sum())
    merged_df.rename(columns = {'Series_Complete_Yes':'Vaccination_count'}, inplace  = True)
    print(merged_df['Vaccination_count'].isna().sum())
    print(vacdf_merge['date'].min())
    print(vacdf_merge['date'].max())
    print(threestreamsdf['date'].min())
    print(threestreamsdf['date'].max())
    print(merged_df['date'].min())
    print(merged_df['date'].max())
    print(merged_df['Vaccination_count'].isna().sum())
    #We have 4550 rows of data that have nan values
    #we have to get to know the values of nan for each of the dates i.e. are they the same values
    merged_df_1 = merged_df[merged_df['date'] == '2021-03-12']
    merged_df_2 = merged_df[merged_df['date'] == '2022-03-12']
    merged_df_3 = merged_df[merged_df['date'] == '2021-06-12']
    print(merged_df_1['Vaccination_count'].isna().sum())
    print(merged_df_2['Vaccination_count'].isna().sum())
    print(merged_df_3['Vaccination_count'].isna().sum())
    merged_df_1_nan = merged_df_1[merged_df_1['Vaccination_count'].isna()]
    merged_df_2_nan = merged_df_2[merged_df_2['Vaccination_count'].isna()]
    merged_df_3_nan = merged_df_3[merged_df_3['Vaccination_count'].isna()]
    print(merged_df_1_nan.head(10))
    print(merged_df_2_nan.head(10))
    print(merged_df_3_nan.head(10))
    list_to_drop = ['Chugach County','Copper River County','Hawaii County','Honolulu County','Kalawao County','Kauai County','Maui County','Barnstable County','Dukes County','Nantucket County']
    merged_df_final = merged_df[~merged_df.location_name.isin(list_to_drop)]
    print(len(merged_df_final))
    print(merged_df_final['Vaccination_count'].isna().sum())

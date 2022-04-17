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
    #checking the unique value counts of the fips column for each of the 4 dates
    vacdf1 = vacdf[vacdf['Date'] == '04/13/2022']
    vacdf2 = vacdf[vacdf['Date'] == '12/26/2021']
    vacdf3 = vacdf[vacdf['Date'] == '09/22/2021']
    vacdf4 = vacdf[vacdf['Date'] == '12/27/2020']
    print(vacdf1.nunique())
    print(vacdf2.nunique())
    print(vacdf3.nunique())
    
    #Checking the lenght of the overall dataframe as well as of the newly formed dataframe vacdf1
    #which works for a single date
    print(len(vacdf1))
    print(len(vacdf2))
    print(len(vacdf3))
    print(len(vacdf4))
    print(vacdf1.head())
    #From above we have the len to be equal to 3284 for a single date however,the unique values for fips is only 3225. This means the rest are duplciates
    #we will delete this duplicates
    #Our focus is to first merge on a single fips location
    print(vacdf1.FIPS.duplicated().sum())
    print(vacdf2.FIPS.duplicated().sum())
    print(vacdf3.FIPS.duplicated().sum())
    print(vacdf4.FIPS.duplicated().sum())
    #We have found out the number of duplicates in each of the dataframe and now we will delete it
    vacdf1 = vacdf1.drop_duplicates(subset = 'FIPS', keep= False)
    print(vacdf1.FIPS.duplicated().sum())
    print(vacdf1.nunique())
    print(len(vacdf1))
    #clearly now we have the length of the dataframe equal to the unique values in a column
    #The total number of fips is 3224
    #Now we will check the total number of fips of the threestreams dataset for the a single date and find out the difference
    threestreamsdf = pd.read_csv('threestreams__county.csv')
    print(threestreamsdf.head())
    print(threestreamsdf.info())
    #We need to get an idea about the range of dates of this dataframe
    threestreamsdf['date'] = pd.to_datetime(threestreamsdf['date'])
    print(threestreamsdf['date'].min())
    print(threestreamsdf['date'].max())
    #So the range of dates is 2020-03-25 to 2022-03-12
    #Our dataframe will have to be between 2020-12-13 to 2022-03-12
    #Now we will have to get the unique location count of the threestreams dataframe for a single date
    threestreamsdf1 = threestreamsdf[threestreamsdf['date'] == '2021-12-13']
    print(len(threestreamsdf1['location']))
    print(threestreamsdf1.location.duplicated().sum())
    #there are a total of 3143 values in the location columns i.e. the fips. So there is a difference of 3225-3143 = 82
    #values between the data source and threestreams dataframe which we have
    #Our goal is to have a merged dataframe and we will merge on the values of threestreams dataframe and all the other values not there
    #will be discarded and if not present for that location than we will take it as na
    #Now we will choose a randomly selected date of 2021-12-13 to merge the dataframe
    vacdf5 = vacdf[vacdf['Date'] == '12/13/2021']
    vacdf5 = vacdf5.drop_duplicates(subset = 'FIPS', keep= False)
    vacdf5['FIPS'] = vacdf5['FIPS'].astype(str).astype(int)
    print(vacdf5.head())
    mergedf = pd.merge(threestreamsdf1, vacdf5, left_on='location', right_on='FIPS', how = 'left')
    print(mergedf.head())
    print(mergedf.info())
    print(mergedf.nunique())
    print(mergedf.shape)
    print(mergedf['FIPS'].isnull().sum())
    mergedf['unmatchedzip']= np.where(mergedf['location'] == mergedf['FIPS'], mergedf['location'], np.nan)
    print(mergedf.head())
    print(mergedf['unmatchedzip'].isnull().sum())
    #from our analysis we have obtained that there are only 2 unmatched zip between the location and the fips 
    #therefore we will get rid of the unmatched zip
    mergedf = mergedf.drop_duplicates(subset = 'unmatchedzip', keep= False)
    print(mergedf.shape)
    print(mergedf.nunique())
    #Now we will have to repeat this excercise for all the dates 
    #for loop:

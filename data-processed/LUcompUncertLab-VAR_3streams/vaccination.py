from cmath import nan
from heapq import merge
import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":

    threestreamsdf = pd.read_csv('threestreams.csv.gz')
    
    threestreamsdf1 = threestreamsdf
    threestreamsdf1['location'] = threestreamsdf1['location'].astype(str)
    threestreamsdf1['state_fip'] = threestreamsdf1['location'].str[:2]
    threestreamsdf1['date'] = pd.to_datetime(threestreamsdf1['date'])
    threestreamsdf1 = threestreamsdf1[(threestreamsdf1['date'] >= '2020-12-13') & (threestreamsdf1['date'] <= '2022-04-29')]
    
    threestreamsdf3 = threestreamsdf1.groupby(['state_fip','date']).sum()

    vaccination_df= pd.read_csv('allVaccinationData.csv.gz')
    vaccination_df = vaccination_df.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]]
    vaccination_df['state_fip'] = vaccination_df['fips'].str[:2]
    vaccination_df['date'] = pd.to_datetime(vaccination_df['date'])
    #We will have to change the names of the columns in vaccination_df_5 so that we have a single column
    vaccination_df.rename(columns = {'date':'date','Recip_County':'location_name', 'fips':'location','series_complete_yes':'vac_count'}, inplace=True)
    vaccination_df = vaccination_df[(vaccination_df['date'] >= '2020-12-13') & (vaccination_df['date'] <= '2022-04-29')]
    lis1 = ['UN','72','66',nan]
    vaccination_df = vaccination_df[~vaccination_df['state_fip'].isin(lis1)]
    #importing the location csv
    location_df = pd.read_csv('locations.csv') 
    location_df_state = location_df[location_df['abbreviation'].notna()]
    location_df_county = location_df[location_df['abbreviation'].isna()]
    location_df_state['state_fip'] = location_df_state['location'].str[:2]
    location_df_county['state_fip'] = location_df_county['location'].str[:2]
    location_final = pd.merge(location_df_county, location_df_state, how  = 'left', left_on=['state_fip'], right_on=['state_fip'])
    location_final.rename(columns = {'location_x':'location','location_name_x':'location_name','abbreviation_y':'state_ab','location_name_y':'state','population_y':'state_pop','population_x':'county_pop'}, inplace  = True)
    location_final.drop(['abbreviation_x','location_y'], inplace = True, axis = 1)
    location_final['per_state_pop'] = location_final['county_pop']/location_final['state_pop']
    temp1 = location_final[['state_ab','state_pop']]
    print(temp1.groupby('state_pop')['state_ab'].unique())
    vaccination_df_merge = vaccination_df
    vaccination_df_merge['date'] = pd.to_datetime(vaccination_df_merge['date'])
    vaccination_df_merge.rename(columns = {'date':'date','recip_county':'location_name', 'FIPS':'location','Series_Complete_Yes':'Vacc_count','recip_state':'state_ab'}, inplace=True)
    vaccination_df_merge = vaccination_df_merge[(vaccination_df_merge['date'] >= '2020-12-13') & (vaccination_df_merge['date'] <= '2022-04-29')]
    
    #creating a new function
    def  f(row):
        if row['state_ab'] == 'WY':
            val = 578759.0
        elif row['state_ab'] == 'VT':
            val = 623989.0
        elif row['state_ab'] == 'DC':
            val = 705749.0
        elif row['state_ab'] == 'AK':
            val = 731545.0
        elif row['state_ab'] == 'ND':
            val = 762062.0
        elif row['state_ab'] == 'SD':
            val = 884659.0
        elif row['state_ab'] == 'DE':
            val = 973764.0
        elif row['state_ab'] == 'RI':
            val = 1059361.0
        elif row['state_ab'] == 'MT':
            val = 1068778.0
        elif row['state_ab'] == 'ME':
            val = 1344212.0
        elif row['state_ab'] == 'NH':
            val = 1359711.0
        elif row['state_ab'] == 'HI':
            val = 1415872.0
        elif row['state_ab'] == 'ID':
            val = 1787065.0
        elif row['state_ab'] == 'WV':
            val = 1792147.0
        elif row['state_ab'] == 'NE':
            val = 1934408.0
        elif row['state_ab'] == 'NM':
            val = 2096829.0
        elif row['state_ab'] == 'KS':
            val = 2913314.0
        elif row['state_ab'] == 'MS':
            val = 2976149.0
        elif row['state_ab'] == 'AR':
            val = 3017804.0
        elif row['state_ab'] == 'NV':
            val = 3080156.0
        elif row['state_ab'] == 'IA':
            val = 3155070.0
        elif row['state_ab'] == 'UT':
            val = 3205958.0
        elif row['state_ab'] == 'CT':
            val = 3565287.0
        elif row['state_ab'] == 'OK':
            val = 3956971.0
        elif row['state_ab'] == 'OR':
            val = 4217737.0
        elif row['state_ab'] == 'KY':
            val = 4467673.0
        elif row['state_ab'] == 'LA':
            val = 4648794.0
        elif row['state_ab'] == 'AL':
            val = 4903185.0
        elif row['state_ab'] == 'SC':
            val = 5148714.0
        elif row['state_ab'] == 'MN':
            val = 5639632.0
        elif row['state_ab'] == 'CO':
            val = 5758736.0
        elif row['state_ab'] == 'WI':
            val = 5822434.0
        elif row['state_ab'] == 'MD':
            val = 6045680.0
        elif row['state_ab'] == 'MO':
            val = 6626371.0
        elif row['state_ab'] == 'IN':
            val = 6732219.0
        elif row['state_ab'] == 'TN':
            val = 6829174.0
        elif row['state_ab'] == 'MA':
            val = 6892503.0
        elif row['state_ab'] == 'AZ':
            val = 7278717.0
        elif row['state_ab'] == 'WA':
            val = 7614893.0
        elif row['state_ab'] == 'VA':
            val = 8535519.0
        elif row['state_ab'] == 'NJ':
            val = 8882190.0
        elif row['state_ab'] == 'MI':
            val = 9986857.0
        elif row['state_ab'] == 'NC':
            val = 10488084.0
        elif row['state_ab'] == 'GA':
            val = 10617423.0
        elif row['state_ab'] == 'OH':
            val = 11689100.0
        elif row['state_ab'] == 'IL':
            val = 12671821.0
        elif row['state_ab'] == 'PA':
            val = 12801989.0
        elif row['state_ab'] == 'NY':
            val = 19453561.0
        elif row['state_ab'] == 'FL':
            val = 21477737.0
        elif row['state_ab'] == 'TX':
            val = 28995881.0
        elif row['state_ab'] == 'CA':
            val = 39512223.0
        else:
            val = 0
        return val
    vaccination_df_merge['state_pop'] = vaccination_df_merge.apply(lambda row:f(row), axis = 1)

    print(vaccination_df_merge[vaccination_df_merge.isna().any(axis=1)])
    print(vaccination_df_merge[vaccination_df_merge['vac_count'].isna()])
    vaccination_df_state = vaccination_df_merge.groupby(['state_ab','date'], as_index=False)['vac_count'].sum()
    print(vaccination_df_state)
    #merging
    merge_3 = pd.merge(vaccination_df_state, location_final, how = 'left',left_on=['state_ab'], right_on=['state_ab'])
    print(merge_3.head())
    print(merge_3.nunique())
    print(merge_3.isna().sum())
    #we get 503 Nan values cause there is no VI in the location file
    #at this moment we will be deleting the VI values based on the assumption that the location.csv was used to build our model and therefore 
    #the model will not have forecatss for VI i.e. virgin island
    merge_3 = merge_3[merge_3.state_ab != 'VI']
    print(merge_3.head())
    print(merge_3.isna().sum())

    #now we dont have any nan values
    #next step is to merge with the vaccination datasource i.e. vaccination_df_merge
    merge_3.rename(columns = {'vac_count':'state_vac_count'}, inplace=True)
    #next step is to investigate the output
    merge_4 = pd.merge(vaccination_df_merge,merge_3, how = 'right', left_on =['location','location_name','state_ab','state_pop','date','state_fip'], right_on=['location','location_name','state_ab','state_pop','date','state_fip'])
    print(merge_3['state_ab'].unique())
    merge_4 = merge_4[merge_4.state_ab != 'VI']
    print(vaccination_df_merge['state_ab'].unique())
    merge_4['cal_vac'] = merge_4['state_vac_count']*(merge_4['county_pop']/merge_4['state_pop'])
    merge_4['vac_count'].fillna(merge_4['cal_vac'], inplace = True)
    #there were a total of 74342 values that dont have a vaccination count out of Nan i.e. 4.7% of the values
    #Also, what we have done is a right merge because we need to make sure that all the counties in the location-file have
    # a vaccination count i.e. again based on assumption that we will base our model on the location.csv file
    # Now we will have to merge on the threestreams dataset
  

    #after this we will merge with the threestreams data
    threestreamsdf_c = pd.read_csv('threestreams__county 2.csv')
    threestreamsdf_c['location'] = threestreamsdf_c['location'].astype(str)
    threestreamsdf_c['state_fip'] = threestreamsdf_c['location'].str[:2]
    threestreamsdf_c['date'] = pd.to_datetime(threestreamsdf_c['date'])
    threestreamsdf_c = threestreamsdf_c[(threestreamsdf_c['date'] >= '2020-12-13') & (threestreamsdf_c['date'] <= '2022-04-29')]
    #now we merge
    fourstreams = pd.merge(threestreamsdf_c, merge_4, how = 'left',left_on=['date','location','location_name','state_fip'], right_on=['date','location','location_name','state_fip'])
    print(fourstreams.head())
    print(fourstreams.info())
    print(fourstreams.nunique())
    print(len(fourstreams))
    print(fourstreams.isna().sum())
    #checking for the state fips
    print(threestreamsdf_c['state_fip'].unique())
    print(merge_4['state_fip'].unique())
    #there are no fips in merge_4 for fips 60, 61,80, 81 and 90 in threestreams
    #maybe we can check for nan values after removing them
    lis1 = ['60','61','80','81','90']
    fourstreams = fourstreams[~fourstreams['state_fip'].isin(lis1)]
    print(fourstreams.head())
    print(fourstreams.info())
    print(fourstreams.nunique())
    print(len(fourstreams))
    print(fourstreams.isna().sum())

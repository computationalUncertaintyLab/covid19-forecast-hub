#berlin

import numpy as np
import pandas as pd

from datetime import date, datetime, timedelta

import covidcast

def addInCounties(dic,d,state):
    val = state["value"].mean()
    for location,location_name in zip(["US","60","66","69","72","78"]
                                      ,["United States","American Samoa","Guam","Northern Mariana Islands","Puerto Rico","Virgin Islands"]):

        dic["date"].append(d)
        dic["location"].append(location)
        dic["location_name"].append(location_name)
        dic["value"].append(val)
    return dic
    
if __name__ == "__main__":

    #--import previous data
    google = pd.DataFrame()
    try:
        print("Importing Past Data")
        google = pd.read_csv("./AllGoogleData.csv")
        #--set start date to last date in data plus 1 day
        start_date = datetime.strptime(max(google["date"]), '%Y-%m-%d').date() + timedelta(days=1)
    except:
        print("No past data found")
        start_date = date(2020,2,22)

    #--find date of most recent data (profm:needed?)

    #--CHOICE HERE
    start_date = date(2022,5,22)
    
    #--get county data
    print("Collecting County Data")
    county = covidcast.signal(data_source = "google-symptoms"
                              ,signal     = "s01_raw_search"
                              ,geo_type   = "county"
                              ,start_day  = start_date)

    #--configure df
    county_data = county[ ["time_value","geo_value","value"]  ]
    county_data = county_data.rename(columns={"geo_value":"location", "time_value":"date"})
    county_data["location"]      = county_data["location"].astype(int).astype(str)
    county_data["location_name"] = covidcast.fips_to_name(county_data["location"])

    #--get state data
    print("Collecting State Data")
    state = covidcast.signal(data_source = "google-symptoms"
                             ,signal     = "s01_raw_search"
                             ,geo_type   = "state"
                             ,start_day  = start_date)

    #--configure df
    state_data = pd.DataFrame()
    state_data["date"]          = state["time_value"]
    state_data["location"]      = [x[0:2] for x in covidcast.abbr_to_fips(state["geo_value"], ignore_case=True)]
    state_data["location_name"] = covidcast.abbr_to_name(state["geo_value"], ignore_case=True)
    state_data["value"]         = state["value"]

    data = pd.concat([county_data, state_data], ignore_index=True)

    #--build full list of locations and dates
    cases = pd.read_csv("../../data-truth/truth-Incident Cases.csv")
    loc_and_date = cases.loc[cases.location_name.str.contains("County"),["date","location","location_name"]]

    loc_and_date["date"] = pd.to_datetime(loc_and_date["date"])
    loc_and_date["location"] = loc_and_date.location.astype(str)
    
    loc_and_date = loc_and_date.merge( data, on = ["date","location","location_name"], how="left" )

    #-- merge in state level google values
    temp_state_data = state_data.rename(columns={"location":"state_location", "location_name":"state_location_name","value":"state_value"})
    loc_and_date["state_location"] = loc_and_date.location.str.slice(0,2)
    
    loc_and_date = loc_and_date.merge( temp_state_data, on=["date","state_location"] )

    ##THIS IS WHERE I LEFT OFF
    #--replace missing values with state level values
    loc_and_date["value"] = loc_and_date.apply( lambda x: x.state_value if np.isnan(x.value) else x.value, 1) 
    
    # agg and save
    if google.empty:
        data = pd.concat([data, pd.DataFrame.from_dict(dic)], ignore_index=True)
    else:
        data = pd.concat([google, data, pd.DataFrame.from_dict(dic)], ignore_index=True)

    data.to_csv("AllGoogleData.csv", index=False)

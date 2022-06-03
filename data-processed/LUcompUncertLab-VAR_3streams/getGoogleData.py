#berlin

# TODO: Add the 5 random missing islands
# 1. 60,American Samoa
# 2. 66,Guam
# 3. 69,Northern Mariana Islands (nice)
# 4. 72,Puerto Rico
# 5. 78,Virgin Islands

import numpy as np
import pandas as pd

from datetime import date, datetime, timedelta

import covidcast


if __name__ == "__main__":

    # import previous data
    google = pd.DataFrame()
    try:
        print("Importing Past Data")
        google = pd.read_csv("./AllGoogleData.csv")
        start_date = datetime.strptime(max(google["date"]), '%Y-%m-%d').date() + timedelta(days=1)

    except:
        print("No past data found")
        start_date = date(2020,1,22)

    # find date of most recent data

    # get county data
    print("Getting County Data")
    county = covidcast.signal(data_source="google-symptoms",signal="s01_raw_search", geo_type="county", start_day = start_date)

    # configure df
    county_data = pd.DataFrame()
    county_data["date"] = county["time_value"]
    county_data["location"] = county["geo_value"].astype(int).astype(str)
    county_data["location_name"] = covidcast.fips_to_name(county_data["location"])
    county_data["value"] = county["value"]

    # get state data
    print("Getting State Data")
    state = covidcast.signal(data_source="google-symptoms",signal="s01_raw_search", geo_type="state", start_day = start_date)

    # configure df
    state_data = pd.DataFrame()
    state_data["date"] = state["time_value"]
    state_data["location"] = [x[0:2] for x in covidcast.abbr_to_fips(state["geo_value"], ignore_case=True)]
    state_data["location_name"] = covidcast.abbr_to_name(state["geo_value"], ignore_case=True)
    state_data["value"] = state["value"]

    data = pd.concat([county_data, state_data], ignore_index=True)

    # import FIPS reference 
    cases = pd.read_csv("../../data-truth/truth-Incident Cases.csv")
    cases["location"] = cases["location"].astype(str)

    # select counties only x_x
    mask = (cases["location"].str.len() > 2) & (cases["location"] != "US")
    cases = cases.loc[mask]
    locations = pd.Index(cases["location"].unique())

    # dict where data is going to go
    dic = {"date":[], "location":[], "location_name":[], "value":[]}

    # for every date in the df
    for d in data["date"].unique():
        # loc to date
        sub_google = data.loc[data["date"] == d]

        # select all states
        mask = (sub_google["location"].str.len() <= 2) & (sub_google["location"] != "US")
        state = sub_google.loc[mask]
        state["location"] = state["location"].astype(int)
        
        # Add US and 5 missing randos
        # sorry for the gross code :(

        val = state["value"].mean()

        dic["date"].append(date)
        dic["location"].append("US")
        dic["location_name"].append("US")
        dic["value"].append(val)

        dic["date"].append(date)
        dic["location"].append("60")
        dic["location_name"].append("American Samoa")
        dic["value"].append(val)

        dic["date"].append(date)
        dic["location"].append("66")
        dic["location_name"].append("Guam")
        dic["value"].append(val)

        dic["date"].append(date)
        dic["location"].append("69")
        dic["location_name"].append("Northern Mariana Islands")
        dic["value"].append(val)

        dic["date"].append(date)
        dic["location"].append("72")
        dic["location_name"].append("Puerto Rico")
        dic["value"].append(val)

        dic["date"].append(date)
        dic["location"].append("78")
        dic["location_name"].append("Virgin Islands")
        dic["value"].append(val)

        # set up list of locations that need to be filled
        google_locations = pd.Index(sub_google["location"].unique())
        diff = locations.difference(google_locations).tolist()
        
        # for every location append the state level number
        for l in diff:
            value = state.loc[state["location"] == int(int(l)/1000)].iat[0,3]
            dic["date"].append(d)
            dic["location"].append(str(l))
            dic["location_name"].append(covidcast.fips_to_name(l)[0])
            dic["value"].append(value)
        break

    # agg and save
    if google.empty:
        data = pd.concat([data, pd.DataFrame.from_dict(dic)], ignore_index=True)
    else:
        data = pd.concat([google, data, pd.DataFrame.from_dict(dic)], ignore_index=True)

    data.to_csv("AllGoogleData.csv", index=False)
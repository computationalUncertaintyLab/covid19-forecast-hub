#berlin

from dataclasses import dataclass
import numpy as np
import pandas as pd

from datetime import date, datetime, timedelta

import covidcast


if __name__ == "__main__":


    google = pd.read_csv("./AllGoogleData.csv")

    start_date = datetime.strptime(max(google["date"]), '%Y-%m-%d').date() + timedelta(days=1)

    county = covidcast.signal(data_source="google-symptoms",signal="s01_raw_search", geo_type="county", start_day = start_date)

    data = pd.DataFrame()
    data["date"] = county["time_value"]
    data["location"] = county["geo_value"].astype(int).astype(str)
    data["location_name"] = covidcast.fips_to_name(data["location"])
    data["value"] = county["value"]

    state = covidcast.signal(data_source="google-symptoms",signal="s01_raw_search", geo_type="state", start_day = start_date)

    data2 = pd.DataFrame()
    data2["date"] = state["time_value"]
    data2["location"] = [x[0:2] for x in covidcast.abbr_to_fips(state["geo_value"], ignore_case=True)]
    data2["location_name"] = covidcast.abbr_to_name(state["geo_value"], ignore_case=True)
    data2["value"] = state["value"]

    google = pd.concat([google, data, data2], ignore_index=True)

    cases = pd.read_csv("../../data-truth/truth-Incident Cases.csv")
    cases_locations = pd.Index(cases["location"].unique())

    dic = {"date":[], "location":[], "location_name":[], "value":[]}
    for d in google["date"].unique():
        # loc to date
        sub_google = google.loc[google["date"] == d]

        # aggregate all states
        mask = (sub_google["location"].str.len() <= 2) & (sub_google["location"] != "US")
        state = sub_google.loc[mask]
        state["location"] = state["location"].astype(int)
        
        # add to df
        dic["date"].append(date)
        dic["location"].append("US")
        dic["location_name"].append("US")
        dic["value"].append(state["value"].mean())


        google_locations = pd.Index(sub_google["location"].unique())
        diff = cases_locations.difference(google_locations).tolist()
        
        
        for l in diff:
            value = state.loc[state["location"] == int(l/1000)].iat[0,3]
            dic["date"].append(d)
            dic["location"].append(str(l))
            dic["location_name"].append(covidcast.fips_to_name(str(l)))
            dic["value"].append(value)

    data = pd.concat([google, pd.DataFrame.from_dict(dic)], ignore_index=True)

    data.to_csv("AllGoogleData.csv", index=False)








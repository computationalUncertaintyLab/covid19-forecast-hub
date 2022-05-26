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

    data = pd.concat([google, data, data2], ignore_index=True)

    data.to_csv("AllGoogleData.csv", index=False)








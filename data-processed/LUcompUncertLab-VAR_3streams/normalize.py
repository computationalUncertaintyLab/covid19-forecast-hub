#berlin

import pandas as pd
import numpy as np

from interface import interface

def norm( x ):
    mean = x.mean()
    std = x.std()
    x = (x - mean) / std
    return x


if __name__ == "__main__":
    io = interface(0, location=1)
    county_data = io.county_data
    state_data = io.data

    locations_c = county_data.location.unique()
    locations_s = state_data.location.unique()

    # normalize data for each location
    for location in locations_c:
        sub_data = county_data[county_data.location == location]
        sub_data.county_cases = norm(sub_data.county_cases)
        sub_data.state_deaths = norm(sub_data.state_deaths)
        sub_data.state_hosps = norm(sub_data.state_hosps)
        sub_data.county_googles = norm(sub_data.county_googles)

        county_data[county_data.location == location] = sub_data

    for location in locations_s:
        sub_data = state_data[state_data.location == location]
        sub_data.cases = norm(sub_data.cases)
        sub_data.deaths = norm(sub_data.deaths)
        sub_data.hosps = norm(sub_data.hosps)
        sub_data.googles = norm(sub_data.googles)
        state_data[state_data.location == location] = sub_data


    # save data
    county_data.to_csv("fourstreams__county.csv.gz", compression = "gzip")
    state_data.to_csv("fourstreams__state.csv.gz", compression = "gzip")





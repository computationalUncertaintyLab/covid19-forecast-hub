#mcandrew
# takes in from __allpredictions from location specified from CL argument
# sets lower bound at 0
# casts target_end_date to datetime object, and formats it to YYYY-MM-DD

import sys
import numpy as np
import pandas as pd

import datetime
import re


import argparse

from interface import interface

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--LOCATION')

    args = parser.parse_args()

    LOCATION = args.LOCATION
    
    io = interface(0,LOCATION)
    forecast = io.grab_recent_all_predictions()
    forecast['value'] = round(forecast['value'].clip(lower=0), 1)

    forecast["target_end_date"] = pd.to_datetime(forecast.target_end_date)
    forecast["target_end_date"] = forecast.target_end_date.dt.strftime("%Y-%m-%d")

    # HERE WE WILL CHANGE SUNDAY FORECAST DATE TO MONDAY
    forecast["forecast_date"] = pd.to_datetime(io.forecast_date) + datetime.timedelta(days=1) # THIS ASSUMES WE RUN ON SUNDAY

    # WE WILL CHANGE 2-29 DAY AHEAD to 1-28 DAY AHEAD and remove the 1 day ahead (monday)

    old_targets = forecast["target"]
    renamed_targets = []

    for target in old_targets:

        #remove word covid from all targets
        target = target.replace("covid ","")

        if "day" in target:
            day = int(re.findall("\d+",target)[0])
            txt = " ".join( re.findall("[a-z]+",target) )

            dayMinusOne = day - 1
            newtarget = "{:d} {:s}".format(dayMinusOne,txt) 

            renamed_targets.append(newtarget)
        else:
            renamed_targets.append(target)
 
            
    forecast["target"] = renamed_targets
    forecast = forecast.loc[ ~forecast.target.str.contains("^0 day") ] # REMOVE SUNDAY "DAY" FORECASTS
    
    day = io.getForecastDate()
    forecast.to_csv("./location_specific_forecasts/{:s}_LUcompUncertLab-VAR4Streams__{:s}.csv.gz".format(day,io.fmtlocation)
                              ,header=True
                              ,index=False
                              ,mode="w"
                              ,compression="gzip")

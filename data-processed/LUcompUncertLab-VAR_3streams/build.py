#mcandrew
# this uses the VAR model created in model.py to create sample forecasts
# for a specific location indicated by the location argument passed in

from interface import interface
from model import VAR
import argparse
import pandas as pd

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--LOCATION')

    args = parser.parse_args()

    LOCATION = args.LOCATION

    io = interface(0,LOCATION)
    io.getClosestDay(6) # Reference will always be from the closest Sunday ( day 6).
        
    io.subset2location()


    forecast_model = VAR(io.modeldata)
    forecast_model.fit()

    io.formatSamples(forecast_model)
    #io.un_center() # multipy by std and add back running mean

    io.fromSamples2Quantiles()

    io.writeout()


#mcandrew
# this aggregates daily cases and deaths into incident weekly cases
# they are then stored in /location_specific_forecasts/[forecast_date]_LUcompUncertLab-VAR__[FIPS]__weeklypredictions.csv.gz

from interface import interface
from model import VAR
import pandas as pd


import argparse

def fromDaily2Weeks():
    from epiweeks import Week
    import pandas as pd

    fromDaily2Weekly = {"target_end_date":[],"week":[]}
    thisWeek = Week.thisweek()
    for week in [1,2,3,4]:
        for day in thisWeek.iterdates():
            fromDaily2Weekly["target_end_date"].append(day.strftime("%Y-%m-%d"))
            fromDaily2Weekly["week"].append( int(week) )
        thisWeek+=1
    return pd.DataFrame(fromDaily2Weekly)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('--LOCATION')

    args = parser.parse_args()

    LOCATION = args.LOCATION
    
    io = interface(data=None,location = LOCATION)
    predictions = io.grab_recent_predictions()

    predictions["day"]  = predictions.target.str.extract("(\d+).*").astype("int")
    predictions["hosp"] = predictions.target.str.match(".*hosp$")
    predictions["target_no_time"] = predictions.target.str.extract(".* day ahead (.*)")

    fromDaily2Weekly = fromDaily2Weeks()
    predictions = predictions.merge(fromDaily2Weekly, on = ["target_end_date"], how="left")

    hospitilizations     = predictions[predictions.hosp==True]
    not_hospitilizations = predictions[predictions.hosp==False]
    
    def addUp(x):
        import pandas as pd

        vals = [ max(v,0) for v in x["value"]]
        summedPrediction = sum(vals)

        maxEndDate = max(pd.to_datetime(x.target_end_date)) 
        
        return pd.Series({"target_end_date": maxEndDate, "value":summedPrediction})
    groupedPredictions = not_hospitilizations.groupby(["forecast_date","location","target_no_time","sample","week"]).apply(addUp)
    groupedPredictions = groupedPredictions.reset_index()

    groupedPredictions["week_ahead"] = " wk ahead "
    groupedPredictions["target"] = groupedPredictions["week"].astype(int).astype(str) + groupedPredictions["week_ahead"] + groupedPredictions["target_no_time"] 

    groupedPredictions = groupedPredictions[["forecast_date","target_end_date","location","target","sample","value"]]
    hospitilizations   = hospitilizations[["forecast_date","target_end_date","location","target","sample","value"]]
    dailyAndWeeklyPredictions = groupedPredictions.append(hospitilizations)

    day = io.getForecastDate()
    dailyAndWeeklyPredictions.to_csv("./location_specific_forecasts/{:s}_LUcompUncertLab-VAR__{:s}__weeklypredictions.csv.gz".format(day,io.fmtlocation)
                              ,header=True
                              ,index=False
                              ,mode="w"
                              ,compression="gzip")

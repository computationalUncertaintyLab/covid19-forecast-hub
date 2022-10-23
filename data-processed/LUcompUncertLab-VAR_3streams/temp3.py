#mcandrew
# accumulates deaths for location passed as CL argument
# appends to previous __weeklypredictions file
# sends to [forecast_date]_LUcompUncertLab-VAR__[FIPS]__allpredictions.csv.gz

from interface import interface
from model import VAR
import pandas as pd

import argparse

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--LOCATION')

    args = parser.parse_args()

    LOCATION = args.LOCATION
 
    io = interface(data=None,location = LOCATION)
    predictions = io.grab_recent_weekly_predictions()

    deathTargets = predictions.loc[ predictions.target.str.contains("death"),:]
    deathTargets["week"]  = deathTargets.target.str.extract("(\d+) wk.*").astype("int")
    def cumulative(d):
        import numpy as np
        d = d.sort_values("week")
        cumulative_values = list(np.cumsum(d["value"]))
        weeks            = d.week.values
        target_end_dates = d.target_end_date.values 
        return pd.DataFrame({"week":weeks, "target_end_date":target_end_dates,"value": cumulative_values})
    deathTargets =  deathTargets.groupby(["forecast_date","location","sample"]).apply(cumulative)

    deathTargets = deathTargets.reset_index()
    deathTargets = deathTargets[["forecast_date","target_end_date","location","sample","value", "week"]]

    deathTargets["target"] = deathTargets["week"].astype(str) + " wk ahead cum death"
    deathTargets = deathTargets[["forecast_date","target_end_date","target","location","sample","value"]]
    print(deathTargets)
    cumu_cases_df = pd.read_csv("../../data-truth/truth-Cumulative Cases.csv")
    cumu_cases_df['date'] = pd.to_datetime(cumu_cases_df['date'])
    latest_cum_value = cumu_cases_df[cumu_cases_df['date'] == cumu_cases_df['date'].max()]
    list_location = latest_cum_value['location'].unique().tolist()
    list_values = latest_cum_value['value'].unique().tolist()
    #trying to merge and then solve
    deathTargets_fixed = deathTargets.merge(latest_cum_value, how = 'left', left_on= 'location', right_on='location')
    deathTargets_fixed['value'] = deathTargets_fixed['value_x'] + deathTargets_fixed['value_y']
    deathTargets_fixed = deathTargets_fixed[["forecast_date","target_end_date","target","location","sample","value"]]
    print(deathTargets_fixed)
    predictions = predictions.append(deathTargets_fixed)
    day = io.getForecastDate()
    predictions.to_csv("./location_specific_forecasts/{:s}_LUcompUncertLab-VAR__{:s}__allpredictions.csv.gz".format(day,io.fmtlocation)
                              ,header=True
                              ,index=False
                              ,mode="w"
                              ,compression="gzip")


    

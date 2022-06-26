# piriya; mcandrew
# iterates through all files with format [forecast_date]_LUcompUncertLab-VAR4Streams_FINAL_[FIPS].csv.gz
# and compiles it into one csv file

from interface import interface
from glob import glob
import pandas as pd

import datetime

if __name__ == "__main__":

    io = interface(0)
    io.getClosestDay(6)  # Reference will always be from the closest Sunday ( day 6).

    monday = (pd.to_datetime(io.forecast_date) + datetime.timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    almost_final_filename = "{:s}-LUcompUncertLab-VAR_4streams__ALMOSTFINAL.csv".format(
        monday
    )

    for n, file in enumerate(
        glob(
            "./location_specific_forecasts/{:s}_LUcompUncertLab-VAR4Streams_FINAL*".format(
                monday
            )
        )
    ):
        curr = pd.read_csv(file, compression="gzip")
        if n == 0:
            curr.to_csv(almost_final_filename, index=False)
        else:
            curr.to_csv(almost_final_filename, index=False, mode="a", header=False)

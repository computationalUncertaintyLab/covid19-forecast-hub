#piriya
# takes in the compiled predictions from [forecast_date]-LUcompUncertLab-VAR_3streams__ALMOSTFINAL.csv
# changes location values to either 2 or 5 character strings ('01' instead of '1' and '01001' instead of '1001')
# normalizes output file --> takes out covid whereever it appears, and replaces week with wk to acommodate to submission
# takes out all quantiles not in [0.025, 0.100, 0.250, 0.500, 0.750, 0.900, 0.975] for week ahead incident cases
# takes out extraneous targets --> the ones that are allowed are: 
# 				STATE: wk ahead cum death, wk ahead inc death, day ahead inc hosp
#				STATE AND COUNTY: wk ahead inc case
# sends to final file [forecast_date]-LUcompUncertLab-VAR_3streams.csv, which SHOULD be submittable

from interface import interface
import pandas as pd

if __name__ == "__main__":
	io = interface(0)
	final_file = "{:s}-LUcompUncertLab-VAR_3streams.csv".format(io.forecast_date)
	file = "{:s}-LUcompUncertLab-VAR_3streams__ALMOSTFINAL.csv".format(io.forecast_date)
	# read in csv
	allPredictions = pd.read_csv(file)
	
	# normalize location values
	newloc = []
	for x in allPredictions['location']:
		if(len(str(x)) > 2):
			newloc.append(str(x).zfill(5))
		else:
			newloc.append(str(x).zfill(2))

	s = pd.Series(newloc)
	allPredictions['location'] = s
	
	# normalize target values
	allPredictions['target'] = allPredictions['target'].str.replace("covid ", "", regex=False)
	allPredictions['target'] = allPredictions['target'].str.replace("week", "wk", regex=False)

	# take out extraneous targets
	toSubmit = {"target":[], "location":[], "quantile": []}
	wk_inc_case_quantiles = [0.025, 0.100, 0.250, 0.500, 0.750, 0.900, 0.975]

	for row in allPredictions.itertuples(index=False):
		if (len(str(row.location)) < 3 and ("wk ahead cum death" in row.target or "wk ahead inc death" in row.target or "day ahead inc hosp" in row.target)) or ("wk ahead inc case" in row.target and row.quantile in wk_inc_case_quantiles):
			toSubmit["target"].append(row.target)
			toSubmit["location"].append(row.location)
			toSubmit["quantile"].append(row.quantile)

	toSubmit = pd.DataFrame(toSubmit)
	toSubmit = toSubmit.merge(allPredictions, on=["target", "location", "quantile"], how="left")

	toSubmit.to_csv(final_file, index=False)

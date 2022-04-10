from interface import interface
import pandas as pd

if __name__ == "__main__":
	io = interface(0)
	final_file = "{:s}-LUcompUncertLab-VAR_3streams.csv".format(io.forecast_date)
	file = "{:s}-LUcompUncertLab-VAR_3streams__ALMOSTFINAL.csv".format(io.forecast_date)
	allPredictions = pd.read_csv(file)
	allPredictions['target'] = allPredictions['target'].str.replace("covid ", "", regex=False)
	allPredictions['target'] = allPredictions['target'].str.replace("week", "wk", regex=False)

	toSubmit = {"target":[], "location":[], "quantile": []}

	for row in allPredictions.itertuples(index=False):
		# print(row["target"], ("wk ahead cum death" in row["target"] or "wk ahead inc death" in row.target or "day ahead inc hosp" in row.target), ("wk ahead inc case" in row.target))
		if (len(str(row.location)) < 3 and ("wk ahead cum death" in row.target or "wk ahead inc death" in row.target or "day ahead inc hosp" in row.target)) or "wk ahead inc case" in row.target:
			toSubmit["target"].append(row.target)
			toSubmit["location"].append(row.location)
			toSubmit["quantile"].append(row.quantile)

	toSubmit = pd.DataFrame(toSubmit)
	toSubmit = toSubmit.merge(allPredictions, on=["target", "location", "quantile"], how="left")

	toSubmit.to_csv(final_file, index=False)

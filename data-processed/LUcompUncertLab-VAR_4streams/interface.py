#mcandrew

class interface(object):
    def __init__(self,data=None, location=None):
        import pandas as pd
        
        if data is None:
            pass
        else:
            self.data          = pd.read_csv("fourstreams__state.csv.gz") 
            self.county_data   = pd.read_csv("fourstreams__county.csv.gz")
            self.locations     = sorted(self.data.location.unique())

            self.buildDataForModel()
            self.getForecastDate()
            self.generateTargetEndDays()
            self.generateTargetNames()

            self.numOfForecasts = 29 # FOR NOW THIS IS HARDCODED AS a 28 day ahead AHEAD bc we run on a sunday and then shift these days to the reference day of a monday

        self.location=location
        try:
            self.fmtlocation = "{:05d}".format(int(location))
        except:
            self.fmtlocation = location

    def include_weekly_data(self):
        import pandas as pd
        self.weeklydata = pd.read_csv("fourstreams__weekly.csv.gz")

    def include_weekly_county_data(self):
        import pandas as pd
        self.weeklycountydata = pd.read_csv("fourstreams__weekly__county.csv.gz")
            
    def subset2location(self):
        def subset(d):
            if self.location !="US":
                return d.loc[d.location.isin(["{:02d}".format(int(self.location)) ])] #US turns this column into a string
            else:
                return d.loc[d.location.isin([str(self.location)])] #US turns this column into a string
        
        # because county locations are all integers, so we don't have to cast as string
        def csubset(d):
            return d.loc[d.location.isin([int(self.location)])]

        if len(str(self.location)) > 2:
            self.data      = csubset(self.county_data)
        else:
            self.data      = subset(self.data)

        #self.locations = locations 

        self.buildDataForModel()
        
    def buildDataForModel(self):
        import numpy as np
        
        y = np.array(self.data.drop(columns=["location","location_name"]).set_index("date"))
        
        self.modeldata = y.T
        return y.T

    def getForecastDate(self):
        import datetime
        from epiweeks import Week

        from datetime import datetime as dt

        today = dt.today()
        dayofweek = today.weekday()

        thisWeek = Week.thisweek()
        self.thisWeek = thisWeek
        
        forecastDate = thisWeek.startdate().strftime("%Y-%m-%d")
        self.forecast_date = forecastDate
        return forecastDate

    def getClosestDay(self,numericDay): # Monday = 0, Sunday = 6
        import datetime
        from epiweeks import Week

        from datetime import datetime as dt
        today     = dt.today()

        weekAhead = today
        weekday   = today.weekday()

        while weekday != numericDay:
            weekAhead = weekAhead + datetime.timedelta(days=1)
            weekday = weekAhead.weekday()

        weekBehind = today
        weekday = today.weekday()
        while weekday != numericDay:
            weekBehind = weekBehind - datetime.timedelta(days=1)
            weekday = weekBehind.weekday()
        
        distance2weekahead  = abs( today - weekAhead)
        distance2weekbehind = abs( today - weekBehind)
                
        if distance2weekbehind < distance2weekahead:
            self.forecast_date = weekBehind.strftime("%Y-%m-%d")
            return weekBehind.strftime("%Y-%m-%d") 
        self.forecast_date = weekAhead.strftime("%Y-%m-%d")
        return weekAhead.strftime("%Y-%m-%d") 

    def generateTargetEndDates(self):
        import numpy as np
        
        target_end_dates = []
        for f in np.arange(0,3+1): # four weeks ahead
            ted = ((self.thisWeek+int(f)).enddate()).strftime("%Y-%m-%d")
            target_end_dates.append(ted)
        self.target_end_dates = target_end_dates
        return target_end_dates

    def generateTargetEndDays(self):
        import numpy as np
        import datetime
        import pandas as pd

        start = pd.to_datetime(self.forecast_date)
        
        target_end_days = []
        for f in np.arange(1,29+1): # four days ahead
            ted = (start+np.timedelta64(f,"D")).strftime("%Y-%m-%d")
            target_end_days.append(ted)
        self.target_end_days = target_end_days
        return target_end_days
    
    def generateTargetNames(self):
        import numpy as np

        # first target is always cases, second deaths, and third hosps, fourth vac_count
        targets = []
        trgts = ["case","death","hosp","vac_count"]
        for trgt in trgts:
            targets.append(["{:d} day ahead inc covid {:s}".format(ahead,trgt) for ahead in np.arange(1,29+1)])

        self.targets = targets
        return targets

    #----------processing model samples
    def formatSamples(self,model):
        import numpy as np
        import pandas as pd
        
        dataPredictions = {"forecast_date":[]
                           ,"target_end_date":[]
                           ,"location":[], "target":[],"sample":[],"value":[]}
        predictions = model.fit["ytilde"][:,-model.F:,:] # this is coming from the model object

        F = self.numOfForecasts
        for sample,forecasts in enumerate(np.moveaxis(predictions,2,0)):
            
            for n,forecast in enumerate(forecasts):
                dataPredictions["forecast_date"].extend(F*[self.forecast_date])
                dataPredictions["location"].extend( F*[self.location] )
                dataPredictions["target_end_date"].extend( self.target_end_days )
                print(n)
                dataPredictions["target"].extend( self.targets[n] )
                dataPredictions["sample"].extend( F*[sample] )
                dataPredictions["value"].extend( forecast )
        dataPredictions = pd.DataFrame(dataPredictions)

        self.dataPredictions = dataPredictions
        return dataPredictions

    def fromSamples2Quantiles(self):
        
        def createQuantiles(x):
            import numpy as np
            import pandas as pd

            quantiles = np.array([0.010, 0.025, 0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.350, 0.400, 0.450, 0.500
                                  ,0.550, 0.600, 0.650, 0.700, 0.750, 0.800, 0.850, 0.900, 0.950, 0.975, 0.990])
            quantileValues = np.percentile( x["value"], q=100*quantiles)     
            return pd.DataFrame({"quantile":list(quantiles),"value":list(quantileValues)})

        dataQuantiles = self.dataPredictions.groupby(["forecast_date"
                                                      ,"target_end_date"
                                                      ,"location","target"]).apply(lambda x:createQuantiles(x)).reset_index().drop(columns="level_4")
        dataQuantiles["type"] = "quantile"
        
        self.dataQuantiles = dataQuantiles
        return dataQuantiles

    def writeout(self):
        fmtlocation = self.fmtlocation
        self.dataQuantiles.to_csv("./location_specific_forecasts/{:s}_LUcompUncertLab-quantiles__location_{:s}.csv.gz".format(self.forecast_date, fmtlocation)
                                  ,header=True,index=False,mode="w",compression="gzip")

        self.dataPredictions.to_csv("./location_specific_forecasts/{:s}_LUcompUncertLab-predictions__location_{:s}.csv.gz".format(self.forecast_date, fmtlocation)
                                  ,header=True,index=False,mode="w",compression="gzip")

    # post processing help
    def grab_recent_forecast_file(self):
        import pandas as pd
        return pd.read_csv("{:s}_LUcompUncertLab-VAR.csv".format(self.forecast_date))

    def transform_stds_long(self):
        stds = self.stds
        stds = stds.drop(columns = ["location_name"]).sort_values("date")
        stds = stds.groupby(["location"]).apply(lambda x: x.iloc[-1]).drop(columns=["date"])

        stds_long = stds.melt(id_vars = ["location"]).rename(columns={"variable":"T","value":"std"})
        
        return stds_long

    def transform_running_means_long(self):
        running_means = self.running_means
        running_means = running_means.drop(columns = ["location_name"]).sort_values("date")
        running_means = running_means.groupby(["location"]).apply(lambda x: x.iloc[-1]).drop(columns=["date"])

        running_means.index = running_means.index.rename("T")
        running_means.columns = running_means.columns.rename("T")
        
        running_means_long = running_means.melt(id_vars = ["location"]).rename(columns={"variable":"T","value":"mean"})
        
        return running_means_long

    def grab_recent_predictions(self):
        from glob import glob
        import pandas as pd
        
        files = sorted(glob("./location_specific_forecasts/*predictions__location_{:s}.csv.gz".format(self.fmtlocation) ))

        self.predictions = pd.read_csv(files[-1])
        return self.predictions

    def grab_recent_weekly_predictions(self):
        from glob import glob
        import pandas as pd
        files = sorted(glob("./location_specific_forecasts/*{:s}__weeklypredictions.csv.gz".format(self.fmtlocation)))

        self.predictions = pd.read_csv(files[-1])
        return self.predictions

    def grab_recent_all_predictions(self):
        from glob import glob
        import pandas as pd
        files = sorted(glob("./location_specific_forecasts/*{:s}__allpredictions.csv.gz".format(self.fmtlocation)))

        self.predictions = pd.read_csv(files[-1])
        return self.predictions

    def grab_post_process_predictions(self):
        from glob import glob
        import pandas as pd
        files = sorted(glob("./location_specific_forecasts/*LUcompUncertLab-VAR4Streams__{:s}.csv.gz".format(self.fmtlocation)))

        self.predictions = pd.read_csv(files[-1])
        return self.predictions

    def grab_recent_quantiles(self):
        from glob import glob
        import pandas as pd

        files = sorted(glob("./location_specific_forecasts/*LUcompUncertLab-VAR4Streams_FINAL__{:s}.csv.gz".format(self.fmtlocation)))

        self.quantiles = pd.read_csv(files[-1])
        return self.quantiles

if __name__ == "__main__":
    pass

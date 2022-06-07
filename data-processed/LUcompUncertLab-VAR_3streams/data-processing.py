#mcandrew
# this is the first step of the model
# it loads in the JHU truth values for cases, deaths, and hosps from the data-truth directory
# then, for states, it merges them all together, and for counties, it merges county cases with state deaths and hosps

class dataprep(object):
    def __init__(self):
        self.load_cases()
        self.load_deaths()
        self.load_hosps()
        self.load_googles()
        self.merge()
        self.mergeCounties()
        self.combineStateAndCounty()
        
    def load_cases(self):
        import pandas as pd
        cases = pd.read_csv("../../data-truth/truth-Incident Cases.csv")
        cases.location = cases.location.astype(str)
        
        # we need to separate cases at the state level from cases at county level
        cases_counties = cases.loc[ [True if len(_)>=4 else False for _ in cases.location] ,:]

        cases_counties = cases_counties.rename(columns = {"value":"cases"})
        cases = cases.rename(columns = {"value":"cases"})

        norm_loc = []
        for l in cases.location:
            if len(l) == 2:
                norm_loc.append(l)
            else:
                norm_loc.append(l.zfill(2))
        cases.location = norm_loc

        self.cases = cases

        # strips the last three digits from the FIPS to get either 1 or 2 digits, then left fills with 0 if 1 digit
        state_codes = cases_counties.apply(lambda x: x['location'][:-3].zfill(2), axis=1)

        # appends to the dataframe
        cases_counties.insert(len(cases_counties.columns), 'state', state_codes)

        self.ccases_counties = cases_counties
        
    def load_deaths(self):
        import pandas as pd
        deaths = pd.read_csv("../../data-truth/truth-Incident Deaths.csv")
        deaths.location = deaths.location.astype(str)

        norm_loc = []
        for l in deaths.location:
            if len(l) == 2:
                norm_loc.append(l)
            else:
                norm_loc.append(l.zfill(2))

        deaths.location = norm_loc

        deaths = deaths.rename(columns = {"value":"deaths"})

        self.deaths = deaths

    def load_hosps(self):
        import pandas as pd
        hosps = pd.read_csv("../../data-truth/truth-Incident Hospitalizations.csv")
        hosps.location = hosps.location.astype(str)

        norm_loc = []
        for l in hosps.location:
            if len(l) == 2:
                norm_loc.append(l)
            else:
                norm_loc.append(l.zfill(2))
        
        hosps = hosps.rename(columns = {"value":"hosps"})
        #hosps["location_name"] = hosps.location_name.replace("United States","US")

        self.hosps = hosps


    def load_googles(self):
        import pandas as pd
        from datetime import date, datetime, timedelta
        googles = pd.read_csv("AllGoogleData.csv.gz", compression="gzip")
        googles.location = googles.location.astype(str)

        #adjust dates 
        cases = pd.read_csv("../../data-truth/truth-Incident Cases.csv")
        googles_date = datetime.strptime(max(googles["date"]), '%Y-%m-%d').date()
        ref_date = datetime.strptime(max(cases["date"]), '%Y-%m-%d').date()
        time_delta = ref_date - googles_date
        googles['date'] = googles["date"].apply(lambda d: datetime.strptime(d, '%Y-%m-%d').date() + time_delta)
        googles['date'] = googles['date'].astype(str)

        # we need to separate cases at the state level from cases at county level
        googles_counties = googles.loc[ [True if len(_)>=4 else False for _ in googles.location] ,:]

        googles_counties = googles_counties.rename(columns = {"value":"googles"})
        googles = googles.rename(columns = {"value":"googles"})

        norm_loc = []
        for l in googles.location:
            if len(l) == 2:
                norm_loc.append(l)
            else:
                norm_loc.append(l.zfill(2))
        googles.location = norm_loc

        self.googles = googles

        # strips the last three digits from the FIPS to get either 1 or 2 digits, then left fills with 0 if 1 digit
        state_codes = googles_counties.apply(lambda x: x['location'][:-3].zfill(2), axis=1)

        # appends to the dataframe
        googles_counties.insert(len(googles_counties.columns), 'state', state_codes)

        self.googles_counties = googles_counties



    def merge(self):
        key = ["date","location","location_name"]
        d = self.cases.merge(self.deaths, on = key)
        d = d.merge(self.hosps, on = key )
        d = d.merge(self.googles, on = key )
        self.fourstreams_state = d
        return d

    def mergeCounties(self):
        # merge the county cases with state deaths
        d = self.ccases_counties.merge(self.deaths, left_on=['date', 'state'], right_on=['date', 'location'], suffixes=(None, "_d"))
        # merge the county cases with state hosps
        d = d.merge(self.hosps, left_on=['date', 'state'], right_on=['date', 'location'], suffixes=(None, "_h"))
        d = d.merge(self.googles, left_on=['date', 'state'], right_on=['date', 'location'], suffixes=(None, "_g"))
        d = d.drop(columns=['state', 'location_d', 'location_name_d', 'location_h', 'location_name_h', 'location_g', 'location_name_g'])

        self.fourstreams_county = d
        return d

    def combineStateAndCounty(self):
        import pandas as pd
        # append county fourstreams to state fourstreams
        d = pd.concat([self.fourstreams_state, self.fourstreams_county])

        # store it in instance variable fourstreams
        self.fourstreams = d
        
        # rename columns in fourstreams_county for clarity
        self.fourstreams_county = self.fourstreams_county.rename(columns={"cases":"county_cases", "deaths":"state_deaths", "hosps":"state_hosps", "googles":"county_googles"})

    def write(self):
        def tocsv(x,f):
            x.to_csv(f,index=False,compression = "gzip")
        tocsv(self.fourstreams_state,"fourstreams__state.csv.gz")
        tocsv(self.fourstreams_county,"fourstreams__county.csv.gz")
        tocsv(self.fourstreams,"fourstreams.csv.gz")
        tocsv(self.cases,"cases.csv.gz")
        tocsv(self.deaths,"deaths.csv.gz")
        tocsv(self.hosps,"hosps.csv.gz")
        tocsv(self.googles,"googles.csv.gz")
        
if __name__ == "__main__":

    dtap = dataprep()
    dtap.write()

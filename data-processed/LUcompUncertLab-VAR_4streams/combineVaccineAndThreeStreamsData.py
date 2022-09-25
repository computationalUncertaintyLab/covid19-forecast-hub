#parth,mcandrew

import sys
import numpy as np
import pandas as pd

if __name__ == "__main__":

    #--import the threesteams dataset
    _3streams__county = pd.read_csv("../LUcompUncertLab-VAR_3streams/threestreams__county.csv.gz")
    _3streams__county = _3streams__county.rename(columns = {"county_cases":"cases","state_deaths":"deaths","state_hosps":"hosps"})
    
    _3streams__state = pd.read_csv("../LUcompUncertLab-VAR_3streams/threestreams__state.csv.gz")

    _3streams = _3streams__state.append(_3streams__county)
    date_3streams = _3streams['date'].max()

    #--import vaccine data
    vaccine1 = pd.read_csv('allVaccinationData.csv.gz')
    vaccine1_date = vaccine1['date'].max()
    if date_3streams > vaccine1_date:
        _3streams = _3streams[_3streams['date'] <= vaccine1_date]
    vaccine1 = vaccine1.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]] #subset to column we care about
    vaccine = vaccine1.rename(columns = {'recip_county':'location_name'
                                        ,'fips':'location'
                                        ,'series_complete_yes':'vac_count'})


    #--create a state level vaccine count dataset and append it to the original vaccine county dataset
    vaccine_state = vaccine.copy()

    #--import locations csv to help determine FIPS
    locations = pd.read_csv("../../data-locations/locations.csv") 
    locations = locations.rename(columns={"location":"replacement_location"})
    vaccine_state = vaccine_state.merge( locations
                                         ,left_on = ["location_name"]
                                         ,right_on=["abbreviation"], how="left" )
    def replaceUnknownWithStateFip(x):
        if x.location=="UN" or x.location=="UNK":
            return x.replacement_location
        else:
            return x.location
    vaccine_state["location"] = vaccine_state.apply(replaceUnknownWithStateFip,1)
    
    vaccine_state['state_fip'] = [str(x)[:2] for x in vaccine_state.location]

    #--clean up columns from vaccine state unknown replacement
    vaccine_state = vaccine_state[["date","state_fip","recip_state","vac_count"]]
    #--add up vaccine counts over counties
    def addUp(x):
        return pd.Series({"vac_count": sum(x.vac_count.fillna(0))})
    vaccine_state = vaccine_state.groupby(["date","state_fip","recip_state"]).apply(addUp)
    vaccine_state = vaccine_state.reset_index()
    #--compute US level vaccine counts
    vaccine_us = vaccine_state.fillna(0).groupby(["date"]).apply(addUp)
    vaccine_us["state_fip"]  ="US"
    vaccine_us["recip_state"]="US"
    vaccine_us = vaccine_us.reset_index()

    vaccine_us["date"]   = pd.to_datetime(vaccine_us.date)
    
    #--add US quantities to state
    vaccine_state = vaccine_state.append(vaccine_us)
    
    #--add in a location name which will be the state name
    vaccine_state["location_name"] = vaccine_state.recip_state

    #--change state fip column to location
    vaccine_state = vaccine_state.rename(columns = {"state_fip":"location"})

    #--append togethe the county and state level vaccine data
    vaccine = vaccine.append(vaccine_state)
    #--exclude locations that are not targets for forecasting
    locations = locations.rename( columns = {"replacement_location":"location"} )
    vaccine = vaccine.merge(locations, on = ["location"])

    #--column cleanup
    vaccine = vaccine[["date","location","vac_count","population"]]
    
    #--reformat dates in threestream and vaccines
    _3streams["date"] = pd.to_datetime(_3streams.date)
    vaccine["date"]   = pd.to_datetime(vaccine.date)

    #--match location column format for vaccine
    def reformat(x):
        try:
            return str(int(x))
        except ValueError:
            return x
    #--create dataframe of unique locations
    unique_locations = _3streams["location"].unique()
    unique_location_and_replacements = {"location":[],"reformatted_location":[]}
    for location in unique_locations:
        unique_location_and_replacements["location"].append(location)

        reformatted_location = reformat(location)
        unique_location_and_replacements["reformatted_location"].append(reformatted_location)
    unique_location_and_replacements = pd.DataFrame(unique_location_and_replacements)
    
    _3streams = _3streams.merge(unique_location_and_replacements, on = ["location"])
    _3streams["location"] = _3streams.reformatted_location
    _3streams = _3streams.drop(columns=["reformatted_location"])

    def removeLeadingZero(x):
        try:
            return str(int(x.location))
        except:
            return x.location
    vaccine["location"] = vaccine.apply(removeLeadingZero,1)

    #--(finally) merge in vaccine data
    _4streams = _3streams.merge(vaccine, on = ["date","location"], how="left")
    _4streams = _4streams.loc[_4streams.date>"2021-01-01"]

    #--create a dataframe with location, proportion, and replacement_vaccine.
    
    #--state level replacement
    vaccine_state["date"] = pd.to_datetime(vaccine_state.date)
    vaccine_us_replacement = vaccine_us[["date","vac_count"]].rename(columns={"vac_count":"us_vac_count"})
    
    #vaccine_us_replacement = vaccine_us_replacement.merge(locations, on = [])
    
    vaccine_state = vaccine_state.merge(vaccine_us_replacement, on = ["date"])
    us_population = float(locations.loc[locations.location=="US","population"].values)

    locations = locations[["location","population"]]
    locations["state"] = [1 if len(x)==2 else 0 for x in locations.location.values]
    #--split pops into state and county
    states   = locations.loc[locations.state==1]
    counties = locations.loc[locations.state==0]

    locations = locations.drop(columns=["state"])

    #--add state location to merge in state populations
    counties['state_location'] = [str(x)[:2] for x in counties.location]

    locations = locations.rename(columns = {"population":"state_population", "location":"state_location"})
    counties = counties.merge(locations, on = ["state_location"] )

    #--compute population proportions
    counties["prop"] = counties["population"] / counties["state_population"]
    states["prop"]   = states["population"] / us_population 

    counties = counties[["location","prop"]]
    states   = states[["location","prop"]]
    
    location_props = counties.append(states)
    location_props["location"] = location_props["location"].apply(reformat,1)
    #--add in proprotion
    _4streams = _4streams.merge( location_props, on = ["location"] )
    #--merge in the population proportions
    def levelup(x):
        loc = str(x)
        if len(loc)<=1:
            return "US"
        return loc[:2]  
    _4streams["levelup"] = _4streams.location.apply(levelup,1)
    vaccine_replace = vaccine.rename(columns={"vac_count":"replacement_count"})
    vaccine_replace = vaccine_replace[["date","location","replacement_count"]]
    _4streams = _4streams.merge( vaccine_replace, left_on = ["date","levelup"], right_on = ["date","location"], how="left"  )
    #--write out the missing rows
    missing_rows = _4streams.loc[np.isnan(_4streams.vac_count)]
    missing_rows.to_csv("missing_rows.csv")
    
    #--impute missing vaccine counts 
    def impute(x):
        if np.isnan(x.vac_count):
            return x.prop*x.replacement_count
        else:
            return x.vac_count
    _4streams["vac_count"] = _4streams.apply(impute,1)
    #--cleanup columns
    _4streams = _4streams[ ["date","location_x","location_name","cases","deaths","hosps","vac_count"] ]
    _4streams = _4streams.rename(columns={"location_x":"location"})
    #--restrict to only FIPS in the locations.csv file
    locations = pd.read_csv("../../data-locations/locations.csv")
    locations = locations[["location"]]

    #--reformat locations
    locations["location"] = locations.location.apply(reformat,1)
    
    _4streams = _4streams.merge(locations, on = ["location"])
    pd.options.display.float_format = '{:.4f}'.format

    #investigating starting with Alabama

    #_4streams['vac_count'] = _4streams['vac_count'].fillna(1)
    #_4streams['vac_count'] = _4streams['vac_count'].astype(int)
    #_4streams.to_csv("_4streams.csv.gz", compression="gzip")

    #trying on all location
    #_4streams = _4streams.set_index('date')
    #_4streams['vac_count'] = _4streams['vac_count'].interpolate()
    #_4streams.reset_index(inplace=True)
    #_4streams['vac_count'] = _4streams['vac_count'].astype(int)
    #_4streams.to_csv("_4streams.csv.gz", compression="gzip")

    #script to interpolate the data for the vaccine values

    _4streams['date'] = _4streams['date'].dt.strftime('%Y-%m-%d')
    # for location in _4streams['location_name'].unique():
    #        for date in _4streams['date'].unique():
    #            if date >= '2022-06-16':
    #                _4streams['vac_count'] = _4streams['vac_count'].interpolate()
    #                _4streams['vac_count'] = _4streams['vac_count'].astype(int)
    
    #converting to csv
    # _4streams.to_csv("_4streams.csv.gz", compression="gzip")
    temp1_lis = ['Jefferson County']
    temp2_lis = 1
    print(_4streams.info())
    print(_4streams.head())
    print(len(_4streams))
    temp1 = _4streams.query('location_name == ["Autauga County"] and location == ["1001"]')
    print(temp1)
    #we will hve to select county names in the location_name column to subset for graphs
    #temp2_lis = ['Jefferson County', '']
    temp1.to_csv("_4streams_1st_Set.csv.gz", compression="gzip")

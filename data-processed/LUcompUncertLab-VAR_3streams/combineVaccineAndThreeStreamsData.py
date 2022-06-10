#parth,mcandrew

import sys
import numpy as np
import pandas as pd

if __name__ == "__main__":

    #--import the threesteams dataset
    _3streams__county = pd.read_csv("threestreams__county.csv.gz")
    _3streams__county = _3streams__county.rename(columns = {"county_cases":"cases","state_deaths":"deaths","state_hosps":"hosps"})
    
    _3streams__state = pd.read_csv("threestreams__state.csv.gz")

    _3streams = _3streams__state.append(_3streams__county)
    
    #--import vaccine data
    vaccine = pd.read_csv('allVaccinationData.csv.gz')
    vaccine = vaccine.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]] #subset to column we care about
    vaccine = vaccine.rename(columns = {'recip_county':'location_name'
                                        ,'fips':'location'
                                        ,'series_complete_yes':'vac_count'})


    #--create a state level vaccine count dataset and append it to the original vaccine county dataset
    vaccine_state = vaccine.copy()

    #--import locations csv to help determine FIPS
    locations = pd.read_csv('../../data-locations/locations.csv')
    locations = locations.rename(columns={"location":"replacement_location"})

    vaccine_state = vaccine_state.merge( locations
                                         ,left_on = ["location_name"]
                                         ,right_on=["abbreviation"], how="left" )
    def replaceUnknownWithStateFip(x):
        if x.location=="UN":
            return x.replacement_location
        else:
            return x.location
    vaccine_state["location"] = vaccine_state.apply(replaceUnknownWithStateFip,1)
    
    vaccine_state['state_fip'] = [str(x)[:2] for x in vaccine_state.location]

    #--clean up columns from vaccine state unknown replacement
    vaccine_state = vaccine_state[["date","state_fip","recip_state","vac_count"]]
    
    #--add up vaccine counts over counties
    def addUp(x):
        return pd.Series({"vac_count": sum(x.vac_count)})
    vaccine_state = vaccine_state.groupby(["date","state_fip","recip_state"]).apply(addUp)
    vaccine_state = vaccine_state.reset_index()

    #--compute US level vaccine counts
    # groupby the same list above but exclude state_fip and recip_state
    # FOR PARTH
    # add rows to vaccine_state that include US level vaccine counts.
    
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
    
    vaccine["location"] = vaccine.location.astype(int).astype(str)
    
    #--(finally) merge in vaccine data
    _4streams = _3streams.merge(vaccine, on = ["date","location"], how="left")
    _4streams = _4streams.loc[_4streams.date>"2021-01-01"]

    #--create a dataframe with location, proportion, and replacement_vaccine.
    
    

    #print(set(_3streams.location.unique() ) - set(_4streams.location.unique()))

    

from interface import interface
from model import VAR
import argparse
import pandas as pd
from sklearn.preprocessing import StandardScaler
from numpy import sqrt
import statistics
if __name__ == "__main__":
    #importing the state level script
    fourstreams_state = pd.read_csv('fourstreams__state.csv.gz', index_col = None)
    #selecting the four columns of cases, deaths, hosps, vac_count
    fourstreams_state_date = fourstreams_state['date']
    fourstreams_state_location = fourstreams_state['location']
    fourstreams_state_location_name = fourstreams_state['location_name']
    fourstreams_state_cases = fourstreams_state['cases']
    fourstreams_state_deaths = fourstreams_state['deaths']
    fourstreams_state_hospitalization = fourstreams_state['hosps']
    fourstreams_state_vac = fourstreams_state['vac_count']
    #finding the average of each of the column
    avg_cases_fourstreams_state = fourstreams_state['cases'].mean()
    avg_deaths_fourstreams_state = fourstreams_state['deaths'].mean()
    avg_hosps_fourstreams_state = fourstreams_state['hosps'].mean()
    avg_cases_fourstreams_state = fourstreams_state['cases'].mean()
    #reshaping the columns
    fourstreams_state_cases = fourstreams_state_cases.values.reshape(len(fourstreams_state_cases), 1)
    fourstreams_state_deaths = fourstreams_state_deaths.values.reshape(len(fourstreams_state_deaths), 1)
    fourstreams_state_hosps = fourstreams_state_hospitalization.values.reshape(len(fourstreams_state_hospitalization), 1)
    fourstreams_state_vac = fourstreams_state_vac.values.reshape(len(fourstreams_state_vac),1)
    #train the standardization
    #cases-Standardization
    scaler = StandardScaler()
    scaler = scaler.fit(fourstreams_state_cases)
    normalized_state_cases = scaler.transform(fourstreams_state_cases)
    #inverse-just checking
    #inversed = scaler.inverse_transform(normalized)
    #for i in range(5):
    	#print(inversed[i])
    #deaths-Standardization
    scaler = StandardScaler()
    scaler = scaler.fit(fourstreams_state_deaths)
    normalized_state_deaths = scaler.transform(fourstreams_state_deaths)
    #hosps-Standardization
    scaler = StandardScaler()
    scaler = scaler.fit(fourstreams_state_hosps)
    normalized_state_hosps = scaler.transform(fourstreams_state_hosps)
    #vac_count-Standardization
    scaler = StandardScaler()
    scaler = scaler.fit(fourstreams_state_vac)
    normalized_state_vac_counts = scaler.transform(fourstreams_state_vac)
    #merge all the values in a single dataframe
    fourstreams_state_cases_normalized = pd.DataFrame(data = normalized_state_cases)
    fourstreams_state_deaths_normalized = pd.DataFrame(data = normalized_state_deaths)
    fourstreams_state_hosps_normalized = pd.DataFrame(data = normalized_state_hosps)
    fourstreams_state_vac_count_normalized = pd.DataFrame(data = normalized_state_vac_counts)
    fourstreams_state_date = pd.DataFrame(fourstreams_state_date)
    fourstreams_state_normalized = pd.concat((fourstreams_state_date,fourstreams_state_location,fourstreams_state_location_name,fourstreams_state_cases_normalized, fourstreams_state_deaths_normalized, fourstreams_state_hosps_normalized,fourstreams_state_vac_count_normalized), axis=1)
    fourstreams_state_normalized.columns = ['date','location','location_name','cases','deaths','hosps','vac_count']
    #converting it to csv
    fourstreams_state_normalized.to_csv('fourstreams_state_normalized.csv.gz')

    

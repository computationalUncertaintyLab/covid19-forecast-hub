import sys
import numpy as np
import pandas as pd

if __name__ == "__main__":
    al_vac = pd.read_csv("allVaccinationData.csv.gz")
    al_vac = al_vac.loc[:, ["date","fips","recip_county", "recip_state", "series_complete_yes"]] #subset to column we care about
    al_vac = al_vac.rename(columns = {'recip_county':'location_name'
                                        ,'fips':'location'
                                        ,'series_complete_yes':'vac_count'})
    al_vac = al_vac[al_vac['recip_state'] == 'AL']
    al_vac['date'] = pd.to_datetime(al_vac['date'])
    temp1 = al_vac.groupby([al_vac['date'].dt.date])['vac_count'].sum()
    print(temp1.tail(40))

import pandas as pd
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

if __name__ == "__main__":

    vacdf = pd.read_csv("COVID-19_Vaccinations_in_the_United_States_County.csv")
    vacdf.drop(['Administered_Dose1_Recip_5Plus','Administered_Dose1_Recip_5PlusPop_Pct','Administered_Dose1_Recip_12Plus','Administered_Dose1_Recip_12PlusPop_Pct','Administered_Dose1_Recip_18Plus','Administered_Dose1_Recip_18PlusPop_Pct','Administered_Dose1_Recip_65Plus','Administered_Dose1_Recip_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_SVI','Series_Complete_5to17Pop_Pct_SVI','Series_Complete_12PlusPop_Pct_SVI','Series_Complete_18PlusPop_Pct_SVI','Series_Complete_65PlusPop_Pct_SVI','Booster_Doses_12Plus','Booster_Doses_12Plus_Vax_Pct','Booster_Doses_18Plus','Booster_Doses_18Plus_Vax_Pct','Booster_Doses_50Plus','Booster_Doses_50Plus_Vax_Pct','Booster_Doses_65Plus','Booster_Doses_65Plus_Vax_Pct','Series_Complete_5Plus','Series_Complete_5PlusPop_Pct','Series_Complete_5to17','Series_Complete_5to17Pop_Pct','Series_Complete_12Plus','Series_Complete_12PlusPop_Pct','Series_Complete_18Plus','Series_Complete_18PlusPop_Pct','Series_Complete_65Plus','Series_Complete_65PlusPop_Pct','Series_Complete_5PlusPop_Pct_UR_Equity','Series_Complete_5to17Pop_Pct_UR_Equity','Series_Complete_12PlusPop_Pct_UR_Equity','Series_Complete_18PlusPop_Pct_UR_Equity','Series_Complete_65PlusPop_Pct_UR_Equity','Booster_Doses_12PlusVax_Pct_SVI','Booster_Doses_18PlusVax_Pct_SVI','Booster_Doses_65PlusVax_Pct_SVI','Booster_Doses_12PlusVax_Pct_UR_Equity','Booster_Doses_18PlusVax_Pct_UR_Equity','Booster_Doses_65PlusVax_Pct_UR_Equity','Census2019_5PlusPop','Census2019_5to17Pop','Census2019_12PlusPop','Census2019_18PlusPop','Census2019_65PlusPop','Metro_status','Census2019','Booster_Doses_Vax_Pct_UR_Equity','Booster_Doses_Vax_Pct_SVI','Series_Complete_Pop_Pct_UR_Equity','Series_Complete_Pop_Pct_SVI','SVI_CTGY','Administered_Dose1_Recip','Administered_Dose1_Pop_Pct'], inplace=True, axis = 1)
    vacdf1 = vacdf[vacdf['Date'] == '04/13/2022']
    print(len(vacdf1))
    print(len(vacdf))
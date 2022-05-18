#gandhi

import sys
import numpy as np
import pandas as pd

from sodapy import Socrata

if __name__ == "__main__":

    client = Socrata("data.cdc.gov", None)

    results = client.get("unsk-b7fc",limit=10**4,offset=0)
    allData = pd.DataFrame(results)

    n=1
    while results !=[]:
        results = client.get("unsk-b7fc",limit=10**4,offset=n*10**4)
        resultsData = pd.DataFrame(results)
        
        allData = allData.append( resultsData )

        n+=1
    allData.to_csv("allVaccinationData.csv")

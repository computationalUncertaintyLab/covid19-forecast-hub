#mcandrew

import sys
import numpy as np
import pandas as pd



if __name__ == "__main__":


    d = pd.read_csv("threestreams__state.csv.gz")

    pa = d.loc[d.location=="42"]


    ## AR with gaussian adaptive basis
    targets = ["cases","deaths","hosps"]
    X = pa.loc[:,targets].to_numpy().T



    num_lags = 2

    def lag(y,l):
        m = np.zeros( y.shape )
        m[:,:] = np.nan
        m[:,l:] = y[:, :-l]
        
        return m

    def lag_gauss(y,l,m,s):
        from scipy.stats import multivariate_normal as mvn       
        
        m = np.zeros( y.shape )
        m[:,:] = np.nan

        ys = y[:, :-l]
        vals = mvn(m,s).pdf( ys.T )

        m[:,l:] = vals
        return vals
    
    def nll(obs, features, params ):
        obs_row, obs_col   = obs.shape
        feat_row, feat_col = features.shape
        
        b0 = np.array(params[:obs_row]).reshape((obs_row,))
        b1 = np.array(params[obs_row: (obs_row+ obs_row*feat_row) ]).reshape( ( obs_row , feat_row) )

        #E = np.array(params[-obs_row**2:]).reshape(obs_row,obs_row)
        #E = np.eye( obs_row )*abs(params[-1])
        
        logpdfs = np.array([ mvn( (b0 + b1.dot(features[:,i])), E).logpdf(obs[:,i]) for i in range(obs_col)])
        return sum( logpdfs[~np.isnan(logpdfs)] )

    lags = [1,2]
    for n,l in enumerate(lags):
        if n==0:
            features = lag(X,l)
        else:
            lagfeats = lag(X,l)
            features = np.vstack( (features,lagfeats) )

    from mystic.solvers import diffev 
    cost = lambda p: nll( X, features, p )

    rslt = diffev(cost, np.random.random(size=22) ,npop=15)
        


        
    
    

    
    
    




    
    

    


    


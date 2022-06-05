#mcandrew

import sys
import numpy as np
import pandas as pd

if __name__ == "__main__":


    d = pd.read_csv("threestreams__state.csv.gz")

    pa = d.loc[d.location=="42"]


    class abVAR(object):
        def __init__(self, data, targets):
            self.data = data
            self.D = data.loc[:,targets].to_numpy().T

            self.ntargets = self.D.shape[0]
            self.nobs     = self.D.shape[1]

            self.features = self.intercept() # always start with an intercept

            self.characteristics = { "lag":[], "adaptive_lag":[] }
            
        def lag(self,y,l):
            m = np.zeros( y.shape )
            m[:,:] = np.nan
            m[:,l:] = y[:, :-l]
            return m

        def lag_norm(self,y,l,mu,s):
            from scipy.stats import norm
        
            m = np.zeros( y.shape )
            m[:] = np.nan

            ys = y[:-l]
            vals = norm(mu,s).pdf( ys )

            m[l:] = vals
            return m

        def add_lagFeature(self,l):
            self.features = np.vstack( (self.features,self.lag(self.D,l) ) )
            self.compute_num_features()

            self.characteristics["lag"].append(l)
            
        def compute_num_features(self):
            self.nfeatures = self.features.shape[0]

        def intercept(self):
            intercept = np.ones( (self.ntargets, self.nobs) )
            return intercept
            
        def sse(self,obs, features, params ):
            
            b = np.array(params).reshape( ( self.ntargets , self.nfeatures) )

            sums = sum( (obs - b.dot(features))**2 )
            return sum(sums[~np.isnan(sums)] )

        def solve(self):
            from mystic.solvers import diffev
            import numpy as np
            
            cost = lambda p: self.sse( self.D, self.features, p )
            rslt = diffev(cost, np.random.random(size=self.ntargets*self.nfeatures) ,npop=15)

            self.optim_params = rslt
            return rslt

        def estimate(self):
            B = self.optim_params.reshape(self.ntargets, self.nfeatures)
            
            preds = B.dot(self.features)       
            self.preds = preds

        def predict_expectation(self,horizon):
            predictions = np.zeros( (self.ntargets, horizon) )
            
            for h in range(horizon):
                


        def compute_residuals(self):
            self.epsilons = self.D - self.preds

    model = abVAR(pa, ["cases","hosps","deaths"])
    model.add_lagFeature(1)
    model.add_lagFeature(2)

    model.solve()
    model.estimate()
    
    model.compute_residuals()

    
    
        
    
    #for n,l in enumerate([1]):
    #    lagfeats = lag_norm(obs[0,:],l,rslt[-2],rslt[-1])
    #    features = np.vstack( (features,lagfeats) )
    
    #b0 = np.array(rslt[:3]).reshape(3,1)
    #b1 = np.array(rslt[3:-2]).reshape( ( 3 , 7) )
    #
    


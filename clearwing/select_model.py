from datetime import timedelta
from numpy import linalg, argmax
from scipy.spatial import distance, KDTree
from pandas.tseries.index import date_range
from pandas import Series
import numpredict
import time, sys, random

def is_long(df, curr_datetime):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    
    return (next_close - current_close) >= 0.03 and (next_low - current_close) > -0.03
             
def is_short(df, curr_datetime):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    
    return (next_close - current_close) <= -0.03 and (next_high - current_close) < 0.03

def get_top_dims(data, top_dims, start_date, end_date, top=10):
    idx = top_dims.ix[end_date].index
    dates = date_range(start_date, end_date, freq='Min')
    return data.ix[dates,idx[:top]].dropna()
    
class KNN:
    def __init__(self, data, qqq):
        self.data = data
        self.qqq = qqq
        
    def getdistances(self, data, vec1):
        distancelist = []
        
        for i in range(len(data)):
            vec2 = data.ix[i,:]
            distancelist.append((euclidean(vec1, vec2),i))
        
        distancelist.sort()
        return distancelist
        
    def qqq_classify(self, idx):
        if self.qqq.ix[idx, 'is_long']:
            return 1
        elif self.qqq.ix[idx, 'is_short']:
            return 2
        else:
            return 0
    
    def estimate(self, vec, k=7):
        dlist = self.getdistances(self.data,vec)
        vals = [0,0,0]
        
        # Take the average of the top k results
        for i in range(k):
            idx = self.data.index[dlist[i][1]]
            if idx in self.qqq.index:
                vals[self.qqq_classify(idx)] +=  numpredict.gaussian(dlist[i][0])
                
        if vals[1] == vals[2]:
            return 0
        return argmax(vals)
    
    def error_score(self, inpt, k=7):
        ncor = 0.
        count = 0
        st = time.time()
        
        for i in range(  len(inpt)  ):
            
            if inpt.index[i].hour == 16:
                continue
                
            if inpt.index[i] in self.qqq.index:
                row = inpt.ix[i,:]
                
                est = self.estimate(row, k)
                act = self.qqq_classify(inpt.index[i])
                
                if est == act:
                    ncor = ncor + 1
                count = count + 1
                
        pct = 1 - (ncor / count)
        return pct * 100.0
        

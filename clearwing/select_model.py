from datetime import timedelta
from numpy import linalg, argmax
from scipy.spatial import distance, KDTree
from pandas.tseries.index import date_range
from pandas import Series
import numpredict
import time, sys, random

def get_cov_inv(df):
    """
    returns the multiplicative inverse of the covariance matrix
    """
    return linalg.inv(df.cov())
    
def mahalanobis_dist(u, v, df=None, VI=None):
    """
    returns the mahalanobis distance as computed by scipy
    """
    if VI is None:
        VI = get_cov_inv(df)
    return distance.mahalanobis(u, v, VI)
    
def euclidean(u,v):
    return distance.euclidean(u, v)
    
def is_long(df, curr_datetime):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    return (next_low - current_close) > -0.03 and \
           ( (next_high - current_close) >= 0.03 or \
             (next_close - current_close) >= 0.03 )
    
def is_short(df, curr_datetime):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    return (next_low - current_close) < -0.03 and \
           ( (next_high - current_close) <= 0.03 or \
             (next_close - current_close) <= 0.03 )

def get_top_dims(data, top_dims, start_date, end_date, top=10):
    idx = top_dims.ix[end_date].index
    dates = date_range(start_date, end_date, freq='Min')
    return data.ix[dates,idx[:top]].dropna()
    
class KNN:
    def __init__(self, data, qqq):
        self.data = data
        self.qqq = qqq
        self.VI = get_cov_inv(data)
        
    def cross_validate(self, k_fold, k_nearest):
        r = Series([x % k_fold for x in range(len(self.data))])
        random.shuffle(r)
        ave = 0.0
        for i in range(0, k_fold):
            x = self.data.index[r != i]
            y = self.data.index[r == i]
            trainset = self.data.ix[x, :]
            testset = self.data.ix[y, :]
            knn = KNN(trainset, self.qqq)
            ave += knn.error_score(testset, k_nearest)
            print 'ave = %f' % (ave/(i+1))
        ave /= k_fold
        return ave
        
    def getdistances(self, data, vec1):
        distancelist = []
        
        VI = get_cov_inv(data)
        
        # Loop over every item in the dataset
        for i in range(len(data)):
            vec2 = data.ix[i,:]
            
            # Add the distance and the index
            distancelist.append((mahalanobis_dist(vec1,vec2,VI=VI),i))
            #distancelist.append((euclidean(vec1, vec2),i))
        
        # Sort by distance
        # Should this not be reversed - ie largest to lowest?
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
        # Get sorted distances
        dlist = self.getdistances(self.data,vec)
        vals = [0,0,0]
        
        # Take the average of the top k results
        for i in range(k):
            idx = self.data.index[dlist[i][1]]
            if idx in self.qqq.index:
                # majority vote
                #vals[self.qqq_classify(idx)] +=  self.qqq_classify(idx)
                
                # weighted by mahalanobis distance
                vals[self.qqq_classify(idx)] += numpredict.inverseweight(dlist[i][0])
                
                # weighted by gaussian
                #vals[self.qqq_classify(idx)] +=  numpredict.gaussian(dlist[i][0])
        s = sum(vals)
        if s == 0:
            return 0
        return s / abs(s)
        if vals[1] == vals[2]:
            return 0
        return argmax(vals)
    
    def error_score(self, inpt, k=7):
        ncor = 0.
        count = 0
        st = time.time()
        for i in range(len(inpt)):
            if inpt.index[i].hour == 16:
                continue
            if inpt.index[i] in self.qqq.index:
                row = inpt.ix[i,:]
                
                est = self.estimate(row, k)
                act = self.qqq_classify(inpt.index[i])
                if est == act:
                    ncor = ncor + 1
                count = count + 1
                #if count % 10 == 0:
                #    print '%s   %d/%d  (%.2f)  %.2f s' % (inpt.index[i],ncor,count,ncor/count,time.time()-st)
        pct = 1 - (ncor / count)
        return pct * 100.0
        

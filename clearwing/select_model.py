from datetime import timedelta
from numpy import linalg
from scipy.spatial import distance, KDTree

def get_cov_inv(df):
    """
    returns the multiplicative inverse of the covariance matrix
    """
    return linalg.inv(df.cov())
    
def mahalanobis_dist(u, v, df, VI):
    """
    returns the mahalanobis distance as computed by scipy
    """
    if VI is None:
        VI = get_cov_inv(df)
    return distance.mahalanobis(u, v, VI)
    
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

def get_top_dims(data, top_vars, date_time, percent):
    idx = top_vars.ix[date_time].index
    last_idx = top_vars.ix[date_time]['% cumulative'].searchsorted(percent)
    return data[idx[:last_idx]]

class KNN:
    def __init__(self, data, leaf=7):
        self.data = data
        self.VI = get_cov_inv(data)
        self.leaf = leaf
        
    def getdistances(self, data,vec1):
        distancelist = []
        
        VI = get_cov_inv(data)
        
        # Loop over every item in the dataset
        for i in range(len(data)):
            vec2 = data.ix[i,:]
            
            # Add the distance and the index
            distancelist.append((mahalanobis_dist(vec1,vec2,VI=VI),i))
        
        # Sort by distance
        distancelist.sort()
        return distancelist
    
    def estimate(self, vec, classifier, k=7):
        # Get sorted distances
        dlist = self.getdistances(self.data,vec)
        avg = 0.0
        
        # Take the average of the top k results
        for i in range(k):
            idx = dlist[i][1]
            avg += classifier(data, data.index[idx])
        avg = avg / k
        return avg
    

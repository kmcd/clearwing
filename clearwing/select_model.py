from datetime import timedelta
from numpy import linalg
from scipy import spatial

def get_cov_inv(df):
    """
    returns the multiplicative inverse of the covariance matrix
    """
    return linalg.inv(df.cov())
    
def mahalanobis_dist(u, v, df):
    """
    returns the mahalanobis distance as computed by scipy
    """
    VI = get_cov_inv(df)
    return spatial.distance.mahalanobis(u, v, VI)
    
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


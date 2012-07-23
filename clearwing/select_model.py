from numpy import linalg
from scipy import spatial

def get_cov_inv(df):
    """
    returns the multiplicative inverse of the covariance matrix
    """
    return linalg.inv(df.cov())
    
def mahalanobis_dist(u, v, VI):
    """
    returns the mahalanobis distance as computed by scipy
    """
    VI = get_cov_inv(df)
    return spatial.distance.mahalanobis(u, v, VI)
    

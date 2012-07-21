from matplotlib import mlab
from pandas.tseries.offsets import BDay
from pandas import *
from sets import Set
import os

def get_pca_variance(df, dates):
    """
    computes the variance of each dimension per date (with 30 days loopback)
    """
    result = {}
    for day in dates:
        end_day = day+BDay(30)
        sd = mlab.PCA(df.ix[day:end_day]).sigma
        variance = [x**2 for x in sd]
        result[end_day] = Series(variance, index=df.columns)
    return DataFrame.from_dict(result, orient='index')

def get_trading_days():
    """
    this is a utility function to gather the set of dates in QQQ
    this resolves the bug of having trading days on a non-business days
    """
    with open(os.path.join('data','qqq','table_qqq.csv')) as f:
        s = Set()
        for line in f:
            parts = line.partition(',')
            if parts[0] not in s:
                s.add(parts[0])
        return s
            

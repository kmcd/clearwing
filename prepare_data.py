# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY
from clearwing import extract_data, utils
from matplotlib import mlab
from pandas import *
from pandas.tseries.offsets import BDay
import numpy as np
import csv
import os
import glob
import sys


qqq_start = datetime(1999,3,10)
qqq_end = datetime(2012,6,7) + timedelta(days=-60)
trading_days = date_range(qqq_start, qqq_end, freq='B')

start_day = sample(trading_days, 1)[0]
training_set = date_range(start_day, periods=60, freq='B')
training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

def get_pca_variance(df, dates):
    result = {}
    for day in dates:
        end_day = day+BDay(30)
        sd = mlab.PCA(df.ix[day:end_day]).sigma
        variance = [x**2 for x in sd]
        result[end_day] = Series(variance, index=df.columns)
    return DataFrame.from_dict(result, orient='index')

close_mat = []
names = []

for nasdaq_100_file in glob.glob(os.path.join('data','nasdaq_100','*')):
    print '\n'
    print 'loading %s' % nasdaq_100_file
    try:
        df = extract_data.start(nasdaq_100_file, training_set_str)
        if len(df.index) == 0:
            print 'training set is empty'
        else:
            df = df.ix[1:,'Close']
            print 'showing first and last three rows'
            print df.head(3)
            print df.tail(3)
            close_mat.append(df)
            names.append(nasdaq_100_file.rpartition('_')[2][:-4])
    except:
        print sys.exc_info()
        print 'error in %s' % nasdaq_100_file
        
close_mat = concat(close_mat, axis=1, keys=names)
#print close_mat.head()
#print close_mat.tail()

variance_matrix = get_pca_variance(close_mat, training_set[:30])
#print variance_matrix

top_vars = []
for i in range(0,len(variance_matrix)):
    row = variance_matrix.ix[i,:]
    row = row / row.sum() * 100.0
    new_index = row.index[np.argsort(row)[-1:-11:-1]]
    row = row.reindex(index=new_index)
    top_vars_day = concat([row, row.cumsum()],axis=1, keys=['% variance', '% cumulative'])
    top_vars.append(top_vars_day)
    
top_vars = concat(top_vars, keys=variance_matrix.index)
print top_vars
print top_vars.head(30)
print top_vars.tail(30)
    
# Save as RANDOM_DATE_training.csv


# Expand & normalise data set
# For each nasdaq 100 stock
#   - expand out h->c, o->c ...
#   - normalise each as %difference from last bar
# Write to RANDOM_DATE_training_normalised.csv

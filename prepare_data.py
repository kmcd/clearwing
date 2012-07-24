# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY
from clearwing import extract_data, select_model
from pandas import *
import numpy as np
import csv
import os
import glob
import sys

# Generate series of business days from QQQ earliest date to QQQ latest date
qqq_start = datetime(1999,3,10)
qqq_end = datetime(2012,6,7) + timedelta(days=-60)
trading_days = date_range(qqq_start, qqq_end, freq='B')

# Pick a date at random
# Generate a list of 60 business days starting from the random date chosen
start_day = sample(trading_days, 1)[0]
training_set = date_range(start_day, periods=60, freq='B')
training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

nasdaq_comp = {}  # compilation of 'High', 'Low' and 'Close' columns from each nasdaq component
names = []      # list of nasdaq component names

for nasdaq_100_file in glob.glob(os.path.join('data','nasdaq_100','*')):
    print '\n'
    print 'loading %s' % nasdaq_100_file
    try:
        df = extract_data.start(nasdaq_100_file, training_set_str)
        if len(df.index) == 0:  # discard empty training set
            print 'training set is empty'
        else:
            df = df.ix[:,'High':'Close']  # remove unused rows and columns
            print 'showing first and last three rows'
            print df.head(3)
            print df.tail(3)
            nasdaq_name = nasdaq_100_file.rpartition('_')[2][:-4]
            names.append(nasdaq_name)
            nasdaq_comp[nasdaq_name] = df
    except:
        print sys.exc_info()
        print 'error in %s' % nasdaq_100_file
    # for dev purposes only
    #if len(nasdaq_comp) == 3:
    #    break
        
nasdaq_comp_close = [df.ix[:,'Close'] for df in nasdaq_comp.values()]
nasdaq_comp_close = concat(nasdaq_comp_close, axis=1, keys=names, join='inner')

# matrix to store all the variance computed by PCA
variance_matrix = extract_data.get_pca_variance(nasdaq_comp_close, training_set[:30])

top_vars = []   # matrix of top 10 dimensions with highest variance per day
for i in range(0,len(variance_matrix)):
    row = variance_matrix.ix[i,:]
    row = row / row.sum() * 100.0
    new_index = row.index[np.argsort(row)[-1:-11:-1]] # get top 10 highest
    row = row.reindex(index=new_index)
    top_vars_day = concat(      # top 10 highest variance for the day
                        [row, row.cumsum()],
                        axis=1,
                        keys=['% variance', '% cumulative'])
    top_vars.append(top_vars_day)
    
top_vars = concat(top_vars, keys=variance_matrix.index)
print '\n\nTop 10 variance per day'
print top_vars.head(3)
print top_vars.tail(3)

<<<<<<< HEAD
print '\nmahalanobis distance between 1st and 2nd row is %s' % \
        select_model.mahalanobis_dist(
                nasdaq_comp_close.ix[1,:],
                nasdaq_comp_close.ix[2,:],
                nasdaq_comp_close)

print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=10,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=11,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=12,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=13,minute=0))
# Save as RANDOM_DATE_training.csv


# Expand & normalise data set
# For each nasdaq 100 stock
#   - expand out h->c, o->c ...
#   - normalise each as %difference from last bar
# Write to RANDOM_DATE_training_normalised.csv
=======
print 'mahalanobis distance between 1st and 2nd row is %s' % \
        select_model.mahalanobis_dist(close_mat.ix[1,:], close_mat.ix[2,:], close_mat)
>>>>>>> f8a3c96704cfc838f4978084e31533947d02c75f

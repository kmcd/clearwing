# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import datetime, date, timedelta
from clearwing import extract_data, select_model
from pandas import *
from pandas.tseries.offsets import BDay
import numpy as np
import os, glob, sys

# for debugging/printing purposes
set_printoptions(max_rows=100, max_columns=200, max_colwidth=10000)
dev_mode = False

# Generate series of business days from QQQ earliest date to QQQ latest date
qqq_start = datetime(1999,3,10)
qqq_end = datetime(2012,7,19) + timedelta(days=-60)
trading_days = date_range(qqq_start, qqq_end, freq='B')

# Pick a date at random
# Generate a list of 60 business days starting from the random date chosen
start_day = datetime(2010,11,12)#sample(trading_days, 1)[0]
training_set = date_range(start_day, periods=60, freq='B')
training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

components = {}
qqq_components = []
day_count = 0

for date in training_set_str:
    day_count = day_count + 1
    print 'loading day %d: %s' % (day_count,date)
    
    # Generate DateTimeIndex to be used for reindexing the per day DataFrame
    start_of_day = datetime.strptime(date,'%Y%m%d').replace(hour=9,minute=30)
    end_of_day = datetime.strptime(date,'%Y%m%d').replace(hour=16)
    idx = date_range(start_of_day, end_of_day, freq='Min')
    
    if dev_mode:
        ct = 0
        
    # Collect nasdaq components of the given date
    for nasdaq_100_file in glob.glob(os.path.join('data','nasdaq_100','allstocks_'+date,'*')):
        name = nasdaq_100_file.rpartition('_')[2][:-4]
        try:
            df = extract_data.start(nasdaq_100_file, date, idx)
            if len(df.index) == 0:  # discard empty training set
                print 'training set is empty'
            else:
                if not components.get(name):
                    components[name] = [df]
                else:
                    components[name].append(df)
                if dev_mode:
                    ct = ct + 1
                    if ct == 20:
                        break
        except:
            print sys.exc_info()
            print 'error in %s' % nasdaq_100_file
            
    # Collect QQQ of the given date
    for qqq_file in glob.glob(os.path.join('data','qqq_dir','allstocks_'+date,'table_qqq.csv')):
        try:
            df = extract_data.start(qqq_file, date, idx)
            qqq_components.append(df)
        except:
            print sys.exc_info()
            print 'error in %s' % nasdaq_100_file
        
nasdaq_comp = {}
for k, v in components.items():
    nasdaq_comp[k] = concat(v)
nasdaq_comp = Panel(nasdaq_comp)
nasdaq_comp_close = nasdaq_comp.ix[:,:,'% Change(close)']
print '\n\n>>> Nasdaq comp'
print nasdaq_comp

qqq = concat(qqq_components)
print '\n\n>>> QQQ'
print qqq.head()
print qqq.tail()

print '\n\n>>> Nasdaq %Change(close) for the first 10 components'
print nasdaq_comp_close.ix[1::391,:10]
#print nasdaq_comp_close.ix[-5:,:10]


lpbk = 30
# matrix to store all the variance computed by PCA
variance_matrix = extract_data.get_pca_variance(nasdaq_comp_close, training_set[:30], loopback=lpbk)

print '\n\n>>> Variance Matrix'
print variance_matrix.ix[:5,:10]

g = lambda x: x.replace(hour=9,minute=30)
gg = [g(x+BDay(lpbk-1)) for x in training_set[:30]]
ggg = nasdaq_comp.ix[:,:,'Volume'].reindex(gg)
ggg.index = [(x+BDay(lpbk-1)) for x in training_set[:30]]
ggg.reindex(variance_matrix.index)
res = ggg * variance_matrix
for i in range(len(res)):
    res.ix[i,:] = res.ix[i,:]/res.ix[i,:].sum()
print '\n\n>>> Nasdaq Liquidity (Volume * variance)'
print res.ix[:5,:10]
print res.ix[:5,10:20]
print res.ix[:5,20:30]
print res.ix[:5,30:40]
print res.ix[:5,40:50]
print res.ix[:5,50:60]
print res.ix[:5,60:70]
print res.ix[:5,70:80]
print res.ix[:5,80:90]
print res.ix[:5,90:100]
print res.ix[:5,100:110]
print res.ix[:5,110:120]
print res.ix[:5,120:130]
print res.ix[:5,130:140]

top_vars = []   # matrix of top 10 dimensions with highest variance per day
for i in range(0,len(variance_matrix)):
    row = variance_matrix.ix[i,:]
    row = row / row.sum() * 100.0
    new_index = row.index[np.argsort(row)[:-11:-1]] # descending order of variances
    row = row.reindex(index=new_index)
    top_vars_day = concat(
                        [row, row.cumsum()],
                        axis=1,
                        keys=['% variance', '% cumulative'])
    top_vars.append(top_vars_day)
    
top_vars = concat(top_vars, keys=variance_matrix.index)
print '\n\n>>> Top 10 variance per day'
print top_vars.head(10)
print top_vars.tail(10)

print '\n\n>>> Top 10 variance of day 30'
print top_vars.ix[training_set[30]].head(15)
print top_vars.ix[training_set[30]].index

pct = 85.0
result = select_model.get_top_dims(nasdaq_comp_close, top_vars, training_set[30], pct)
print '\n\n>>> Nasdaq components within %f%% cumulative variance on day 30' % pct
print result.head()
#import time

#start_time = time.time()
#tree = select_model.knn(result, 7)
#print tree
#print 'done in %fs' % (time.time() - start_time)


"""
print '\nmahalanobis distance between 1st and 2nd row is %s' % \
        select_model.mahalanobis_dist(
                nasdaq_comp_close.ix[1,:],
                nasdaq_comp_close.ix[2,:],
                nasdaq_comp_close)

print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=10,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=11,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=12,minute=0))
print select_model.is_long(nasdaq_comp['jnpr'], start_day.replace(hour=13,minute=0))
"""

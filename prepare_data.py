# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import datetime, date, timedelta
from clearwing import extract_data, select_model, utils
from pandas import *
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
start_day = sample(trading_days, 1)[0]
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
    nasdaq_comp[k] = concat(v).fillna(method='pad').fillna(method='bfill')
nasdaq_comp = Panel(nasdaq_comp)

print '\n\n>>> Nasdaq comp'
print nasdaq_comp

qqq = concat(qqq_components)
print '\n\n>>> QQQ'
print qqq.head()
print qqq.tail()

# compute for liquidity (Volume * Close)
# converted to per million units for printing
close_price_mat = nasdaq_comp.ix[:,:,'Close']
vol_mat = nasdaq_comp.ix[:,:,'Volume']
liq_mat = close_price_mat * vol_mat / 1000000
liq_mat = liq_mat.fillna(value=0)
print '\n\n>>> Nasdaq Liquidity in millions'
for i in range(0,len(liq_mat.columns),10):
    print liq_mat.ix[:5,i:i+10]

# select liquidity at the end of the each day
lpbk = 30
training_days = date_range(training_set[lpbk], training_set[-1], freq='B').shift(16, freq='H')
liq_mat_eod = liq_mat.reindex(training_days)

# collect top nasdaq components in terms of liquidity
top_dims = []
with_records = []
for i in range(0, len(liq_mat_eod)):
    try:
        row = liq_mat_eod.ix[i,:]
        new_index = row.index[np.argsort(row)[:-11:-1]]
        row = row.reindex(index=new_index)
        top_dims_day = DataFrame({'Liquidity':row})
        top_dims.append(top_dims_day)
        with_records.append(liq_mat_eod.index[i])
    except:
        print 'no record on %s, maybe a holiday' % liq_mat_eod.index[i]
top_dims = concat(top_dims, keys=with_records)
    
print '\n\n>>> Top 10 liquidity per day'
print top_dims.head(10)
print top_dims.tail(10)

print '\n\n>>> Top 10 liquidity of day %d, %s' % (lpbk, training_days[0])
print top_dims.ix[training_days[0]].head(15)
print top_dims.ix[training_days[0]].index

top10_liq = select_model.get_top_dims(liq_mat, top_dims, training_set[0], training_days[0])
print '\n\n>>> Top 10 Nasdaq components with highest liquidity on day %d' % lpbk
print top10_liq.head()
print top10_liq.tail()

# kNN
knn = select_model.KNN(top10_liq, qqq)
#print knn.estimate(top10_liq.ix[0,:], select_model.is_long)
print knn.error_score(select_model.is_long, top10_liq, k=7)
print knn.error_score(select_model.is_short, top10_liq, k=7)

# save to hdf5 format
dir_name = 'hdf_stored_data'
if not os.path.exists(dir_name):
    os.makedirs(dir_name)
store = utils.create_hdf5(dir_name+'/'+start_day.strftime('%Y%m%d'))
utils.save_object(store, nasdaq_comp, 'nasdaq_comp')
utils.save_object(store, vol_mat, 'vol_mat')
utils.save_object(store, liq_mat, 'liq_mat')
utils.save_object(store, top10_liq, 'top10_liq')
print store


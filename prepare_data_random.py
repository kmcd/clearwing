"""
usage:
python prepare_data_random.py <set_num> <lkbk>

default:
    set_num = 1
    lkbk = 3
"""
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

# parse inputs to script
set_num = 1 if not len(sys.argv) > 1 else int(sys.argv[1])
lkbk = 3 if not len(sys.argv) > 2 else int(sys.argv[1])

# Generate series of business days from QQQ earliest date to QQQ latest date
qqq_start = datetime(1999,3,10)
qqq_end = datetime(2012,7,19) - 60 * datetools.BDay()
trading_days = date_range(qqq_start, qqq_end, freq='B')

# Pick 15 days at random
training_set = sample(trading_days, 15)
training_set.sort()
training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

# save to hdf5 format
dir_name = 'data/training'
if not os.path.exists(dir_name):
    os.makedirs(dir_name)
store = utils.create_hdf5(dir_name+'/dataset_'+str(set_num))

# save dates to file
f = open(dir_name+'/dates_set_'+str(set_num)+'.txt', 'w')
for date in training_set_str:
    f.write(date+'\n')
f.close()

training_set_str = utils.gen_lkbk_days(day_list=training_set)

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
    print nasdaq_comp[k].ix[:10,:10]
nasdaq_comp = Panel(nasdaq_comp)

print '\n\n>>> Nasdaq comp'
print nasdaq_comp

qqq = concat(qqq_components)
print '\n\n>>> QQQ'
print qqq.head()
print qqq.tail()

utils.save_object(store, nasdaq_comp, 'nasdaq_comp')
utils.save_object(store, qqq, 'qqq')
print store


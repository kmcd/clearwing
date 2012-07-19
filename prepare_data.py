# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import datetime, date, timedelta
from dateutil.rrule import rrule, DAILY
from clearwing import extract_data, utils
import csv
import os
import glob
import sys



trading_days = []
for trading_day in utils.get_trading_days():
    trading_days.append(datetime.strptime(trading_day, '%Y%m%d'))

start_day = sample(trading_days, 1)[0]
end_day = start_day + timedelta(days=59)

training_set = []
for training_day in rrule(DAILY, dtstart=start_day, until=end_day):
    training_set.append(training_day.strftime('%Y%m%d'))
    
for nasdaq_100_file in glob.glob(os.path.join('data','nasdaq_100','*')):
    print '\n\nloading %s' % nasdaq_100_file
    try:
        df = extract_data.start(nasdaq_100_file, training_set)
        if len(df.index) == 0:
            print 'training set is empty'
        else:
            print 'showing first and last three rows'
            print df.head(3)
            print df.tail(3)
    except:
        print sys.exc_info()
        print 'error in %s' % nasdaq_100_file
# Save as RANDOM_DATE_training.csv


# Expand & normalise data set
# For each nasdaq 100 stock
#   - expand out h->c, o->c ...
#   - normalise each as %difference from last bar
# Write to RANDOM_DATE_training_normalised.csv

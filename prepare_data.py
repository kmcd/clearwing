# Get data by date
# Pick a day at random from data set
from random import sample
from datetime import date, timedelta
from dateutil.rrule import rrule, DAILY
import csv
import os
import glob

trading_days = []
for trading_day in rrule(DAILY, dtstart=date(1999,03,10), until=date(2011,11,01)):
    trading_days.append(trading_day)

start_day = sample(trading_days, 1)[0]
end_day = start_day + timedelta(days=59)

training_set = []
for training_day in rrule(DAILY, dtstart=start_day, until=end_day):
    training_set.append(training_day)

training_days = {}
for day in training_set:
  training_days[day] = []

foo = {'bar':[]}

for nasdaq_100_file in glob.glob('./data/nasdaq_100/*'):
    print 'loading %s' % nasdaq_100_file
    f = csv.reader(open(nasdaq_100_file))
    try:
        for row in f:
          if row[0] == '19990310':
            foo['bar'].append('baz')
          # if date in training_days.to_string
          #   append to training_days[day]
    except:
        print 'error in %s' % nasdaq_100_file
# Save as RANDOM_DATE_training.csv


# Expand & normalise data set
# For each nasdaq 100 stock
#   - expand out h->c, o->c ...
#   - normalise each as %difference from last bar
# Write to RANDOM_DATE_training_normalised.csv

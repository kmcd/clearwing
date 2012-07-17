from pandas import *
from datetime import datetime, timedelta
import numpy as np
import utils
import random
import glob

# load qqq first to identify date range
print 'loading table_qqq.csv'
qqq = read_csv('data/qqq/table_qqq.csv', names=['Date','Time','Open','High','Low','Close','Volume','A','B','C'])
qqq_df = DataFrame(qqq)
print 'done'

start_date = datetime(1999, 03, 10) #qqq earliest date
end_date = datetime(2012, 06, 07) - timedelta(days=59)  #qqq latest date
date_diff = end_date - start_date

# generate random date
sel_start_date = start_date + timedelta(days=random.randint(0,date_diff.days))
sel_end_date = sel_start_date + timedelta(days=59)

rep_start_date = utils.MDYtoYMD(sel_start_date)
rep_end_date = utils.MDYtoYMD(sel_end_date)

print '\n\ndates selected: from %s to %s\n' % (sel_start_date, sel_end_date)

def extract_training_set(df, start_date, end_date):
    """
    Given a DataFrame df, a start_date and an end_date
    Returns a portion of df that is between the given dates (inclusive)
        and between the times 9:30 and 16:00
    """
    which = []
    for row_index, row in df.iterrows():
        date = row['Date']
        time = row['Time']
        if date >= rep_start_date and date <= rep_end_date:
            if time >= 930 and time <= 1600:
                which.append(row_index)

    df_sel = df.ix[which]
    return df_sel

for nasdaq_100_file in glob.glob('./data/nasdaq_100/*'):
    try:
        print 'loading %s' % nasdaq_100_file
        nasdaq_file = read_csv(nasdaq_100_file, names=['Date','Time','Open','High','Low','Close','Volume','A','B','C'])
        print 'done'
        sel_df = extract_training_set(nasdaq_file, sel_start_date, sel_end_date)
        print sel_df[:5]
    except:
        print 'there was a problem in parsing this file: %s' % nasdaq_100_file
    



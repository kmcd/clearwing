from pandas import *
from datetime import datetime, timedelta
from cStringIO import StringIO
import random
import glob
import dateutil
import time

COLUMN_NAMES = ['Date','Time','Open','High','Low','Close','Volume','Splits','Earnings','Dividends']
VALID_TIMES = [str(x) for x in range(930,1601)]

def date_time_merger(row):
    """
    custom datetime parser function
    dateutil.parser does not recognize time without separators for hours and minutes
    """
    row = row[:-2] + ':' + row[-2:]
    return dateutil.parser.parse(row)


def start(filepath = 'data/qqq/table_qqq.csv', dates=[]):
    """
    this function performs extraction of data as described in clearwing/wiki
    
    Optimization:
        Read and filter data first as text
        Then convert to DataFrame
    """
    
    start_time = time.time() # used only for measuring elapsed time

    s = StringIO()
    with open(filepath) as f:
        for line in f:
            vals = line.split(',')
            if len(vals) != 10:
                continue
            if vals[0] in dates and vals[1] in VALID_TIMES:
                s.write(line)
    s.seek(0)
    if not s.getvalue():
        return DataFrame()
    data = read_csv(
                s,
                names=COLUMN_NAMES,
                sep=',',
                parse_dates=[('Date','Time')],
                date_parser=date_time_merger,
           )

    print 'done in %fs' % (time.time() - start_time)
    return data


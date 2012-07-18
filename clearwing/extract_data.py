from pandas import *
from datetime import datetime, timedelta
import numpy as np
import utils
import random
import glob
import dateutil
import time

COLUMN_NAMES = ['Date','Time','Open','High','Low','Close','Volume','Splits','Earnings','Dividends']

def date_time_merger(row):
    """
    custom datetime parser function
    dateutil.parser does not recognize time without separators for hours and minutes
    """
    row = row[:-2] + ':' + row[-2:]
    return dateutil.parser.parse(row)


def start():
    """
    this function performs extraction of data as described in clearwing/wiki
    
    Note: as of this writing, this function is used for testing speed of parsing data
    """
    start_time = time.time()
    print __file__
    print 'loading table_qqq.csv %s' % start_time
    chunker = read_csv(
                    'data/qqq/table_qqq.csv',
                    names=COLUMN_NAMES,
                    sep=',',
                    # uncomment next line to enable parsing of date only
                    parse_dates=[('Date')],
                    
                    # uncomment next two lines to enable parsing of both date and time
                    #parse_dates=[('Date','Time')],
                    #date_parser=date_time_merger,
                    
                    # uncomment next line to enable 'by chunk' parsing
                    chunksize=100000,
                    )
                   
    # uncomment next two lines if 'by chunk' is enabled
    for chunk in chunker:
        chunk.head()
    print 'done'
    
    print time.time() - start_time



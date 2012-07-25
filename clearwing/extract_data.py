from pandas import *
from datetime import datetime, timedelta
from cStringIO import StringIO
from matplotlib import mlab
from pandas.tseries.offsets import BDay
import dateutil
import time

COLUMN_NAMES = ['Date','Time','Open','High','Low','Close','Volume','Splits','Earnings','Dividends']
VALID_TIMES = range(930,960)
for x in range(1000,1600,100):
    for y in range(0,60):
        VALID_TIMES = VALID_TIMES + [x+y]
VALID_TIMES = VALID_TIMES + [1600]
VALID_TIMES = [str(x) for x in VALID_TIMES]

def date_time_merger(row):
    """
    custom datetime parser function
    dateutil.parser does not recognize time without separators for hours and minutes
    """
    row = row[:-2] + ':' + row[-2:]
    return dateutil.parser.parse(row)
    
def get_pca_variance(df, dates, loopback=30):
    """
    computes the variance of each dimension per date (with 30 days loopback)
    """
    result = {}
    for day in dates:
        end_day = day+BDay(loopback-1)
        sd = mlab.PCA(df.ix[day:end_day]).sigma
        variance = [x**2 for x in sd]
        result[end_day] = Series(variance, index=df.columns)
    return DataFrame.from_dict(result, orient='index')

def start(filepath, date, idx):
    """

    """
    data = read_csv(
                filepath,
                names=COLUMN_NAMES,
                sep=',',
                parse_dates=[('Date','Time')],
                date_parser=date_time_merger,
           )
    data = data.ix[:,['Date_Time','Open','High','Low','Close','Volume']]
    data = data.set_index('Date_Time').reindex(idx, method='pad').dropna()
    data['% Change(close)'] = data['Close'].pct_change().fillna(value=0)
    return data


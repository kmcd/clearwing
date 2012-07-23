from pandas import *
from datetime import datetime, timedelta
from cStringIO import StringIO
from matplotlib import mlab
from pandas.tseries.offsets import BDay
from pandas import *
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
    
def get_pca_variance(df, dates):
    """
    computes the variance of each dimension per date (with 30 days loopback)
    """
    result = {}
    for day in dates:
        end_day = day+BDay(29)
        sd = mlab.PCA(df.ix[day:end_day]).sigma
        variance = [x**2 for x in sd]
        result[end_day] = Series(variance, index=df.columns)
    return DataFrame.from_dict(result, orient='index')

def start(filepath = 'data/qqq/table_qqq.csv', dates=[]):
    """
    this function performs extraction of data and conversion from stock prices
    to returns
    
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
    data = data.set_index('Date_Time')
    
    # fill-in missing rows per minute of each day from 09:30 to 16:00
    slices = []
    for date in dates:
        start_date = datetime.strptime(date,'%Y%m%d').replace(hour=9, minute=30)
        data_slice = data.reindex(
                        date_range(
                            start_date, 
                            start_date + timedelta(hours=6, minutes=30),
                            freq='Min',),
                        method='pad',
                     )
        slices = slices + [data_slice]
    print 'done in %fs' % (time.time() - start_time)
    return concat(slices).fillna(value=0)
    #return concat(slices).pct_change().fillna(value=0)


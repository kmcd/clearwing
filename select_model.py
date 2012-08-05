"""
usage:
python select_model.py <date>

where:
<date> is the start_day from prepare_data.py with format YYYYMMDD
"""
from random import sample
from clearwing import select_model, utils
from datetime import datetime
from pandas import *
import sys, time

dir_name = 'data/training'
start_day_str = sys.argv[1]
store = HDFStore(dir_name+'/'+start_day_str+'.h5')

nasdaq_comp = store['nasdaq_comp']
qqq = store['qqq']

qqq_long = {}
qqq_short = {}
for i in range(len(qqq)):
    t = qqq.index[i]
    if t.hour == 16:
        continue
    qqq_long[t] = select_model.is_long(qqq, t)
    qqq_short[t] = select_model.is_short(qqq, t)
    
qqq['is_long'] = Series(qqq_long)
qqq['is_short'] = Series(qqq_short)
qqq['is_long'].fillna(value=False)
qqq['is_short'].fillna(value=False)
print qqq.head()

start_day = datetime.strptime(start_day_str, '%Y%m%d')
training_set = date_range(start_day, periods=60, freq='B')
training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

# compute for liquidity (Volume * Close)
# converted to per million units for printing
close_price_mat = nasdaq_comp.ix[:,:,'Close']
vol_mat = nasdaq_comp.ix[:,:,'Volume']
liq_mat = close_price_mat * vol_mat / 1000000
liq_mat = liq_mat.fillna(value=0)
print '\n\n>>> Nasdaq Liquidity in millions'
for i in range(0,len(liq_mat.columns),10):
    print liq_mat.ix[:5,i:i+10]

utils.save_object(store, vol_mat, 'vol_mat')
utils.save_object(store, liq_mat, 'liq_mat')

lkbk = 3
ntop = 10

def set_start_time(date):
    return date.shift(9, freq='H').shift(30, freq='Min')
    
# select liquidity at the end of the each day
training_days = set_start_time(date_range(training_set[lkbk], training_set[-1], freq='B'))
liq_at_start = liq_mat.reindex(set_start_time(training_set))

print '\n\nliq_at_start'
print liq_at_start.ix[:5,:5]

_all = {}
for end_day in training_days:
    print 'processing %s' % end_day
    try:
        start_day = end_day - (lkbk-1) * datetools.BDay()
        lkbk_days = date_range(start_day, end_day, freq='B')
             
        row = liq_at_start.ix[start_day,:]
        row = row[np.argsort(row)[::-1]]
        topn_nasdaq = row.index[:ntop]
        
        pct_close = DataFrame()
        for lkbk_day in lkbk_days:
            mins = date_range(lkbk_day, lkbk_day.replace(hour=16, minute=0), freq='Min')
            pct_close = concat([pct_close, nasdaq_comp.ix[topn_nasdaq, mins, '% Change(close)']])
        
        pct_liq_min = DataFrame()
        for lkbk_day in lkbk_days:
            mins = date_range(lkbk_day, lkbk_day.replace(hour=16, minute=0), freq='Min')
            pct_liq_min = concat([pct_liq_min, liq_mat.ix[mins,topn_nasdaq]])
            pct_liq_min = pct_liq_min.apply(lambda x : x / x.sum() * 100.0, axis=1)
            
        pct_liq_day = (row / row.sum() * 100.0)
        pct_liq_day = pct_liq_day.ix[topn_nasdaq]
        pct_liq_day = DataFrame({start_day:pct_liq_day})
        pct_liq_day = pct_liq_day.T.reindex(index=pct_close.index, method='pad')
        
        _all[end_day] = Panel({'% Change(close)':pct_close,
                               '% Liquidity 1min':pct_liq_min,
                               '% Liquidity 3day':pct_liq_day,})
    except:
        print sys.exc_info()
        #print "no record found, maybe a holiday"
print _all[training_days[0]].ix[['% Change(close)','% Liquidity 1min','% Liquidity 3day'],training_days[0],:].head()


















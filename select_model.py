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

def set_start_time(dates):
    """
    set all date's time to 9:30
    """
    return dates.shift(9, freq='H').shift(30, freq='Min')
    
def day_time_range(date):
    """
    generate range of time from 9:30 to 16:00 for the given date
    """
    return date_range(date.replace(hour=9,minute=30),
                      date.replace(hour=16,minute=0),
                      freq='Min')

lkbk = 5
ntop = 20
    
training_days = set_start_time(training_set[lkbk:])

today_data_all = {}
lkbk_days_data_all = {}
multiplier = {}

for today in training_days:
    print 'processing %s' % today
    try:
        end_day = today - datetools.BDay()
        start_day = end_day - (lkbk-1) * datetools.BDay()
        
        today_time_range = day_time_range(today)
        
        lkbk_days = date_range(start_day, end_day, freq='B')
        lkbk_days_range = None
        for lkbk_day in lkbk_days:
            mins = day_time_range(lkbk_day)
            if lkbk_days_range is None:
                lkbk_days_range = mins
            else:
                lkbk_days_range = lkbk_days_range.append(mins)
            
        vols = nasdaq_comp.ix[:, lkbk_days_range, 'Volume'].sum()
        closes = nasdaq_comp.ix[:, lkbk_days_range, 'Close'].mean()
        liqs = vols * closes
        liqs = liqs / liqs.sum()
        
        liqs = liqs[np.argsort(liqs)[::-1]]
        topn_nasdaq = liqs.index[:ntop]
        
        multiplier[today] = liqs[topn_nasdaq]
        
        pct_close = nasdaq_comp.ix[topn_nasdaq, today_time_range, '% Change(close)']
        pct_close = pct_close * multiplier[today]
        pct_liq_min = liq_mat.ix[today_time_range, topn_nasdaq].pct_change().fillna(0)
        pct_liq_min = pct_liq_min * multiplier[today]
        today_data_all[today] = Panel({
                                '% Change(close)':pct_close,
                                '% Change(liquidity)':pct_liq_min,}).transpose(2,0,1)
        
        pct_close = nasdaq_comp.ix[topn_nasdaq, lkbk_days_range, '% Change(close)']
        pct_close = pct_close * multiplier[today]
        pct_liq_min = liq_mat.ix[lkbk_days_range, topn_nasdaq].pct_change().fillna(0)
        pct_liq_min = pct_liq_min * multiplier[today]
        lkbk_days_data_all[today] = Panel({
                                    '% Change(close)':pct_close,
                                    '% Change(liquidity)':pct_liq_min,}).transpose(2,0,1)
                                    
    except:
        print sys.exc_info()
        print "no record found, maybe a holiday"


chosen_date = training_days[0]

today_data = today_data_all[chosen_date]
lkbk_days_data = lkbk_days_data_all[chosen_date]

close_name = '% Change(close)'
liq_name = '% Change(liquidity)'

today_close = today_data.ix[:,close_name,:]
today_liq = today_data.ix[:,liq_name,:]
today_close.columns = [(x + '_close') for x in today_close.columns]
today_liq.columns = [(x + '_liq') for x in today_liq.columns]

lkbk_close = lkbk_days_data.ix[:,close_name,:]
lkbk_liq = lkbk_days_data.ix[:,liq_name,:]
lkbk_close.columns = [(x + '_close') for x in lkbk_close.columns]
lkbk_liq.columns = [(x + '_liq') for x in lkbk_liq.columns]

print '%dclose and %dliq'
test_set = concat([today_close, today_liq], axis=1)
train_set = concat([lkbk_close, lkbk_liq], axis=1)
# kNN
knn = select_model.KNN(train_set, qqq)
for k in [1,2,3,4,5,6,7]:
    st = time.time()
    error = knn.error_score(test_set, k)
    print '(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, chosen_date)

print '%dclose'
test_set = today_close #concat([today_close, today_liq], axis=1)
train_set = lkbk_close #concat([lkbk_close, lkbk_liq], axis=1)
# kNN
knn = select_model.KNN(train_set, qqq)
for k in [1,2,3,4,5,6,7]:
    st = time.time()
    error = knn.error_score(test_set, k)
    print '(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, chosen_date)

print '%dliq'
test_set = today_liq #concat([today_close, today_liq], axis=1)
train_set = lkbk_liq #concat([lkbk_close, lkbk_liq], axis=1)
# kNN
knn = select_model.KNN(train_set, qqq)
for k in [1,2,3,4,5,6,7]:
    st = time.time()
    error = knn.error_score(test_set, k)
    print '(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, chosen_date)















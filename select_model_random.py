"""
usage:
python select_model.py <set_num>
"""
from random import sample, shuffle
from clearwing import select_model, utils
from datetime import datetime
from pandas import *
import sys, time

dir_name = 'data/training'
set_num = sys.argv[1]
store = HDFStore(dir_name+'/dataset_'+set_num+'.h5')

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

f = open(dir_name+'/dates_set_'+set_num+'.txt')
training_set_str = [line[:-1] for line in f]
training_set = [datetime.strptime(x, '%Y%m%d').replace(hour=9, minute=30) for x in training_set_str]

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

lkbk = 3
ntop = 10

today_data_all = {}
multiplier = {}

for i in range(len(training_set)):
    today = training_set[i]
    print 'processing %s' % today
    try:
        today_time_range = day_time_range(today)
            
        vols = nasdaq_comp.ix[:, today, 'Volume']
        closes = nasdaq_comp.ix[:, today, 'Close']
        liqs = vols * closes
        liqs = liqs / liqs.sum()
        liqs = liqs.fillna(0)
        
        liqs = liqs[np.argsort(liqs)[::-1]]
        topn_nasdaq = liqs.index[:]
        
        multiplier[today] = liqs[topn_nasdaq]
        
        pct_close = nasdaq_comp.ix[topn_nasdaq, today_time_range, '% Change(close)']
        pct_close = pct_close * multiplier[today]
        pct_liq_min = liq_mat.ix[today_time_range, topn_nasdaq].pct_change().fillna(0)
        pct_liq_min = pct_liq_min * multiplier[today]
        today_data_all[today] = Panel({
                                '% Change(close)':pct_close,
                                '% Change(liquidity)':pct_liq_min,}).transpose(2,0,1)
                                    
    except:
        print sys.exc_info()
        print "no record found, maybe a holiday"

# backup file of console prints, just in case
f = open(dir_name+'/error_mahalanobis_'+set_num+'.txt', 'w')

def _print(f, string):
    print string
    f.write(string+'\n')
                
sum_err = {}
ct = 0
for k in range(11): sum_err[k] = 0
for i in range(15):
    try:
        _set = range((lkbk+1)*i,(lkbk+1)*(i+1)+1)
        today = training_set[_set[lkbk]]
        lkbk_days = [training_set[x] for x in _set[:lkbk]]
        today_data = today_data_all[today].ix[:10,:,:]
        
        lkbk_days_data = [today_data_all[x] for x in lkbk_days]
        lkbk_days_data = concat(lkbk_days_data, axis=2)
        lkbk_days_data = lkbk_days_data.ix[today_data.items,:,:]

        _print(f, '\n\n>>>>>>>>>>> set %d' % i )
        _print(f, 'today = %s' % today )
        _print(f, 'lkbk_days = %s' % [x.year for x in lkbk_days] )
        _print(f, 'top10 nasdaq components : %s' % today_data.items )

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
        
        day_err = {}
        test_set = today_close #concat([today_close, today_liq], axis=1)
        train_set = lkbk_close #concat([lkbk_close, lkbk_liq], axis=1)
        # kNN
        knn = select_model.KNN(train_set, qqq)
        for k in [5,6,7]:
            st = time.time()
            error = knn.error_score(test_set, k)
            day_err[k] = error
            sum_err[k] += error
            _print(f,'(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, today) )
        _print(f,'%s' % day_err)
        ct += 1
    except:
        print sys.exc_info()

_print(f, sum_err)
for k in range(8):
    _print(f,'%f' % (sum_err[k] / ct))
f.close() 


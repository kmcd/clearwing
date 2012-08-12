"""
usage:
python select_model_random.py <set_num>
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
vol_mat = store['vol_mat']
liq_mat = store['liq_mat']

f = open(dir_name+'/dates_set_'+set_num+'.txt')
training_set_str = [line[:-1] for line in f]
training_set = [datetime.strptime(x, '%Y%m%d').replace(hour=9, minute=30) for x in training_set_str]

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
lkbk_days_data_all = {}
multiplier = {}

for i in range(len(training_set)):
    today = training_set[i]
    lkbk_days = utils.gen_lkbk_days(today=today)
    lkbk_days = [datetime.strptime(x,'%Y%m%d') for x in lkbk_days]
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
                                
        lkbk_days_range = None
        for lkbk_day in lkbk_days:
            mins = day_time_range(lkbk_day)
            if lkbk_days_range is None:
                lkbk_days_range = mins
            else:
                lkbk_days_range = lkbk_days_range.append(mins)
                
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

# backup file of console prints, just in case
f = open(dir_name+'/error_euclidean_'+set_num+'.txt', 'w')

sum_err = {}
ct = 0
for k in range(11): sum_err[k] = 0

for i in range(len(training_set)):
    try:
        today = training_set[i]
        today_data = today_data_all[today].ix[:10,:,:]
        lkbk_days_data = lkbk_days_data_all[today].ix[today_data.items,:,:]
        
        utils._print(f, '\n\n>>>>>>>>>>> set %d' % (i+1) )
        utils._print(f, 'today = %s' % today )
        utils._print(f, 'top10 nasdaq components : %s' % today_data.items )

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
        for k in range(5,10):
            st = time.time()
            error = knn.error_score(test_set, k)
            day_err[k] = error
            sum_err[k] += error
            utils._print(f,'(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, today) )
        utils._print(f,'%s' % day_err)
        ct += 1
    except:
        print sys.exc_info()

utils._print(f, str(sum_err))
for k in range(5,10):
    utils._print(f,'%f' % (sum_err[k] / ct))
f.close() 


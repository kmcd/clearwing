"""
for usage, see:
    python select_model.py -h
"""
from random import sample
from clearwing import select_model, utils
from datetime import datetime
from pandas import *
import sys, time
import argparse

# parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-d','--dir', dest='dir_name', default='data/training', help='directory to save datasets, reports')
parser.add_argument('-r','--random', dest='is_random', type=bool, const=True, 
                                default=False, nargs='?', help='generate random days or consecutive days')
parser.add_argument('-l','--lkbk', type=int, default=3, help='number of lookback days')
parser.add_argument('-nt','--ntop', type=int, default=10, help='number of top nasdaq components to use')
parser.add_argument('-sn','--setn', dest='set_num', type=int, default=1, help='data set number, required if random')
parser.add_argument('-sd','--sday', dest='start_day', help='chosen start date, required if not random')
parser.add_argument('-nd','--ndays', type=int, default=15, help='number of random days to generate')
parser.add_argument('-k', dest='k_range', type=int, nargs='*', default=[5,6,7,8,9], help='k values')

args = parser.parse_args()
lkbk = args.lkbk
ntop = args.ntop

# open .h5, fetch stored values from prepare_data
if args.is_random:
    store = HDFStore(args.dir_name+'/dataset_'+str(args.set_num)+'.h5')
else:
    start_day_str = args.start_day
    store = HDFStore(args.dir_name+'/'+start_day_str+'.h5')
nasdaq_comp = store['nasdaq_comp']
qqq = store['qqq']
vol_mat = store['vol_mat']
liq_mat = store['liq_mat']

# fetch/generate dates
if args.is_random:
    f = open(args.dir_name+'/dates_set_'+str(args.set_num)+'.txt')
    training_set_str = [line[:-1] for line in f]
    training_set = [datetime.strptime(x, '%Y%m%d').replace(hour=9, minute=30) for x in training_set_str]
else:
    start_day = datetime.strptime(start_day_str, '%Y%m%d')
    training_set = date_range(start_day, periods=args.ndays*(lkbk+1), freq='B')
    training_set = training_set[lkbk::lkbk+1]
    training_set_str = [date.date().strftime('%Y%m%d') for date in training_set]
    training_set = utils.set_start_time(training_set)

# file to backup console prints
if args.is_random:
    log_file = open(args.dir_name+'/error_rates_'+str(args.set_num)+'.txt', 'w')
else:
    log_file = open(args.dir_name+'/error_rates_'+start_day_str+'.txt', 'w')


today_data_all = {}
lkbk_days_data_all = {}
multiplier = {}

for i in range(len(training_set)):
    today = training_set[i]
    lkbk_days = utils.gen_lkbk_days(today=today)
    lkbk_days = [datetime.strptime(x,'%Y%m%d') for x in lkbk_days]
    utils._print(log_file,  'processing %s' % today)
    try:
        today_time_range = utils.day_time_range(today)
            
        vols = nasdaq_comp.ix[:, today, 'Volume']
        closes = nasdaq_comp.ix[:, today, 'Close']
        liqs = vols * closes
        liqs = liqs / liqs.sum()
        liqs = liqs.fillna(0)
        
        # get top nasdaq components by liquidity
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
            mins = utils.day_time_range(lkbk_day)
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
        utils._print(log_file, sys.exc_info())
        utils._print(log_file, "no record found, maybe a holiday")

error_rates = {}
for k in args.k_range: error_rates[k] = []

for i in range(len(training_set)):
    try:
        today = training_set[i]
        today_data = today_data_all[today].ix[:10,:,:]
        lkbk_days_data = lkbk_days_data_all[today].ix[today_data.items,:,:]
        
        utils._print(log_file, '\n\n>>>>>>>>>>> set %d' % (i+1) )
        utils._print(log_file, 'today = %s' % today )
        utils._print(log_file, 'top10 nasdaq components : %s' % today_data.items )

        close_name = '% Change(close)'
        liq_name = '% Change(liquidity)'

        # filter train and test data
        today_close = today_data.ix[:,close_name,:]
        today_liq = today_data.ix[:,liq_name,:]
        lkbk_close = lkbk_days_data.ix[:,close_name,:]
        lkbk_liq = lkbk_days_data.ix[:,liq_name,:]
        
        # fix column names, for merging/concat
        today_close.columns = [(x + '_close') for x in today_close.columns]
        today_liq.columns = [(x + '_liq') for x in today_liq.columns]
        lkbk_close.columns = [(x + '_close') for x in lkbk_close.columns]
        lkbk_liq.columns = [(x + '_liq') for x in lkbk_liq.columns]
        
        test_set = today_close
        train_set = lkbk_close
        
        # kNN
        knn = select_model.KNN(train_set, qqq)
        for k in args.k_range:
            st = time.time()
            error = knn.error_score(test_set, k)
            error_rates[k].append(error)
            utils._print(log_file,'(ntop=%d, lkbk=%d, k=%d) error = %.2f%% (time=%.2fs) date = %s' % (ntop, lkbk, k, error, time.time()-st, today) )
    except:
        print sys.exc_info()

error_rates = DataFrame(error_rates)
utils._print(log_file, '\nerror_rates:')
utils._print(log_file, error_rates)
utils._print(log_file, '\nerror_mean:')
utils._print(log_file, error_rates.apply(np.mean))
utils._print(log_file, '\nerror_std:')
utils._print(log_file, error_rates.apply(np.std))




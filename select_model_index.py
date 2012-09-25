"""
for usage, see:
    python select_model_index.py -h
"""
from random import sample
from clearwing import select_model, utils
from datetime import datetime
from pandas import *
import sys, time
import argparse

# parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('dataset', help='the dataset to use')
parser.add_argument('-k', dest='k_range', type=int, nargs='*', default=[5,6,7,8,9], help='k values')
parser.add_argument('-nd','--ndays', type=int, default=20, help='number of random days to be selected')
parser.add_argument('-it','--iters', type=int, default=4, help='number of iterations')
parser.add_argument('-cin','--inputCompositeIndexName', dest='input_composite_index_name', required=True, help='this is input component index name')
parser.add_argument('-cta','--targetCompositeIndexName', dest='target_composite_index_name', required=True, help='this is target component index name')
parser.add_argument('-i','--input', dest='in_dir', default='data/training', help='directory to retrieve dataset from')
parser.add_argument('-o','--output', dest='out_dir', default='data/training', help='directory to store text reports')
parser.add_argument('-l','--lkbk', type=int, default=3, help='number of lookback days')
parser.add_argument('-nt','--ntop', type=int, default=10, help='number of top nasdaq components to use')

args = parser.parse_args()
lkbk = args.lkbk
ntop = args.ntop

inputComponentIndexName = args.input_composite_index_name.lower()
targetComponentIndexName = args.target_composite_index_name.lower()

# open .h5, fetch stored values from prepare_data
store = HDFStore(args.in_dir+'/'+args.dataset)
input_comp = store[ inputComponentIndexName ]
target = store[ targetComponentIndexName ]
vol_mat = store['vol_mat']
liq_mat = store['liq_mat']

# fetch/generate dates
f = open(args.in_dir+'/datelist_'+args.dataset[:-3]+'.txt')
training_set_str = [line[:-1] for line in f]
training_set = [datetime.strptime(x, '%Y%m%d').replace(hour=9, minute=30) for x in training_set_str]

# file to backup console prints
log_file = open(args.out_dir+'/log_'+args.dataset[:-3]+'.txt', 'w')

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
            
        vols = input_comp.ix[:, today, 'Volume']
        closes = input_comp.ix[:, today, 'Close']
        liqs = vols * closes
        liqs = liqs / liqs.sum()
        liqs = liqs.fillna(0)
        
        # get top input components by liquidity
        liqs = liqs[np.argsort(liqs)[::-1]]
        topn_input = liqs.index[:]
        
        multiplier[today] = liqs[topn_input]
        
        # Consolidate today data into one Panel object
        pct_close = input_comp.ix[topn_input, today_time_range, '% Change(close)']
        pct_close = pct_close * multiplier[today]
        pct_liq_min = liq_mat.ix[today_time_range, topn_input].pct_change().fillna(0)
        pct_liq_min = pct_liq_min * multiplier[today]
        today_data_all[today] = Panel({
                                '% Change(close)':pct_close,
                                '% Change(liquidity)':pct_liq_min,}).transpose(2,0,1)
                                
        # generate DateTimeIndex for lkbk_days (9:30 to 16:00 for 3 days)
        lkbk_days_range = None
        for lkbk_day in lkbk_days:
            mins = utils.day_time_range(lkbk_day)
            if lkbk_days_range is None:
                lkbk_days_range = mins
            else:
                lkbk_days_range = lkbk_days_range.append(mins)
                
        # Consolidate lkbk_days data into one Panel object
        pct_close = input_comp.ix[topn_input, lkbk_days_range, '% Change(close)']
        pct_close = pct_close * multiplier[today]
        pct_liq_min = liq_mat.ix[lkbk_days_range, topn_input].pct_change().fillna(0)
        pct_liq_min = pct_liq_min * multiplier[today]
        lkbk_days_data_all[today] = Panel({
                                    '% Change(close)':pct_close,
                                    '% Change(liquidity)':pct_liq_min,}).transpose(2,0,1)
                                    
    except:
        utils._print(log_file, sys.exc_info())
        utils._print(log_file, "no record found, maybe a holiday")

error_rates = {}
for k in args.k_range: error_rates[k] = []

for j in range(args.iters):
    print 'iter %d' % (j+1)
    for today in sample(training_set, args.ndays):
        try:
            # get slice of data for 'today'
            today_data = today_data_all[today].ix[:10,:,:]
            lkbk_days_data = lkbk_days_data_all[today].ix[today_data.items,:,:]
            
            utils._print(log_file, 'today = %s' % today )
            utils._print(log_file, 'top10 input components : %s' % today_data.items )

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
            knn = select_model.KNN(train_set, target)
            error = knn.error_score_k_range(test_set, args.k_range)
            for k in args.k_range:
                error_rates[k].append(error[k])
                
            utils._print(log_file, error)
        except:
            print sys.exc_info()

error_rates = DataFrame(error_rates)

utils._print(log_file, 'dataset: %s' % args.dataset)
utils._print(log_file, 'k range: %s' % args.k_range)
utils._print(log_file, 'iterations: %s' % args.iters)
utils._print(log_file, 'days: %s' % args.ndays)
utils._print(log_file, DataFrame(error_rates.std(), columns=['std dev error:']).T)
utils._print(log_file, DataFrame(error_rates.mean(), columns=['avg error:']).T)






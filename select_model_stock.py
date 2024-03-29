"""
for usage, see:
    python select_model_stock.py -h
"""
from random import sample
from clearwing import select_model, utils, numpredict, optimization
from datetime import datetime
from pandas import *
import sys, time
import argparse

# parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-d','--dataset', help='the dataset to use')
parser.add_argument('-k', dest='k_range', type=int, nargs='*', default=[5,6,7,8,9], help='k values')
parser.add_argument('-nd','--ndays', type=int, default=20, help='number of random days to be selected')
parser.add_argument('-it','--iters', type=int, default=4, help='number of iterations')
parser.add_argument('-p','--primary', required=True, help='primary stock (required)')
parser.add_argument('-s','--secondary', nargs='*', help='list of secondary stocks (optional). Stocs are separated with space.')
parser.add_argument('-r','--range', type=int, default=[-0.03,0.03], nargs=2, help='long/short boundaries')
parser.add_argument('--annealingoff', type=bool, const=True, 
                                default=False, nargs='?', help='switch to turn off annealing')
parser.add_argument('-cin','--inputCompositeIndexName', dest='input_composite_index_name', required=True, help='this is input component index name')
parser.add_argument('-cta','--targetCompositeIndexName', dest='target_composite_index_name', required=True, help='this is target component index name')
parser.add_argument('-ss','--stepsize', type=float, default=0.1, help='number of lookback days')
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

# get slice of stocks to be used
stock_primary = target if args.primary.lower() == targetComponentIndexName else input_comp[args.primary.lower()]
list_stock_secondary = []
if args.secondary is not None:
    for stock in args.secondary:
        list_stock_secondary.append( target ) if stock.lower() == targetComponentIndexName else list_stock_secondary.append( input_comp[ stock.lower() ] )

def add_dimensions(stock):
    if stock is not None:
        stock['% Change(open)'] = stock['Open'].pct_change().fillna(0)
        stock['% Change(high)'] = stock['High'].pct_change().fillna(0)
        stock['% Change(low)'] = stock['Low'].pct_change().fillna(0)
        stock['% Change(close)'] = stock['Close'].pct_change().fillna(0)
        stock['% Change(volume)'] = stock['Volume'].pct_change().fillna(0)
        
        def pct_change_dim(stock, first, second):
            return (stock[first] - stock[second]) / stock[second]
        stock['% Change(open-low,curr)'] = pct_change_dim(stock, 'Open', 'Low')
        stock['% Change(open-close,curr)'] = pct_change_dim(stock, 'Open', 'Close')
        stock['% Change(open-high,curr)'] = pct_change_dim(stock, 'Open', 'High')
        stock['% Change(low-close,curr)'] = pct_change_dim(stock, 'Low', 'Close')
        stock['% Change(low-high,curr)'] = pct_change_dim(stock, 'Low', 'High')
        stock['% Change(close-high,curr)'] = pct_change_dim(stock, 'Close', 'High')
    
        stock['% Change(open-low,prev)'] =  stock['% Change(open-low,curr)'].shift().fillna(0)
        stock['% Change(open-close,prev)'] =  stock['% Change(open-close,curr)'].shift().fillna(0)
        stock['% Change(open-high,prev)'] =  stock['% Change(open-high,curr)'].shift().fillna(0)
        stock['% Change(low-close,prev)'] =  stock['% Change(low-close,curr)'].shift().fillna(0)
        stock['% Change(low-high,prev)'] =  stock['% Change(low-high,curr)'].shift().fillna(0)
        stock['% Change(close-high,prev)'] =  stock['% Change(close-high,curr)'].shift().fillna(0)
        
        stock_long = {}
        stock_short = {}
        for i in range(len(stock)):
            t = stock.index[i]
            if t.hour == 16:
                continue
            stock_long[t] = select_model.is_long(stock, t, args.range)
            stock_short[t] = select_model.is_short(stock, t, args.range)
        stock['is_long'] = Series(stock_long)
        stock['is_short'] = Series(stock_short)
        stock['is_long'] = stock['is_long'].fillna(value=False)
        stock['is_short'] = stock['is_short'].fillna(value=False)
        
        stock = stock.ix[:,['% Change(open)',
                            '% Change(high)',
                            '% Change(low)',
                            '% Change(close)',
                            '% Change(volume)',
                            '% Change(open-low,curr)',
                            '% Change(open-close,curr)',
                            '% Change(open-high,curr)',
                            '% Change(low-close,curr)',
                            '% Change(low-high,curr)',
                            '% Change(close-high,curr)',
                            '% Change(open-low,prev)',
                            '% Change(open-close,prev)',
                            '% Change(open-high,prev)',
                            '% Change(low-close,prev)',
                            '% Change(low-high,prev)',
                            '% Change(close-high,prev)',
                            'is_long',
                            'is_short',]]
        return stock
        
stock_primary = add_dimensions(stock_primary)
for stock in list_stock_secondary:
    index = 0
    stock = add_dimensions(stock)

    if stock is not None:
        # append dimensions of secondary stock to the primary stock, not including the long and short classifiers' columns
        cols = [args.primary+' '+x for x in stock_primary.columns[:-2]]
        cols.append('is_long')
        cols.append('is_short')
        stock_primary.columns = cols
        stock.columns = [ args.secondary[ index ]+' '+x for x in stock.columns]
        index = index + 1
    stock_primary = concat([stock_primary.ix[:,:-2],stock.ix[:,:-2],stock_primary.ix[:,-2:]], axis=1)
    
# file to backup console prints
log_file = open(args.out_dir+'/log_'+args.dataset[:-3]+'.txt', 'w')

today_data_all = {}
lkbk_days_data_all = {}

for i in range(len(training_set)):
    today = training_set[i]
    lkbk_days = utils.gen_lkbk_days(today=today)
    lkbk_days = [datetime.strptime(x,'%Y%m%d') for x in lkbk_days]
    utils._print(log_file,  'processing %s' % today)
    try:
        # generate date/time ranges for 'today'
        today_time_range = utils.day_time_range(today)
        lkbk_days_range = None
        for lkbk_day in lkbk_days:
            mins = utils.day_time_range(lkbk_day)
            if lkbk_days_range is None:
                lkbk_days_range = mins
            else:
                lkbk_days_range = lkbk_days_range.append(mins)
                    
        # get slice of data for 'today'
        today_data_all[today] = stock_primary.ix[today_time_range,:]
        lkbk_days_data_all[today] = stock_primary.ix[lkbk_days_range,:]
                                    
    except:
        utils._print(log_file, sys.exc_info())
        utils._print(log_file, "no record found, maybe a holiday")


# CalculateWeights
cw = select_model.CalculateWeights(today_data_all, lkbk_days_data_all)
if args.annealingoff:
    error_rates_df = DataFrame()
    for i in range(args.iters):
        error_rates = cw.crossvalidate_k_range(args.ndays, [0.1]*(len(stock_primary.columns)-2), training_set, args.k_range)
        error_rates_df = concat([error_rates_df, DataFrame(error_rates)])
    
    utils._print(log_file, '\n\n')
    utils._print(log_file, 'dataset: %s' % args.dataset)
    utils._print(log_file, 'k range: %s' % args.k_range)
    utils._print(log_file, 'iterations: %s' % args.iters)
    utils._print(log_file, 'days: %s' % args.ndays)
    utils._print(log_file, DataFrame(error_rates_df.std(), columns=['std dev error:']).T)
    utils._print(log_file, DataFrame(error_rates_df.mean(), columns=['avg error:']).T)
else:
    days = sample(training_set, args.ndays)
    costf = numpredict.createcostfunction(cw, args.ndays, training_set, days=days)
    vec, error_mean, error_std = optimization.annealingoptimize(
                                                [(0.0,1.0)]*(len(stock_primary.columns)-2),
                                                costf,
                                                step=args.stepsize,
                                                iters=args.iters)
    utils._print(log_file, 'dataset: %s' % args.dataset)
    utils._print(log_file, 'k range: %s' % args.k_range)
    utils._print(log_file, 'iterations: %s' % args.iters)
    utils._print(log_file, 'days: %s' % args.ndays)
    sort_idx = np.argsort(vec)[::-1]
    vec = [(stock_primary.columns[x],vec[x]) for x in sort_idx]
    utils._print(log_file, 'scale:')
    for name, val in vec:
        name = name.replace('% Change(', '%').replace(')','')
        utils._print(log_file, '\t%s = %.2f' % (name, val))
    utils._print(log_file, 'std dev error: %.2f' % error_std)
    utils._print(log_file, 'avg error: %.2f' % error_mean)
    





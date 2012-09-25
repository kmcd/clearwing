"""
for usage, see:
    python prepare_data.py -h
"""
from random import sample
from datetime import datetime, date, timedelta
from clearwing import extract_data, select_model, utils
from pandas import *
import numpy as np
import os, glob, sys
import argparse

# parse command line arguments
parser = argparse.ArgumentParser()
parser.add_argument('-i','--input', dest='in_dir', help='input directory where is traiding stock data that you want to analyze')
parser.add_argument('-t','--target', dest='target_dir', help='target directory where composite index data is')
parser.add_argument('-cin','--inputCompositeIndexName', dest='input_composite_index_name', required=True, help='this is input component index name')
parser.add_argument('-cta','--targetCompositeIndexName', dest='target_composite_index_name', required=True, help='this is target component index name')
parser.add_argument('-o','--output', dest='out_dir', default='data/training', help='directory to store text reports')
parser.add_argument('-r','--range', type=float, default=[-0.03,0.03], nargs=2, help='long/short boundaries')
parser.add_argument('-c','--consecutive', dest='is_random', type=bool, const=False, 
                                default=True, nargs='?', help='generate random (ommit -c option) days or consecutive days (use -c option)')
parser.add_argument('-l','--lkbk', type=int, default=3, help='number of lookback days')
parser.add_argument('-nt','--ntop', type=int, default=10, help='number of top composite index components to use')
parser.add_argument('-sd','--sday', dest='start_day', help='composite index start date to use', required=True)
parser.add_argument('-ed','--eday', dest='end_day', help='composite index end date to use', required=True)
parser.add_argument('-cd','--cday', dest='chosen_day', help='chosen start date')
parser.add_argument('-nd','--ndays', type=int, default=60, help='number of random days to generate')
args = parser.parse_args()
# for debugging/printing purposes
set_printoptions(max_rows=100, max_columns=200, max_colwidth=10000)

# Generate series of business days from earliest date to latest date
compIndexStartDay = args.start_day
compIndexEndDay = args.end_day

inputComponentIndexName = args.input_composite_index_name.lower()
targetComponentIndexName = args.target_composite_index_name.lower()

trading_days = date_range(compIndexStartDay, compIndexEndDay, freq='B')

if args.is_random:
    # sample ndays from given date range
    trading_days = sample(trading_days, args.ndays)
    
    # divide into 3 sets
    train_set = sample(trading_days, args.ndays/3)
    validate_set = sample(utils.list_diff(trading_days, train_set), args.ndays/3)
    test_set = utils.list_diff(utils.list_diff(trading_days, train_set), validate_set)
    
    train_set_str = [date.date().strftime('%Y%m%d') for date in train_set]
    validate_set_str = [date.date().strftime('%Y%m%d') for date in validate_set]
    test_set_str = [date.date().strftime('%Y%m%d') for date in test_set]

    # h5 savefile
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    train_store = utils.create_hdf5(args.out_dir+'/train_'+args.start_day+'_'+args.end_day)
    validate_store = utils.create_hdf5(args.out_dir+'/validate_'+args.start_day+'_'+args.end_day)
    test_store = utils.create_hdf5(args.out_dir+'/test_'+args.start_day+'_'+args.end_day)

    # save dates to file
    train_f = open(args.out_dir+'/datelist_train_'+args.start_day+'_'+args.end_day+'.txt', 'w')
    validate_f = open(args.out_dir+'/datelist_validate_'+args.start_day+'_'+args.end_day+'.txt', 'w')
    test_f = open(args.out_dir+'/datelist_test_'+args.start_day+'_'+args.end_day+'.txt', 'w')
    utils.save_dates_to_file(train_f, train_set_str)
    utils.save_dates_to_file(validate_f, validate_set_str)
    utils.save_dates_to_file(test_f, test_set_str)
    
    train_set_str = utils.gen_lkbk_days(day_list=train_set)
    validate_set_str = utils.gen_lkbk_days(day_list=validate_set)
    test_set_str = utils.gen_lkbk_days(day_list=test_set)
else:
    # Pick a date at random
    # Generate a list of 60 business days starting from the random date chosen
    start_day = sample(trading_days, 1)[0]
    if args.chosen_day is not None:
        start_day = datetime.strptime(args.chosen_day, '%Y%m%d')
    training_set = date_range(start_day, periods=args.ndays, freq='B')
    train_set_str = [date.date().strftime('%Y%m%d') for date in training_set]

    # h5 savefile
    if not os.path.exists(args.out_dir):
        os.makedirs(args.out_dir)
    train_store = utils.create_hdf5(args.out_dir+'/'+start_day.strftime('%Y%m%d'))
    
def save_data_of_set(_set, _store):
    components = {}
    targetCompositeIndexComponents = []
    day_count = 0
    for date in _set:
        day_count = day_count + 1
        print 'loading day %d: %s' % (day_count,date)
        
        # Generate DateTimeIndex to be used for reindexing the per day DataFrame
        start_of_day = datetime.strptime(date,'%Y%m%d').replace(hour=9,minute=30)
        end_of_day = datetime.strptime(date,'%Y%m%d').replace(hour=16)
        idx = date_range(start_of_day, end_of_day, freq='Min')
        
        # Collect input component index components of the given date
        for inputCompositeIndexComponentsFile in glob.glob(os.path.join('data',args.in_dir,'allstocks_'+date,'*')):
            name = inputCompositeIndexComponentsFile.rpartition('_')[2][:-4]
            try:
                df = extract_data.start(inputCompositeIndexComponentsFile, date, idx)
                if len(df.index) == 0:  # discard empty set
                    print 'set is empty'
                else:
                    if not components.get(name):
                        components[name] = [df]
                    else:
                        components[name].append(df)
            except:
                print sys.exc_info()
                print 'error in %s' % inputCompositeIndexComponentsFile
                
        # Collect output component index data of the given date
        for targetCompositeIndexFile in glob.glob(os.path.join('data',args.target_dir,'allstocks_'+date,'table_'+targetComponentIndexName+'.csv')):
            try:
                df = extract_data.start(targetCompositeIndexFile, date, idx)
                targetCompositeIndexComponents.append(df)
            except:
                print sys.exc_info()
                print 'error in %s' % inputCompositeIndexComponentsFile
                
    # concatenate all the input index composite index components into one Panel object
    inputCompositeIndexComponents = {}
    for k, v in components.items():
        inputCompositeIndexComponents[k] = concat(v).fillna(method='pad').fillna(method='bfill')
    inputCompositeIndexComponents = Panel(inputCompositeIndexComponents)
    print '\n\n>>> '+inputComponentIndexName+' comp'
    print inputCompositeIndexComponents

    # concatenate all targetCompositeIndexComponents into one DataFrame object
    # append long and short classifiers
    targetCompositeIndex = concat(targetCompositeIndexComponents)
    targetCompositeIndex_long = {}
    targetCompositeIndex_short = {}
    for i in range(len(targetCompositeIndex)):
        t = targetCompositeIndex.index[i]
        if t.hour == 16:
            continue
        targetCompositeIndex_long[t] = select_model.is_long(targetCompositeIndex, t, args.range)
        targetCompositeIndex_short[t] = select_model.is_short(targetCompositeIndex, t, args.range)
    targetCompositeIndex['is_long'] = Series(targetCompositeIndex_long)
    targetCompositeIndex['is_short'] = Series(targetCompositeIndex_short)
    targetCompositeIndex['is_long'] = targetCompositeIndex['is_long'].fillna(value=False)
    targetCompositeIndex['is_short'] = targetCompositeIndex['is_short'].fillna(value=False)

    print '\n\n>>> '+targetComponentIndexName
    print targetCompositeIndex.head()
    print targetCompositeIndex.tail()

    # compute for liquidity (Volume * Close)
    # converted to per million units for printing
    close_price_mat = inputCompositeIndexComponents.ix[:,:,'Close']
    vol_mat = inputCompositeIndexComponents.ix[:,:,'Volume']
    liq_mat = close_price_mat * vol_mat / 1000000 # liquidity in millions
    liq_mat = liq_mat.fillna(value=0)

    # save everything into h5 format
    utils.save_object(_store, inputCompositeIndexComponents, inputComponentIndexName)
    utils.save_object(_store, targetCompositeIndex, targetComponentIndexName)
    utils.save_object(_store, vol_mat, 'vol_mat')
    utils.save_object(_store, liq_mat, 'liq_mat')
    print _store
    
if args.is_random:
    save_data_of_set(train_set_str, train_store)
    save_data_of_set(validate_set_str, validate_store)
    save_data_of_set(test_set_str, test_store)
else:
    save_data_of_set(train_set_str, train_store)

"""
usage:
python select_model.py <date>

where:
<date> is the start_day from prepare_data.py with format YYYYMMDD
"""
from clearwing import select_model, utils
from datetime import datetime
from pandas import *
import sys

dir_name = 'data/training'
start_day_str = sys.argv[1]
store = HDFStore(dir_name+'/'+start_day_str+'.h5')

nasdaq_comp = store['nasdaq_comp']
qqq = store['qqq']

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

# select liquidity at the end of the each day
lpbk = 3
training_days = date_range(training_set[lpbk], training_set[-1], freq='B').shift(16, freq='H')
liq_mat_eod = liq_mat.reindex(training_days)

# collect top nasdaq components in terms of liquidity
top_dims = []
with_records = []
for i in range(0, len(liq_mat_eod)):
    try:
        row = liq_mat_eod.ix[i,:]
        new_index = row.index[np.argsort(row)[:-11:-1]]
        row = row.reindex(index=new_index)
        top_dims_day = DataFrame({'Liquidity':row})
        top_dims.append(top_dims_day)
        with_records.append(liq_mat_eod.index[i])
    except:
        print 'no record on %s, maybe a holiday' % liq_mat_eod.index[i]
top_dims = concat(top_dims, keys=with_records)
    
print '\n\n>>> Top 10 liquidity per day'
print top_dims.head(10)
print top_dims.tail(10)

print '\n\n>>> Top 10 liquidity of day %d, %s' % (lpbk, training_days[0])
print top_dims.ix[training_days[0]].head(15)
print top_dims.ix[training_days[0]].index

top10_liq = select_model.get_top_dims(liq_mat, top_dims, training_set[0], training_days[0])
print '\n\n>>> Top 10 Nasdaq components with highest liquidity on day %d' % lpbk
print top10_liq.head()
print top10_liq.tail()

# kNN
knn = select_model.KNN(top10_liq, qqq)
print '\n\n>>> Start error_score using long classifier'
print 'error rate = %f%%' % knn.error_score(select_model.is_long, top10_liq, k=7)
print '\n\n>>> Start error_score using short classifier'
print 'error rate = %f%%' % knn.error_score(select_model.is_short, top10_liq, k=7)

utils.save_object(store, vol_mat, 'vol_mat')
utils.save_object(store, liq_mat, 'liq_mat')
utils.save_object(store, top10_liq, 'top10_liq')


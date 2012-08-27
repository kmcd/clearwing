from datetime import timedelta
from numpy import linalg, argmax
from scipy.spatial import distance, KDTree
from pandas import *
import numpredict, utils
import time, sys, random

def is_long(df, curr_datetime, rng):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    
    return (next_close - current_close) >= max(rng) and (next_low - current_close) > min(rng)
             
def is_short(df, curr_datetime, rng):
    next_datetime = curr_datetime + timedelta(minutes=1)
    current_close = df.ix[curr_datetime, 'Close']
    next_high = df.ix[next_datetime, 'High']
    next_low = df.ix[next_datetime, 'Low']
    next_close = df.ix[next_datetime, 'Close']
    
    return (next_close - current_close) <= min(rng) and (next_high - current_close) < max(rng)

def get_top_dims(data, top_dims, start_date, end_date, top=10):
    idx = top_dims.ix[end_date].index
    dates = date_range(start_date, end_date, freq='Min')
    return data.ix[dates,idx[:top]].dropna()
        
class KNN:
    def __init__(self, data, qqq):
        self.data = data
        self.qqq = qqq
        
    def qqq_classify(self, idx):
        if self.qqq.ix[idx, 'is_long']:
            return 1
        elif self.qqq.ix[idx, 'is_short']:
            return 2
        else:
            return 0
            
    def get_dist_np(self, data, vec1):
        dist = (vec1-data)**2
        dist = np.sum(dist, axis=1)
        dist = np.sqrt(dist)
        dist.sort()
        return dist
        
    def estimate(self, vec, k=7):
        """
        gives an estimate of vec using kNN algorithm for the given k
        uses inverseweight as weights
        """
        dlist = self.get_dist_np(self.data, vec)[:k].reset_index()
        vals = [0,0,0]
        
        def weighted_classify(row):
            vals[self.qqq_classify(row['index'])] += numpredict.inverseweight(row[0])
        dlist.apply(weighted_classify, axis=1)
                
        if vals[1] == vals[2]:
            return 0
        return argmax(vals)

    def estimate_k_range(self, vec, k_range):
        """
        gives an estimate of vec using kNN algorithm for the given k_range
        uses inverseweight as weights
        """
        dlist = self.get_dist_np(self.data, vec)[:max(k_range)].reset_index()
        est = {}
        def fun(row, vals):
            vals[self.qqq_classify(row['index'])] += numpredict.inverseweight(row[0])
        for k in k_range:
            vals = [0,0,0]
            dlist[:k].apply(fun, axis=1, args=[vals])
            if vals[1] == vals[2]:
                est[k] = 0
            else:
                est[k] = argmax(vals)
        return est
        
    def error_score_k_range(self, inpt, k_range):
        """
        gives the %error for the estimates of the given set of vectors
        """
        ncor = {}
        count = 0
        for k in k_range:
            ncor[k] = 0.0
        st = time.time()       
        for i in range(  len(inpt)  ):
            
            if inpt.index[i].hour == 16:
                continue
            if inpt.index[i] in self.qqq.index:
                row = inpt.ix[i,:]                
                est = self.estimate_k_range(row, k_range)
                act = self.qqq_classify(inpt.index[i])
                for k in k_range:
                    if est[k] == act:
                        ncor[k] = ncor[k] + 1
                count = count + 1
                
        pct = {}
        for k in k_range:
            pct[k] = 1 - (ncor[k] / count)
            pct[k] *= 100.0
        return pct
            
    
    def error_score(self, inpt, k=7):
        """
        gives the %error for the estimates of the given set of vectors
        """
        ncor = 0.
        count = 0
        st = time.time()       
        for i in range(  len(inpt)  ):
            
            if inpt.index[i].hour == 16:
                continue
            if inpt.index[i] in self.qqq.index:
                row = inpt.ix[i,:]
                
                est = self.estimate(row, k)
                act = self.qqq_classify(inpt.index[i])
                
                if est == act:
                    ncor = ncor + 1
                count = count + 1
                
        pct = 1 - (ncor / count)
        return pct * 100.0
        
class CalculateWeights:
    def __init__(self, today_data_all, lkbk_days_data_all):
        self.today = today_data_all
        self.lkbk_days = lkbk_days_data_all
        
    def dividedata(self, today, lkbk_days):
        trainset = {}
        testset = {}
        trainset['input'] = lkbk_days.ix[:,:-2]
        trainset['result'] = lkbk_days.ix[:,['is_long','is_short']]
        testset['input'] = today.ix[:,:-2]
        testset['result'] = today.ix[:,['is_long','is_short']]
        return trainset, testset
        
    def rescale(self, _set, scale):
        _set['input'] = _set['input'] * scale
        return _set
        
    def map_classify(self, row):
        if row['is_long']:
            return 1
        elif row['is_short']:
            return 2
        else:
            return 0
            
    def getdistances(self, data, vec1):
        """
        get the euclidean distance of vec1 from all vectors of data
        sort by ascending order
        """
        distancelist = []
        
        for i in range(len(data)):
            vec2 = data.ix[i,:]
            distancelist.append((distance.euclidean(vec1, vec2),i))
        
        distancelist.sort()
        return distancelist
        
    def get_dist_np(self, data, vec1):
        """
        get the euclidean distance of vec1 from all vectors of data using numpy
        sort by ascending order
        """
        dist = (vec1-data)**2
        dist = np.sum(dist, axis=1)
        dist = np.sqrt(dist)
        dist.sort()
        return dist
        
    def estimate(self, vec, trainset, k=7):
        dlist = self.get_dist_np(trainset['input'], vec)[:k].reset_index()
        vals = [0,0,0]
        
        def weighted_classify(row):
            vals[self.map_classify(trainset['result'].ix[row['index'],:])] += numpredict.inverseweight(row[0])
        dlist.apply(weighted_classify, axis=1)

        if vals[1] == vals[2]:
            return 0
        return argmax(vals)

    def error_score(self, testset, trainset, k=7):
        ncor, count = 0, 0     
        for i in range(  len(testset['input'])  ):
            row = testset['input'].ix[i,:]
            act = self.map_classify(testset['result'].ix[i,:])
            est = self.estimate(row, trainset, k)
            if est == act:
                ncor = ncor + 1
            count = count + 1
                
        pct = 1 - (float(ncor) / count)
        return pct * 100.0
            
    def crossvalidate(self, ndays, scale, training_set, k=7, days=None):
        errors = []
        if days is None:
            days = random.sample(training_set, ndays)
        for today in days:
            try:
                st = time.time()
                today_data = self.today[today]
                lkbk_days_data = self.lkbk_days[today]
                trainset, testset = self.dividedata(today_data, lkbk_days_data)
                
                trainset = self.rescale(trainset, scale)
                testset = self.rescale(testset, scale)
                
                error_rate = self.error_score(testset, trainset)
                print '(today = %s, k = %d) error_rate = %.2f (time = %.2fs)' % (today, k, error_rate, time.time()-st)
                errors.append(error_rate)
            except:
                print sys.exc_info()
        return errors
        
    def estimate_k_range(self, vec, trainset, k_range):
        dlist = self.get_dist_np(trainset['input'], vec)[:max(k_range)].reset_index()
        est = {}
        def fun(row, vals):
            vals[self.map_classify(trainset['result'].ix[row['index'],:])] += numpredict.inverseweight(row[0])
        for k in k_range:
            vals = [0,0,0]
            dlist[:k].apply(fun, axis=1, args=[vals])
            if vals[1] == vals[2]:
                est[k] = 0
            else:
                est[k] = argmax(vals)
        return est

    def error_score_k_range(self, testset, trainset, k_range):
        ncor = {}
        count = 0
        for k in k_range: ncor[k] = 0.0
        for i in range(  len(testset['input'])  ):
            row = testset['input'].ix[i,:]
            act = self.map_classify(testset['result'].ix[i,:])
            est = self.estimate_k_range(row, trainset, k_range)
            for k in k_range:
                if est[k] == act:
                    ncor[k] = ncor[k] + 1
            count = count + 1
                
        pct = {}
        for k in k_range:
            pct[k] = 1 - (float(ncor[k]) / count)
            pct[k] *= 100.0
        return pct

    def crossvalidate_k_range(self, ndays, scale, training_set, k_range=None, days=None):
        errors = {}
        for k in k_range:
            errors[k] = []
        if days is None:
            days = random.sample(training_set, ndays)
        for today in days:
            try:
                st = time.time()
                today_data = self.today[today]
                lkbk_days_data = self.lkbk_days[today]
                trainset, testset = self.dividedata(today_data, lkbk_days_data)
                
                trainset = self.rescale(trainset, scale)
                testset = self.rescale(testset, scale)
                
                error_rate = self.error_score_k_range(testset, trainset, k_range)
                print '(today = %s, k_range = %s, time = %.2fs)' % (today, k_range, time.time()-st)
                for k in k_range:
                    errors[k].append(error_rate[k])
            except:
                print sys.exc_info()
        return errors
        

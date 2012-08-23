from random import random,randint
import math

def inverseweight(dist,num=1.0,const=0.1):
    return num/(dist+const)

def subtractweight(dist,const=1.0):
    if dist>const: 
        return 0
    else: 
        return const-dist

def gaussian(dist,sigma=5.0):
    return math.e**(-dist**2/(2*sigma**2))

def weightedknn(data,vec1,k=5,weightf=gaussian):
    # Get distances
    dlist=getdistances(data,vec1)
    avg=0.0
    totalweight=0.0
    
    # Get weighted average
    for i in range(k):
        dist=dlist[i][0]
        idx=dlist[i][1]
        weight=weightf(dist)
        avg+=weight*data[idx]['result']
        totalweight+=weight
    if totalweight==0: return 0
    avg=avg/totalweight
    return avg

def dividedata(data,test=0.05):
    trainset=[]
    testset=[]
    for row in data:
        if random()<test:
            testset.append(row)
        else:
            trainset.append(row)
    return trainset,testset

def testalgorithm(algf,trainset,testset):
    error=0.0
    for row in testset:
        guess=algf(trainset,row['input'])
        error+=(row['result']-guess)**2
        #print row['result'],guess
    #print error/len(testset)
    return error/len(testset)

def crossvalidate(algf,data,trials=100,test=0.1):
    error=0.0
    for i in range(trials):
        trainset,testset=dividedata(data,test)
        error+=testalgorithm(algf,trainset,testset)
    return error/trials

def rescale(data,scale):
    scaleddata=[]
    for row in data:
        scaled=[scale[i]*row['input'][i] for i in range(len(scale))]
        scaleddata.append({'input':scaled,'result':row['result']})
    return scaleddata

def createcostfunction(cw, ndays, training_set, k=7):
    def costf(scale):
        return np.mean(cw.crossvalidate(ndays, scale, training_set, k))
    return costf

def probguess(data,vec1,low,high,k=5,weightf=gaussian):
    dlist=getdistances(data,vec1)
    nweight=0.0
    tweight=0.0
    
    for i in range(k):
        dist=dlist[i][0]
        idx=dlist[i][1]
        weight=weightf(dist)
        v=data[idx]['result']
        
        # Is this point in the range?
        if v>=low and v<=high:
            nweight+=weight
        tweight+=weight
    if tweight==0: return 0
    
    # The probability is the weights in the range
    # divided by all the weights
    return nweight/tweight

from pylab import *

def cumulativegraph(data,vec1,high,k=5,weightf=gaussian):
    t1=arange(0.0,high,0.1)
    cprob=array([probguess(data,vec1,0,v,k,weightf) for v in t1])
    plot(t1,cprob)
    show()


def probabilitygraph(data,vec1,high,k=5,weightf=gaussian,ss=5.0):
    # Make a range for the prices
    t1=arange(0.0,high,0.1)
    
    # Get the probabilities for the entire range
    probs=[probguess(data,vec1,v,v+0.1,k,weightf) for v in t1]
    
    # Smooth them by adding the gaussian of the nearby probabilites
    smoothed=[]
    for i in range(len(probs)):
        sv=0.0
        for j in range(0,len(probs)):
            dist=abs(i-j)*0.1
            weight=gaussian(dist,sigma=ss)
            sv+=weight*probs[j]
        smoothed.append(sv)
    smoothed=array(smoothed)
        
    plot(t1,smoothed)
    show()

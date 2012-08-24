import time
import random
import math
import numpy as np

people = [('Seymour','BOS'),
          ('Franny','DAL'),
          ('Zooey','CAK'),
          ('Walt','MIA'),
          ('Buddy','ORD'),
          ('Les','OMA')]
# Laguardia
destination='LGA'

flights={}
#
"""
for line in file('schedule.txt'):
origin,dest,depart,arrive,price=line.strip().split(',')
flights.setdefault((origin,dest),[])

# Add details to the list of possible flights
flights[(origin,dest)].append((depart,arrive,int(price)))
"""
def getminutes(t):
  x=time.strptime(t,'%H:%M')
  return x[3]*60+x[4]

def printschedule(r):
  for d in range(len(r)/2):
    name=people[d][0]
    origin=people[d][1]
    out=flights[(origin,destination)][int(r[d])]
    ret=flights[(destination,origin)][int(r[d+1])]
    print '%10s%10s %5s-%5s $%3s %5s-%5s $%3s' % (name,origin,
                                                  out[0],out[1],out[2],
                                                  ret[0],ret[1],ret[2])

def schedulecost(sol):
  totalprice=0
  latestarrival=0
  earliestdep=24*60

  for d in range(len(sol)/2):
    # Get the inbound and outbound flights
    origin=people[d][1]
    outbound=flights[(origin,destination)][int(sol[d])]
    returnf=flights[(destination,origin)][int(sol[d+1])]
    
    # Total price is the price of all outbound and return flights
    totalprice+=outbound[2]
    totalprice+=returnf[2]
    
    # Track the latest arrival and earliest departure
    if latestarrival<getminutes(outbound[1]): latestarrival=getminutes(outbound[1])
    if earliestdep>getminutes(returnf[0]): earliestdep=getminutes(returnf[0])
  
  # Every person must wait at the airport until the latest person arrives.
  # They also must arrive at the same time and wait for their flights.
  totalwait=0
  for d in range(len(sol)/2):
    origin=people[d][1]
    outbound=flights[(origin,destination)][int(sol[d])]
    returnf=flights[(destination,origin)][int(sol[d+1])]
    totalwait+=latestarrival-getminutes(outbound[1])
    totalwait+=getminutes(returnf[0])-earliestdep

  # Does this solution require an extra day of car rental? That'll be $50!
  if latestarrival>earliestdep: totalprice+=50
  
  return totalprice+totalwait

def randomoptimize(domain,costf):
  best=999999999
  bestr=None
  for i in range(0,1000):
    # Create a random solution
    r=[float(random.randint(domain[i][0],domain[i][1]))
       for i in range(len(domain))]
    
    # Get the cost
    cost=costf(r)
    
    # Compare it to the best one so far
    if cost<best:
      best=cost
      bestr=r
  return r


def annealingoptimize(domain,costf,step=1,iters=20):
  T = 100000.0
  cool = math.pow(T,-1.0/iters)

  # Initialize the values randomly
  vec=[float(random.randint(domain[i][0],domain[i][1]))
       for i in range(len(domain))]
  
  print '\n\norig'
  error_list_a =costf(vec)
  ea = np.mean(error_list_a)
  
  iter_count = 0
    
  while T>0.1:
    print '\n\niter %d' % iter_count
    iter_count += 1
    
    # Choose one of the indices
    i=random.randint(0,len(domain)-1)

    # Choose a direction to change it
    dir=random.randint(-1,1)

    # Create a new list with one of the values changed
    vecb=vec[:]
    vecb[i]+=dir*step
    if vecb[i]<domain[i][0]: vecb[i]=domain[i][0]
    elif vecb[i]>domain[i][1]: vecb[i]=domain[i][1]

    # Calculate the current cost and the new cost
    error_list_b=costf(vecb)
    eb = np.mean(error_list_b)
    p=pow(math.e,(-eb-ea)/T)

    print vec,ea


    # Is it better, or does it make the probability
    # cutoff?
    if (eb<ea or random.random()<p):
      vec=vecb
      ea = eb
      error_list_a = error_list_b

    # Decrease the temperature
    T=T*cool
  return vec, ea, np.std(error_list_a)

def swarmoptimize(domain,costf,popsize=20,lrate=0.1,maxv=2.0,iters=50):
  # Initialize individuals
  # current solutions
  x=[]

  # best solutions
  p=[]

  # velocities
  v=[]
  
  for i in range(0,popsize):
    vec=[float(random.randint(domain[i][0],domain[i][1]))
         for i in range(len(domain))]
    x.append(vec)
    p.append(vec[:])
    v.append([0.0 for i in vec])
  
  
  for ml in range(0,iters):
    for i in range(0,popsize):
      # Best solution for this particle
      if costf(x[i])<costf(p[i]):
        p[i]=x[i][:]
      g=i

      # Best solution for any particle
      for j in range(0,popsize):
        if costf(p[j])<costf(p[g]): g=j
      for d in range(len(x[i])):
        # Update the velocity of this particle
        v[i][d]+=lrate*(p[i][d]-x[i][d])+lrate*(p[g][d]-x[i][d])

        # constrain velocity to a maximum
        if v[i][d]>maxv: v[i][d]=maxv
        elif v[i][d]<-maxv: v[i][d]=-maxv

        # constrain bounds of solutions
        x[i][d]+=v[i][d]
        if x[i][d]<domain[d][0]: x[i][d]=domain[d][0]
        elif x[i][d]>domain[d][1]: x[i][d]=domain[d][1]

    print p[g],costf(p[g])
  return p[g]

from sets import Set
import os

def get_trading_days():
    with open(os.path.join('data','qqq','table_qqq.csv')) as f:
        s = Set()
        for line in f:
            parts = line.partition(',')
            if parts[0] not in s:
                s.add(parts[0])
        return s
            

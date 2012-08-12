from pandas import *
from datetime import datetime

def create_hdf5(fname):
    store = HDFStore(fname + '.h5')
    return store
    
def save_object(store, obj, key):
    store[key] = obj
    
def gen_lkbk_days(today=None, day_list=None, lkbk=3):
    # generate lkbk_days for the given day(s)
    seen = set()
    with_lkbk = []
    if today is not None:
        for i in range(lkbk,0,-1):
            lkbk_day = today - i*datetools.BDay()
            if lkbk_day in seen: continue
            seen.add(lkbk_day)
            with_lkbk.append(lkbk_day)
    elif day_list is not None:
        for day in day_list:
            for i in range(lkbk,0,-1):
                lkbk_day = day - i*datetools.BDay()
                if lkbk_day in seen: continue
                seen.add(lkbk_day)
                with_lkbk.append(lkbk_day)
            if day in seen: continue
            seen.add(day)
            with_lkbk.append(day)
    days_str = [date.date().strftime('%Y%m%d') for date in with_lkbk]
    return days_str

def _print(f, string):
    print string
    f.write(string+'\n')
    

from pandas import *
from datetime import datetime
import random

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
    f.write(str(string))
    f.write('\n')
    
def set_start_time(dates):
    """
    set all date's time to 9:30
    """
    return dates.shift(9, freq='H').shift(30, freq='Min')
    
def day_time_range(date):
    """
    generate range of time from 9:30 to 16:00 for the given date
    """
    return date_range(date.replace(hour=9,minute=30),
                      date.replace(hour=16,minute=0),
                      freq='Min')

def list_diff(a, b):
    b = set(b)
    return [aa for aa in a if aa not in b]
    
def save_dates_to_file(f, f_str):
    for date in f_str:
        f.write(date+'\n')
    f.close()

def sample_index(index, n):
    return random.sample(index, n)

from pandas import *

def create_hdf5(fname):
    store = HDFStore(fname + '.h5')
    return store
    
def save_object(store, obj, key):
    store[key] = obj

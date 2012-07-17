import random
import time

def MDYtoYMD(date):
    """
    Converts a datetime object to an integer of the form YYYYMMDD
    """
    return date.year * 10000 + date.month * 100 + date.day


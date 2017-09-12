from copy import copy, deepcopy
from datetime import timedelta
from lenses import lens
from functools import reduce
import datetime as dt
import pandas as pd
import numpy as np


def base10toN(num,n=36):
    """Change a  to a base-n number.
    Up to base-36 is supported without special notation."""
    num_rep = {10:'a', 11:'b', 12:'c', 13:'d', 14:'e', 15:'f', 16:'g', 17:'h', 18:'i', 19:'j', 20:'k', 21:'l', 22:'m', 
               23:'n', 24:'o', 25:'p', 26:'q', 27:'r', 28:'s', 29:'t', 30:'u', 31:'v', 32:'w', 33:'x', 34:'y', 35:'z'}
    new_num_string = ''
    current = num
    while current != 0:
        remainder = current % n
        if 36 > remainder > 9:
            remainder_string = num_rep[remainder]
        elif remainder >= 36:
            remainder_string = '(' + str(remainder) + ')'
        else:
            remainder_string = str(remainder)
        new_num_string = remainder_string + new_num_string
        current = current // n
    return new_num_string

def memloc(self):
    return base10toN(id(self), 36)[-4:]

def listunion(*lists):
    if lists == (): return []
    lnew = []
    for ll in lists:
        if not isinstance(ll, list): ll = [ll]
        for l in ll:
            if l not in lnew:
                lnew.append(l)
    return lnew

def listminus(l1,l2):
    if not isinstance(l2, list): l2 = [l2]
    return [l for l in l1 if not l in l2]
    
def listintersection(l1,l2):
    if not isinstance(l2, list): l2 = [l2]
    return [l for l in l1 if l in l2]


# %% ^━━━━━━━━━━━━━━━━━ SUBJECT TO CHANGE ━━━━━━━━━━━━━━━━━━━━━━^

class KV(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value
    # def key(self):
    #     return self[0]
    # def value(self):
    #     return self[1]
    def __iter__(self):
        return iter((self.key, self.value))


class L(list):
    
    def __init__(self, *args):
        args = [arg for arg in args if arg is not None]
        list.__init__(self, args)
    
    def __add__(self, other):
        return L(*listunion(self, other))
    
    def __xor__(self, other):
        return L(*listintersection(self, other))
    
    def __sub__(self, other):
        return L(*listminus(self, other))
    
    def __and__(self, other):
        return L(*listintersection(self, other))
    
    def __le__(self, other):
        return not bool(self - other)
    
    def __radd__(self, other):
        res = copy(self)
        if hasattr(other, '__iter__'):
            for el in other:
                res += el
        return res
    
    def __iadd__(self, other):
        # can't remember what this is for
        return NotImplemented
    
    def __repr__(self):
        return f'L<{list.__repr__(self)[1:-1]}>'
    
    def __rshift__(self, func):
        return func(self)
    
    def filter(self, cond):
        return L(*[el for el in self if cond(el)])
    
    def fmap(self, ffunc):
        return L(*[ffunc(el) for el in self])
    
    def modify(self, mfunc):
        for i, el in self.enumerate():
            self[i] = mfunc(el)
    
    def combine(self):
        # import query
        return self.fold(lambda x, y: x @ y)
    
    def enumerate(self):
        return L(*[(k, v) for k, v in enumerate(self)])
    
    def flatten(self):
        if not self:
            return L()
        return sum(self)
    
    def intersperse(self, separator):
        return separator.join(self.fmap(str))
        
    def bind(self, bfunc):
        return self.fmap(bfunc).flatten()
        
    def len(self):
        return len(self)
        
    def fold(self, ffunc=None, mzero=None, meth=None):
        if not self:
            print("Empty Fold")
        res = mzero if mzero else self[0]
        ffunc = ffunc if ffunc is not None else getattr(mzero.__class__, meth)
        for el in self:
            res = ffunc(res, el)
        return res
    
    def sum(self):
        return self.flatten()
    
    def all(self):
        return all(self)
        # return self.fold(self[0].__class__.__and__)
    
    def any(self):
        return any(self)
        # return self.fold(self[0].__class__.__or__)
    
    def groupby(self, gpbyfunc):
        res = {}
        for el in self:
            dummylabel = str(gpbyfunc(el))
            if dummylabel in res:
                res[dummylabel] += el
            else:
                res[dummylabel] = L(el)
        return L(*[KV(key, value) for key, value in res.items()])
    
    def exists(self):
        return bool(self)
    
    def notExists(self):
        return not self.exists()
    
    def head(self):
        return L(self[0])
    
    def copy(self):
        return copy(self)
    
    def sort(self, key=None):
        return L(*sorted(self, key=key))
        
    def getTables(self):
        return self.bind(lambda x: x.getTables())


def updateUnique(self, other, makecopy=False):
    res = copy(self) if makecopy else self
    for key, value in other.items():
        if key not in res:
            res[key] = value
        elif value not in res.values():
            n = 0
            while True:
                if key + str(n) not in res:
                    res[key + str(n)] = value
                    break
                n += 1
    if makecopy:
        return res

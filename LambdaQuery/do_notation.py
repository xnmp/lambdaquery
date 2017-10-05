import types

# gives do notation for python, with "yield" statements and enabling python code in between binds
# take from http://www.valuedlessons.com/2008/01/monads-in-python-with-nice-syntax.html
# just made a few changes to let mreturn handle a tuple, and to convert python 2 to 3
# I actually have no idea how this works

class Monad:
    def bind(self, func):
        raise NotImplementedError

    def __rshift__(self, bindee):
        return bindee(self)

    def __add__(self, bindee_without_arg):
        return self.bind(lambda _ : bindee_without_arg())

def make_decorator(func, *dec_args):
    def decorator(undecorated):
        def decorated(*args, **kargs):
            return func(undecorated, args, kargs, *dec_args) 
        # print(func.__name__)
        decorated.__name__ = undecorated.__name__
        return decorated
    
    decorator.__name__ = func.__name__
    return decorator

def make_decorator_with_args(func):
    def decorator_with_args(*dec_args):
        return make_decorator(func, *dec_args)
    return decorator_with_args

decorator           = make_decorator
decorator_with_args = make_decorator_with_args

@decorator_with_args
def do(func, func_args, func_kargs, Monad):
    @handle_monadic_throws(Monad)
    def run_maybe_iterator():
        itr = func(*func_args, **func_kargs)

        if isinstance(itr, types.GeneratorType):
            
            @handle_monadic_throws(Monad)
            def send(*vals):
                try:
                    # here's the real magic
                    monad = itr.send(*vals) 
                    return monad.bind(send)
                except StopIteration:                    
                    return Monad.unit(None)
                
            return send(None)
        else:
            #not really a generator
            if itr is None:
                return Monad.unit(None)
            else:
                return itr
    run_maybe_iterator.__name__ = func.__name__
    return run_maybe_iterator()

@decorator_with_args
def handle_monadic_throws(func, func_args, func_kargs, Monad):
    try:
        return func(*func_args, **func_kargs)
    # except Guard as cond:        
    #     return cond.cond.values().sum().asQuery()
    except MonadReturn as ret:
        return Monad.unit(*ret.values, **ret.kwvals)
    except Done as done:
        assert isinstance(done.monad, Monad)
        return done.monad

class Guard(Exception):
    def __init__(self, cond):
        self.cond = cond
        Exception.__init__(self)

class MonadReturn(Exception):
    def __init__(self, *values, **kwargs):
        self.values = values
        self.kwvals = kwargs
        Exception.__init__(self)

class Done(Exception):
    def __init__(self, monad):
        self.monad = monad
        Exception.__init__(self, monad)

def guard(cond):
    raise Guard(cond)
    # yield cond.values().sum().asQuery()

def returnM(*vals, **kwargs):
    raise MonadReturn(*vals,**kwargs)

def done(val):
    print("ghgjhgjh")
    raise Done(val)

# def fid(val):
    # return val


# example below for guac

# class List(Monad):

#     def __init__(self, *args):
#         self.values = list(args)
    
#     @classmethod
#     def unit(cls, x):
#         return List(x)
    
#     def __add__(self, other):
#         self.values += other.values
#         return self
    
#     @classmethod
#     def empty(cls):
#         return List()
    
#     def bind(m, f):
#         result = List()
#         for elem in m:
#             result += f(elem)
#         return result
    
#     def __getitem__(self, n):
#         return self.values[n]
    
#     def __repr__(self):
#         return self.values.__repr__()
    
#     def append(self, newobj):
#         self.values.append(newobj)


# @do(List)
# def make_change(amount_still_owed, possible_coins):
#     change = List()
    
    
#     # Keep adding coins while we owe them money and there are still coins.
#     while amount_still_owed > 0 and possible_coins:
    
#         # "Nondeterministically" choose whether to give anther coin of this value.
#         # Aka, try both branches, and return both results.
#         give_min_coin = yield List(True, False)
        
#         if give_min_coin:
#             # Give coin
#             min_coin = possible_coins[0]
#             change.append(min_coin)
#             amount_still_owed -= min_coin
#         else:
#             # Never give this coin value again (in this branch!)
#             del possible_coins[0]
            
#     # Did we charge them the right amount?
#     yield guard(amount_still_owed == 0)
    
#     # Lift the result back into the monad.
#     returnM (change)

# @do(List)
# def guard(condition):
#     if condition:
#         yield List.unit(())
#     else:
#         yield List.empty()

# if __name__ == '__main__':
#     print(make_change(27, [1, 5, 10, 25]))

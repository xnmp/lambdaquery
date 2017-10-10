from functools import wraps
from LambdaQuery.query import *


def asExpr(value):
    if isinstance(value, Columns):
        if len(value) > 1:
            raise TypeError(f"Can't convert Columns with more than one Expr to Expr: {value}")
        return value.asExpr()
    elif isinstance(value, ConstExpr.allowedTypes):
        return ConstExpr(value)
    elif isinstance(value, Expr):
        return value
    raise TypeError(f"Cannot convert to Expr: {value} of type {type(value)}")


def labelResult(func, args):
    return func.__name__.strip('_') + '_' + args.bind(lambda x: x.keys() if type(x) is Columns 
                                                      else L(x.__class__.__name__.lower())).intersperse('_')


def augment(func):
    # Columns -> Columns and Columns -> Query
    # all we want to do is lift func to something that carries through the joinconds and the groupbys
    # the complication is that we need it to addquery, or do we?
    @wraps(func)
    def mfunc(*args, **kwargs):
        
        res = func(*args, **kwargs)
        # if isinstance(res, Query): res = res.joinM()
        
        colargs = L(*args).filter(lambda x: isinstance(x, Columns))
        oldgroupbys = colargs.bind(lambda x: x.groupbys)
        oldjc = colargs.fmap(lambda x: x.asQuery()).fold(lambda x, y: x | y)
        
        if isinstance(res, Query) and type(res.columns) is not Columns:            
            for table in oldgroupbys.getTables():
                table.derivatives += res.columns.getTables()[0]
        
        res.groupbys = oldgroupbys + res.groupbys
        # res.joincond @= colargs.fmap(lambda x: x.asQuery()).combine()
        
        if isinstance(res, Columns):
            res = addQuery(oldjc, res.asQuery(), addcols='right').asCols()            
            if type(res) is Columns: res = res.label(labelResult(func, colargs))
        else:
            res = addQuery(oldjc, res.asQuery(), addcols='right')
            # this breaks things
            # res.columns = res.columns.label(func.__name__)
        
        return res
    return mfunc


def lift(func):
    """
    Lifts Expr -> Expr to Columns -> Columns. "Applicative instance for Columns"
    """
    @wraps(func)
    def colfunc(*args, **kwargs):
        
        res = Columns()
        colargs = L(*args).filter(lambda x: isinstance(x, Columns))
        res[labelResult(func, colargs)] = func(*L(*args).fmap(asExpr), **kwargs)
        
        # replica of augment logic
        res.groupbys = colargs.bind(lambda x: x.groupbys)
        # we're NOT using addQuery here
        res.joincond &= colargs.fmap(lambda x: x.joincond).fold(lambda x, y: x & y, mzero=AndExpr())
        # res.joincond @= colargs.fmap(lambda x: x.asQuery()).combine()
        # oldjc = colargs.fmap(lambda x: x.asQuery()).fold(lambda x, y: x @ y)
        # res = addQuery(oldjc, res.asQuery(), addcols='right').asCols()
        
        return res
        
    setattr(Columns, func.__name__, colfunc)
    setattr(Expr, '_' + func.__name__, func)
    return colfunc


def injective(fname=None):
    def decorator(func):
        nonlocal fname
        if fname is None:
            fname = func.__name__
        @property
        def colfunc(self, *args, **kwargs):
            if hasattr(self, fname + '_saved'):
                return getattr(self, fname + '_saved')
            
            res = func(self, *args, **kwargs)
            
            # another replica of augment
            res.groupbys = self.groupbys + res.groupbys
            
            if type(res) is Query and type(res.columns) is not Columns:
                for table in res.groupbys.getTables():
                    table.derivatives += res.columns.getTables()[0]
            
            res = addQuery(self.asQuery(), res.asQuery(), addcols='right').one
            # res.joincond &= self.joincond
            
            # STOP using primary as a way to track where the column came from, that's the role of the group bys
            object.__setattr__(self, fname + '_saved', res)
            return res
        setattr(Columns, func.__name__, colfunc)
        return colfunc
    return decorator


def sqlfunc(strfunc):
    @lift
    @wraps(strfunc)
    def exprfunc(*exprs, **kwargs):
        return FuncExpr(strfunc, *exprs, **kwargs)
    return exprfunc


def aggfunc(strfunc):
    @lift
    @wraps(strfunc)
    def exprfunc(*exprs, **kwargs):
        return AggExpr(strfunc, *exprs, **kwargs)
    @wraps(strfunc)
    def qfunc(q0, colname=None, **kwargs):
        q0 = copy(q0)
        if colname is not None:
            q0 = q0.fmap(lambda x: getattr(x, colname))
        elif len(q0.columns) > 1:
            # this is so you can do milestones().count() instead of milestones().count('trid')
            q0.columns = q0.getPrimary()
            # q0 = q0.fmap(lambda x: q0.getPrimary())
        return q0.aggregate(lambda x: exprfunc(x, **kwargs))
    setattr(Query, strfunc.__name__[:-1], qfunc)
    return exprfunc


def windowfunc(strfunc):
    # not implemented
    return strfunc


# this dooesn't work...
def memoize(fname=None):
    def decorator(func):
        nonlocal fname
        if fname is None:
            fname = func.__name__
        @wraps(func)
        def mfunc(self, *args, **kwargs):
            if hasattr(self, func.__name__ + '_saved'):
                return getattr(self, func.__name__ + '_saved')
            res = func(*args, **kwargs)
            object.__setattr__(self, func.__name__ + '_saved', res)
            return res
        return mfunc        
    return decorator


# def addJC(func):
#     @wraps(func)
#     def mfunc(*args, **kwargs):        
#         return func(*args, **kwargs).joinM() + Query.unit(*L(*args).filter(lambda x: isinstance(x, Columns)))
#     return mfunc


class Lifted(object):
    def __init__(self, func):
        self.func = augment(func)
        setattr(Columns, func.__name__, self.func)
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


class Kleisli(object):
    def __init__(self, func):        
        self.func = augment(func)
        setattr(Columns, func.__name__, self.func)
        setattr(Query, func.__name__, lambda x: x.bind(self.func))
    def __call__(self, *args, **kwargs):
        return self.func(*args, **kwargs)


# %% ^━━━━━━━━━━━━━━━━ OPERATORS ━━━━━━━━━━━━━━━━━━━━^

@lift
def __eq__(self, other):
    return EqExpr(self, other)
@lift
def __gt__(self, other):
    return BinOpExpr(">", self, other)
@lift
def __ge__(self, other):
    return BinOpExpr(">=", self, other)
@lift
def __lt__(self, other):
    return BinOpExpr("<", self, other)
@lift
def __le__(self, other):
    return BinOpExpr("<=", self, other)
@lift
def __ne__(self, other):
    return BinOpExpr("!=", self, other)
@lift
def __add__(self, other):
    if self.isTime() and type(other) is ConstExpr and isinstance(other.value, int):
        other = ConstExpr(timedelta(days = other.value))
    return BinOpExpr("+", self, other)
@lift
def __sub__(self, other):
    if self.isTime() and other.isTime():
        self = Expr._epoch_(self)
        other = Expr._epoch_(other)
    return BinOpExpr("-", self, other)
@lift
def __mul__(self, other):
    return BinOpExpr("*", self, other)
@lift
def __truediv__(self, other):
    return BinOpExpr(":: FLOAT /", self, other)
@lift
def __radd__(self, other):
    return BinOpExpr("+", other, self)
@lift
def __rsub__(self, other):
    return BinOpExpr("-", other, self)
@lift
def __rmul__(self, other):
    return BinOpExpr("*", other, self)
@lift
def __rtruediv__(self, other):
    return BinOpExpr(":: FLOAT /", other, self)
@lift
def __floordiv__(self, other):
    return BinOpExpr("/", self, other)
@lift
def __or__(self, other):
    return BinOpExpr("OR", self, other)
@lift
def __and__(self, other):
    return self & other
@sqlfunc
def __invert__(expr):
    if isinstance(expr, FuncExpr) and expr.func.__name__ == "__invert__":
        return str(expr).replace("NOT ", "")
    if isinstance(expr, FuncExpr) and expr.func.__name__ == "notnull_":
        return str(expr).replace("NOT NULL", "NULL")
    return f"NOT ({expr})"
@sqlfunc
def __neg__(expr):
    return f"-{expr}"


# %% ^━━━━━━━━━━━━━━━━━━━ COLUMN FUNCTIONS ━━━━━━━━━━━━━━━━━^

@sqlfunc
def round_(expr, interval=86400/4):
    return f"TIMESTAMP WITH TIME ZONE 'EPOCH' + FLOOR(EXTRACT(EPOCH FROM {expr}) / {interval}) * {interval} * INTERVAL '1 second'"
@sqlfunc
def zerodiv_(numer, denom):
    return f"COALESCE({numer} :: FLOAT / NULLIF({denom}, 0), 0)"
@sqlfunc
def len_(expr):
    return f"CHAR_LENGTH({expr})"
@sqlfunc
def in_(expr, *inlist):
    return f"{expr} IN {inlist}"
@sqlfunc
def like_(expr1, likestr=None):
    return f"{expr1} LIKE {likestr}"
@sqlfunc
def ilike_(expr1, likestr=None):
    return f"{expr1} ILIKE \'%{likestr}%\'"
@sqlfunc
def epoch_(expr):
    return f"EXTRACT(EPOCH FROM {expr})"
@sqlfunc
def randomize_(expr):
    return f"STRTOL(SUBSTRING(MD5({expr}), 1, 8), 16) :: FLOAT / (STRTOL('ffffffff', 16))"
@sqlfunc
def if_(cond, expr1, expr2):
    return f"CASE WHEN {cond} THEN {expr1} ELSE {expr2} END"
@sqlfunc
def ifen_(expr, cond):
    # if type(expr) is FuncExpr and expr.func.__name__ == "ifen_":
    #     cond = cond & expr.children[0]._notnull_() & expr.children[1]._notnull_()
    return f"CASE WHEN {cond} THEN {expr} END"
@sqlfunc
def case_(*pairs):
    finalexpr = pairs[-1]
    res = "CASE "
    for i in range(0, len(pairs) - 2, 2):
        res += f"\n  WHEN {pairs[i]} THEN {pairs[i+1]} "
    else:
        res += f"\n  ELSE {finalexpr} \nEND"
    return res
@sqlfunc
def roundnum_(expr, interval=1, up=False):
    if not up: 
        return f"FLOOR({expr} :: FLOAT / {interval}) * {interval}"
    else:
        return f"CEILING({expr} :: FLOAT / {interval}) * {interval}"
@sqlfunc
def cast_(expr, sqltype):
    strtype = str(sqltype)[1:-1]
    return f"({expr}) :: {strtype}"
@sqlfunc
def roundtime_(expr, interval=86400):
    # Expr._round_(Expr._epoch_(expr), interval)
    return f"TIMESTAMP WITH TIME ZONE 'EPOCH' + FLOOR(EXTRACT(EPOCH FROM {expr}) / {interval}) * {interval} * INTERVAL '1 second'"
@sqlfunc
def coalesce_(*exprs):
    return "COALESCE(" + ', '.join(map(str, exprs)) + ")"
@sqlfunc
def from_unixtime_(expr):
    return f"TIMESTAMP WITH TIME ZONE 'EPOCH' + {expr} * INTERVAL '1 SECOND'"
@sqlfunc
def log_(expr):
    return f"LN(GREATEST({expr}, 0) + 1)"
@sqlfunc
def exp_(expr):
    return f"EXP({expr})"
@sqlfunc
def floor_(expr):
    return f"FLOOR({expr})"
@sqlfunc
def isnull_(expr):
    if type(expr) is FuncExpr and expr.func.__name__ == "ifen_":        
        return f'{expr.children[0]} IS NULL OR NOT {expr.children[1]}'
    return f"{expr} IS NULL"
@sqlfunc
def notnull_(expr):
    if type(expr) is FuncExpr and expr.func.__name__ == "ifen_":        
        return f'{expr.children[0]} IS NOT NULL AND {expr.children[1]}'
    return f"{expr} IS NOT NULL"
@sqlfunc
def least_(*exprs):
    parts = ', '.join(map(str, exprs))
    return f"LEAST({parts})"
@sqlfunc
def zscore_(expr, partitionexpr):    
    return f"({expr} - AVG({expr}) OVER (PARTITION BY {partitionexpr})) :: FLOAT / STDDEV({expr}) OVER (PARTITION BY {partitionexpr})"
@sqlfunc
def greatest_(*exprs):
    parts = ', '.join(map(str, exprs))
    return f"GREATEST({parts})"
@sqlfunc
def row_(partitionexpr, orderexpr):    
    return f"ROW_NUMBER() OVER (PARTITION BY {partitionexpr} ORDER BY {orderexpr})"



@sqlfunc
def zscore_(expr, partitionexpr):    
    return f"({expr} - AVG({expr}) OVER (PARTITION BY {partitionexpr})) :: FLOAT / STDDEV({expr}) OVER (PARTITION BY {partitionexpr})"




# %% ^━━━━━━━━━━━━━━━━━━ AGGREGATES ━━━━━━━━━━━━━━━━━━━━^

@aggfunc
def count_(expr):
    return f'COUNT(DISTINCT {expr})'
@aggfunc
def avg_(expr, ntile=None):
    # expr = Expr._coalesce_(expr, 0)
    # return f'AVG({expr})'
    return f'AVG(COALESCE({expr}, 0))'
@aggfunc
def sum_(expr):
    return f'SUM({expr})'
@aggfunc
def max_(expr):
    return f'MAX({expr})'
@aggfunc
def min_(expr):
    return f'MIN({expr})'
@aggfunc
def any_(expr):
    return f'BOOL_OR({expr})'
@aggfunc
def all_(expr):
    return f'BOOL_AND({expr})'
@aggfunc
def median_(expr, partitions=None):
    return f'MEDIAN({expr})'



# %% ^━━━━━━━━━━━━━━━━━━━━ WINDOW FUNCTIONS ━━━━━━━━━━━━━━━━━━^

def partstr(partitions):
    if partitions is not None:
        return 'OVER' + '(PARTITION BY' + ','.join(map(str, partitions)) + ')'
    return ''

# @windowfunc
# def ntile_(perc=100, expr=None, partitions=None, order=None):
#     return f'NTILE({perc}) OVER (PARTITION BY {partitions} ORDER BY {expr})'
@windowfunc
def listagg_(expr=None, order=None):
    return f'LISTAGG({expr}) WITHIN GROUP (ORDER BY {expr})'
# @windowfunc
# def quantileof_(perc=100, expr=None, partitions=None):
#     return f'PERCENTILE_DISC({perc/100}) WITHIN GROUP (ORDER BY {expr}){partitions}'
# @windowfunc
# def median_(expr, partitions=None):
#     return f'MEDIAN({expr}) OVER (PARTITION BY {partitions})'
@windowfunc
def rank_(order=None, partitions=None):
    return f'ROW_NUMBER() OVER (PARTITION BY {partitions} ORDER BY {order})'
@windowfunc
def first_(expr, *partitions, order):
    parts = ','.join(map(str, partitions))
    return f'''FIRST_VALUE ({expr}) OVER (PARTITION BY {parts} ORDER BY {orderby} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'''
@windowfunc
def last_(expr, *partitions, order):
    parts = ','.join(map(str, partitions))
    return f'''LAST_VALUE ({expr}) OVER (PARTITION BY {parts} ORDER BY {orderby} ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)'''



# %% ^━━━━━━━━━━━━━━━━━━━━━ MISC FUNCTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

@Lifted
def ratio_(expr, cond):    
    return expr.ifen_(cond).count_().zerodiv_(expr.count_())

@Lifted
def between(self, bound, tlimit=None, ts='ts'):
    
    # todo: add a decorator for this
    try:
        tsvar = getattr(self, ts)
    except AttributeError:
        tsvars = self.items().filter(lambda x: '_ts' in x[0]).fmap(lambda x: x[1])
        if tsvars:
            tsvar = tsvars[0]
        else:
            tsvar = self
            # raise AttributeError(f"No timestamp in {self}")
    
    
    if isinstance(tlimit, Columns) or isinstance(tlimit, str) or isinstance(tlimit, dt.datetime):
        return (tsvar > bound) & (tsvar < tlimit)
    elif isinstance(tlimit, int):
        if tlimit > 0:
            tlimit = timedelta(days=tlimit)
            return (tsvar > bound) & (tsvar < bound + tlimit)
        else:
            tlimit = timedelta(days=-tlimit)
            return (tsvar < bound) & (tsvar > bound - tlimit)
    else:
        raise TypeError("Between func: invalid upper limit")


# %% ^━━━━━━━━━━━━━━━━━ OTHER FUNCTIONS ━━━━━━━━━━━━━━━━━━^


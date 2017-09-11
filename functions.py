from functools import wraps
from query import *


def asExpr(value):
    if isinstance(value, Columns):
        if len(value) > 1:
            raise TypeError(f"Can't convert Columns with more than one Expr to Expr: {value}")
        return value.asExpr()
    elif type(value) in ConstExpr.allowedTypes:
        return ConstExpr(value)
    elif isinstance(value, Expr):
        return value
    raise TypeError(f"Cannot convert to Expr: {value} of type {type(value)}")


def augment(func):
    # for functions that go to another table
    @wraps(func)
    def mfunc(*args, **kwargs):
        res = func(*args, **kwargs)
        if isinstance(res, Query):
            res = res.joinM()
        colargs = L(*args).filter(lambda x: isinstance(x, Columns))
        res.joincond += colargs.fmap(lambda x: x.joincond).sum()
        res.groupbys += colargs.bind(lambda x: x.groupbys)        
        if isinstance(res, Query):
            res.groupbys -= res.columns.primary().values()
        else:
            res.groupbys -= res.primary().values()
        if isinstance(res, Query) and not res.groupbys:
            res.groupbys += colargs.bind(lambda x: x.primary().values())
        # else:
        #     res.primary = args[0].primary
        return res
    return mfunc


def lift(func):
    """
    Applicative instance for Columns
    Lifts Expr -> Expr to Columns -> Columns
    """
    @wraps(func)
    def colfunc(*args, **kwargs):
        res = Columns()
        # exact replica of augment logic
        colargs = L(*args).filter(lambda x: isinstance(x, Columns))
        res.joincond += colargs.fmap(lambda x: x.joincond).sum()
        res.groupbys += colargs.bind(lambda x: x.groupbys)
        # end
        newkey = func.__name__.strip('_') + '_' + colargs.bind(lambda x: x.keys()).intersperse('_')
        res[newkey] = func(*L(*args).fmap(asExpr), **kwargs)
        return res
    setattr(Columns, func.__name__, colfunc)
    setattr(Expr, '_' + func.__name__, func)
    return colfunc


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
        if colname is not None:
            q0 = q0.fmap(lambda x: getattr(x, colname))
        elif len(q0.columns) > 1:
            q0 = q0.fmap(lambda x: x.primary())
        return q0.aggregate(lambda x: exprfunc(x, **kwargs))
    setattr(Query, strfunc.__name__[:-1], qfunc)
    return exprfunc


# this dooesn't work...
def memoize(funcname=None):
    def decorator(func):
        nonlocal funcname
        if funcname is None:
            funcname = func.__name__
        def mfunc(self, *args, **kwargs):
            if hasattr(self, func.__name__ + '_saved'):
                return getattr(self, func.__name__ + '_saved')
            res = func(*args, **kwargs)
            object.__setattr__(self, func.__name__ + '_saved', res)
            return res
        return mfunc        
    return decorator


def injective(funcname=None):
    def decorator(func):
        nonlocal funcname
        if funcname is None:
            funcname = func.__name__
        @property
        def colfunc(self, *args, **kwargs):
            if hasattr(self, funcname + '_saved'):
                return getattr(self, funcname + '_saved')
            res = func(self, *args, **kwargs)
            # if not isinstance(res, Columns):                
                # self.joincond @= res.joincond
            res = res.asCols()
            res.groupbys -= res.primary().values()
            # this is the only place other than binds where we add another groupby
            res.groupbys = self.primary().values() + res.groupbys
            # def injective_primary():
            #     return self.primary()
            # res.primary = injective_primary
            object.__setattr__(self, funcname + '_saved', res)
            return res
        return colfunc
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
        setattr(Columns, func.__name__ + '_', self.func)
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
    return BinOpExpr("+", self, other)
@lift
def __sub__(self, other):
    if type(self) is BaseExpr and type(other) is BaseExpr and 'time' in self.fieldname and 'time' in other.fieldname:
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
    if type(expr) is FuncExpr and expr.func.__name__ == "__invert__":
        return str(expr).replace("NOT ", "")
    if type(expr) is FuncExpr and expr.func.__name__ == "notnull_":
        return str(expr).replace("NOT NULL", "NULL")
    return f"NOT {expr}"
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
    return f"{expr1} LIKE \'%{likestr}%\'"
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
def roundnum_(expr, interval=1):
    return f"FLOOR({expr} :: FLOAT / {interval}) * {interval}"
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
    return f"{expr} IS NULL"
@sqlfunc
def notnull_(expr):
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



# %% ^━━━━━━━━━━━━━━━━━━ AGGREGATES ━━━━━━━━━━━━━━━━━━━━^

@aggfunc
def count_(expr):
    return f'COUNT(DISTINCT {expr})'
@aggfunc
def avg_(expr, ntile=None):
    expr = Expr._coalesce_(expr, 0)
    return f'AVG({expr})'
    # return f'AVG(COALESCE({expr}), 0)'
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



# %% ^━━━━━━━━━━━━━━━━━━━━━ MISC FUNCTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

@Lifted
def ratio_(expr, cond):    
    return expr.ifen_(cond).count_().zerodiv_(expr.count_())

@Lifted
def between(self, bound, tlimit=None, ts='ts'):
    
    # add a decorator for this
    try:
        tsvar = getattr(self, ts)
    except AttributeError:
        tsvars = self.items().filter(lambda x: '_ts' in x[0]).fmap(lambda x: x[1])
        if tsvars:
            tsvar = tsvars[0]
        else:
            raise AttributeError(f"No timestamp in {self}")
    
    if isinstance(tlimit, Columns) or isinstance(tlimit, str):
        return (tsvar > bound) & (tsvar < tlimit)
    elif isinstance(tlimit, int):
        if tlimit > 0:
            tlimit = timedelta(days=tlimit)
            return (tsvar > bound) & (tsvar < bound + tlimit)
        else:
            tlimit = timedelta(days=-tlimit)
            return (tsvar < bound) & (tsvar > bound - tlimit)
    else:
        return TypeError("Between func: invalid upper limit")


# %% ^━━━━━━━━━━━━━━━━━ OTHER FUNCTIONS ━━━━━━━━━━━━━━━━━━^



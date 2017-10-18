from LambdaQuery.misc import *
import LambdaQuery.functions
import LambdaQuery.query


# need a way to connect the table to the default row - this will happen when we instantiate the row
# no more tags - they all happen at the end

class Table(object):
    
    leftjoin = False
    groupbys = L()
    ordervar = L()
    limitvar = None
    
    # for rerouting, messy that it's here though
    derivatives = L()    
    parents = L()
    tableclass = None
    instance = None
    # classes = {}
    
    def __init__(self, tablename, alias, tableclass=None, instance=None):
        self.tablename = tablename
        self.alias = alias
        
        # misc data for rerouting purposes
        self.tableclass = tableclass
        # self.instance = instance
        
    def __repr__(self):
        return self.tablename + " AS " + self.abbrev
    
    # def __eq__(self, other):
    #     return self.tablename == other.tablename and self.alias == other.alias
    #     copy1 = copy(self).__dict__
    #     copy2 = copy(other).__dict__
    #     del copy1['instance']
    #     del copy2['instance']
    #     return copy1 == copy2
    
    def isEq(self, other):
        if self.isTable() and other.isTable():
            return self.tablename == other.tablename and self.leftjoin == other.leftjoin
        if not self.isTable() and not other.isTable():
            return self.columns == other.columns and self.joincond == other.joincond \
                and self.groupbys == other.groupbys and self.leftjoin == other.leftjoin
        return False
    
    @property
    def abbrev(self):
        ljstr = '_l' if self.leftjoin else ''
        return self.alias + ljstr + '_' + memloc(self)
    
    def getTables(self):
        return L()
    
    def isTable(self):
        return type(self) is Table
        
    def isQuery(self):
        return not self.isTable()
    
    def lj(self):
        res = copy(self)
        res.leftjoin = True
        return res
        # return lens(self).leftjoin.set(True)
    
    def primarynames(self):
        return self.tableclass().groupbys.fmap(lambda x: x.fieldname)
    
    def primaryExpr(self):
        return BaseExpr(self.primarynames()[0], self)
    
    def isJoined(self, jclist):
        # if the table is joined by primary key to a query
        return jclist.filter(lambda x: x.isJoin() 
                                               and ((x.children[0].isPrimary() and x.children[0].table == self) 
                                                    or (x.children[1].isPrimary() and x.children[1].table == self))
                                               ).exists()

    def getDept(self, jclist):
        # tables that the table depends on
        return jclist.filter(lambda x: x.isJoin() 
                                               and ((x.children[0].isPrimary() and x.children[0].table == self) 
                                                    or (x.children[1].isPrimary() and x.children[1].table == self))
                                               ).getTables() - self

class FixedTabe(Table):
    def __init__(alias, *args, tableclass=None, rawsql=None, func=None):
        if rawsql:
            self.tablename = rawsql
        else:
            self.tablename = func(*args)
        self.alias = alias
        self.tableclass = tableclass


def baseFunc(exprfunc):
    # turns a function on a BaseExpr into a function on all Exprs
    def modfunc(expr, *args, **kwargs):
        if type(expr) is BaseExpr:
            return exprfunc(expr, *args, **kwargs)
        return expr.fmap(lambda x: modfunc(x, *args, **kwargs))
    return modfunc


class Expr(object):
    
    table = Table("dummy", "dummy")
    children = L()
    iswhere = False
    isjoin = True
    
    def __repr__(self):
        return f"{self.table.abbrev}.{self.fieldname}"
    
    def __eq__(self, other):
        return str(self) == str(other)
        
    def __and__(self, other):
        return AndExpr(self.getAnds() + other.getAnds())
    
    def __or__(self, other):
        return AndExpr(self.getAnds() & other.getAnds())
    
    def __sub__(self, other):
        return AndExpr(self.getAnds() - other.getAnds())
    
    def __add__(self, other):
        return self.addExpr(other)
    
    def __matmul__(self, other):
        return (self.asQuery() @ other.asQuery()).joincond
    
    def __radd__(self, other):
        return self
    
    def __rshift__(self, func):
        return func(self)
    
    def getAnds(self):
        return L(self)
    
    def setWhere(self, val=True):
        # MUTATES
        self.iswhere = val
        return self
        # return lens.iswhere.set(val)(self)
    
    def setJoin(self, val=True):
        # MUTATES
        self.isjoin = val
        return self
        # return lens.iswhere.set(val)(self)
    
    def isRedundant(self):
        return type(self) is EqExpr and self.children[0] == self.children[1]
    
    def isPrimary(self):
        try:
            return type(self) is BaseExpr and self.fieldname in self.table.primarynames()
        except AttributeError:
            # this part is for distinct
            return self.getRef() in self.table.groupbys
    
    def isExist(self):
        return (L(self) + self.descendants()).fmap(lambda x: isinstance(x, FuncExistsExpr)).any()
    
    def getPrimary(self):
        self.table.primarynames()
        return Columns({self.table.primarynames()[0] == self.fieldname})
    
    def getTables(self):
        return self.children.getTables()
    
    def asCols(self):
        return Columns({'': self})
    
    def asQuery(self):
        return LambdaQuery.query.Query(Columns(), AndExpr(self.getAnds()))
    
    def addExpr(self, other):
        return (self.asQuery() + other.asQuery()).joincond
    
    def descendants(self):
        return copy(self.children) + self.children.bind(lambda x: x.descendants())
    
    def baseExprs(self):
        return self.descendants().filter(lambda x: type(x) is BaseExpr)
    
    def nonAggDescendants(self):
        nonaggs = copy(self.children.filter(lambda x: not x.isagg()))
        return nonaggs + nonaggs.bind(lambda x: x.nonAggDescendants())
    
    def nonAggBaseExprs(self):
        return self.nonAggDescendants().filter(lambda x: type(x) is BaseExpr)
    
    def havingGroups(self):
        # only used for deciding what to group by in the having
        if not self.isagg():
            return L(self)
        return self.children.filter(lambda x: not x.isagg() and type(x) is not ConstExpr) \
             + self.children.filter(lambda x: x.isagg() and type(x) is not AggExpr).bind(Expr.havingGroups)
    
    def modify(self, mfunc):
        reschildren = copy(self.children)
        reschildren.modify(mfunc)
        self.children = reschildren
    
    def fmap(self, mfunc):
        # reschildren = copy(self.children)
        # reschildren.modify(mfunc)
        res = copy(self)
        res.modify(mfunc)
        # res.children = reschildren
        # return lens.children.each_().modify(mfunc)(self)
        return res
    
    def getJoins(self):
        return self.children.filter(lambda x: x.isJoin())
    
    def filterexpr(self, cond):
        return lens.children.modify(lambda x: x.filter(cond))(self)
    
    def isJoin(self):
        return type(self) is EqExpr \
            and type(self.children[0]) is BaseExpr \
            and type(self.children[1]) is BaseExpr \
            and self.children[0].table != self.children[1].table
    
    def isagg(self):
        return self.children.filter(lambda x: x.isagg()).any()
    
    def iswindow(self):
        return self.children.filter(lambda x: x.iswindow()).any()
    
    def isTime(self):
        return (type(self) is BaseExpr and ('time' in self.fieldname or 'date' in self.fieldname)) \
            or (type(self) is BaseExpr and type(self.table) is not Table and self.getRef().isTime()) \
            or isinstance(self, FuncExpr) and self.children.fmap(lambda x: x.isTime()).all()
    
    def isJoin2(self):
        return isinstance(self, BinOpExpr) and self.func.__name__ in ['=','<','>','<=','>='] \
            and self.children[0].getTables() != self.children[1].getTables() \
            and self.children[0].getTables() and self.children[1].getTables()
    
    def isBool(self):
        return isinstance(self, BinOpExpr) and self.func.__name__ in ['=','<','>','<=','>=']
    
    def getEqs(self, jc):
        res = L()
        for eqexpr in jc.children.filter(lambda x: isinstance(x, EqExpr)):
            if self == eqexpr.children[0] and self.table not in eqexpr.children[1].getTables():
                res += eqexpr.children[1]
            elif self == eqexpr.children[1] and self.table not in eqexpr.children[0].getTables():
                res += eqexpr.children[0]
        return res
    
    # def setSource(self, newtable, oldtable=None):
    #     return self.fmap(lambda x: x.setSource(newtable, oldtable))
    
    def andrepr(self):
        if type(self) is BaseExpr:
            return f'{self} = TRUE'
        if type(self) is FuncExpr and self.func.__name__ == '__invert__' and type(self.children[0]) is BaseExpr:
            return f'{self.children[0]} = FALSE'
        return repr(self)
    
    @baseFunc
    def setSource(self, newtable, oldtable):        
        res = copy(self)
        if not isinstance(oldtable, L): oldtable = L(oldtable)
        if res.table in oldtable: res.table = newtable
        return res
    
    @baseFunc
    def getRef(self, oldtables=None):
        if type(self.table) is Table:
            return self
        elif oldtables and self.table not in oldtables:
            return self
        elif self.fieldname in self.table.columns:
            return self.table.columns[self.fieldname]
        else:
            return BaseExpr(self.fieldname, self.table.columns.getTables()[0])
    
    def getRefBase(self):
        res = copy(self)
        while res != res.getRef():
            res = res.getRef()
        return res


class BaseExpr(Expr):
    def __init__(self, field, table):
        self.fieldname = field
        self.table = table
    # def setSource(self, newtable, oldtable):
    #     res = copy(self)
    #     if not isinstance(oldtable, L): oldtable = L(oldtable)
    #     if res.table in oldtable: res.table = newtable
    #     return res
    def __eq__(self, other):
        if type(other) is not BaseExpr:
            return False
        return self.fieldname == other.fieldname and self.table == other.table
    def getTables(self):
        return L(self.table)
    def isagg(self):
        return False
    def iswindow(self):
        return False
    def asCols(self):
        return Columns({self.fieldname: self})


class AndExpr(Expr):
    
    def __init__(self, exprs=L()):
        self.children = exprs.bind(lambda x: x.getAnds())
    
    def __repr__(self):
        if not self.children:
            return "EmptyAndExpr" #or TRUE
        else:
            return " AND ".join(self.children.fmap(repr))
    
    def __eq__(self, other):
        return self.children.len() == other.children.len() \
            and all([self.children.sort(str)[i] == other.children.sort(str)[i] for i in range(len(self.children))])
    
    def __bool__(self):
        return bool(self.children)
    
    # def __repr__(self):
    #     return f"AndExpr({self})"
    
    def getAnds(self):
        return self.children
    
    def _filter(self, basetable, extratables=L()):
        return AndExpr(self.children.filter(lambda x: basetable in x.getTables() and not (x.getTables() - L(basetable) - L(*extratables))))
    
    def __le__(self, other):
        return self.children <= other.children
    
    def setWhere(self):
        # MUTATES
        for expr in self.children:
            expr.iswhere = True
        return self
        # return lens.children.each_().iswhere.set(True)(self) # doesn't work since each_() will return a list not an L()
        
    def groupEq(self):
        # DO NOT USE: this is mutating somehow
        # segments a joincond into equivalence classes of equals
        eqsets = L()
        for eqexpr in self.children.filter(lambda x: type(x) is EqExpr):            
            newset = True
            for i, eqset in eqsets.enumerate():
                if eqexpr.children[1] in eqset:
                    eqsets[i] = eqset + eqexpr.children[0]
                    newset = False
                elif eqexpr.children[0] in eqset:
                    eqsets[i] = eqset + eqexpr.children[1]
                    newset = False
            if newset:
                eqsets.append(eqexpr.children)
        return eqsets
    

class FuncExpr(Expr):
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.children = L(*args).filter(lambda x: isinstance(x, Expr))
        self.args = L(*args).filter(lambda x: not isinstance(x, Expr))
        self.kwargs = kwargs
    def __repr__(self):
        return self.func(*self.children, *self.args, **self.kwargs)
    def __eq__(self, other):
        if isinstance(other, FuncExpr):
            return self.children == other.children and self.func.__name__ == other.func.__name__
        return False


class AggExpr(FuncExpr):
    def isagg(self):
        return True


class WindowExpr(FuncExpr):
    def iswindow(self):
        return True


class BinOpExpr(FuncExpr):
    def __init__(self, opname, expr1, expr2):
        func = lambda x, y: f'({x} {opname} {y})'
        func.__name__ = opname
        super(BinOpExpr, self).__init__(func, expr1, expr2)
    def __eq__(self, other):
        # reminder: I introduced randomness here by using memloc
        if self.func.__name__ in ['='] and isinstance(other, BinOpExpr):
            return self.func.__name__ == other.func.__name__ \
                and self.children.sort(lambda x: str(x)) == other.children.sort(lambda x: str(x))
        elif isinstance(other, BinOpExpr):
            return self.children == other.children and self.func.__name__ == other.func.__name__
        else:
            return False


class EqExpr(BinOpExpr):
    def __init__(self, expr1, expr2):
        super(EqExpr, self).__init__('=', expr1, expr2)


class ConstExpr(Expr):
    
    allowedTypes = (int, str, float, bool, 
                    dt.datetime, dt.timedelta, type(None))#pd.Timestamp, np.float64
    
    def __init__(self, value):
        # if not isinstance(value, ConstExpr.allowedTypes):
        #     raise TypeError("Inappropriate type of ConstExpr")
        self.value = value
    
    def __repr__(self):
        if self.value is None:
            return "NULL"
        elif isinstance(self.value, dt.timedelta):
            # timedelta
            seconds = round(self.value.total_seconds())            
            if abs(seconds) >= 86400 * 5:
                days = round(seconds / 86400)
                return f"INTERVAL '{days} DAY'"
            if seconds >= 86400:
                hours = round(seconds / 3600)
                return f"INTERVAL '{hours} HOUR'"
            else:
                return f"INTERVAL '{seconds} SECOND'"
        elif isinstance(self.value, dt.datetime):# or isinstance(self.value, pd.Timestamp):
            return "\'" + self.value.strftime("%Y-%m-%d %H:%M:%S") + "\' :: TIMESTAMP"
        elif isinstance(self.value, str):
            return f"\'{self.value}\'"
        else:
            return f"{self.value}"

class SubQueryExpr(Expr):
    # value for a correlated subquery
    def __init__(self, subquery):
        self.subquery = subquery
    def __repr__(self):
        return f'({self.subquery.sql(display=False)})'


class FuncExistsExpr(FuncExpr):
    def __init__(self, expr):
        self.__dict__.update(expr.__dict__)


class ExistsExpr(Expr):
    def __init__(self, query=None, expr=None):
        alias = query.columns.__class__.__name__.lower()
        res = copy(query)
        resjc = copy(res.joincond)
        resjc.children = resjc.children.filter(lambda x: x.getTables() ^ (res.columns.getTables() + res.dependents()))
        res.joincond = resjc
        res.alias = alias
        res.ungroupby()
        self.tables = res.groupbys[-1].getTables()        
        res.columns = Columns({None: ConstExpr(None)})
        self.query = res
    
    def __repr__(self):
        # if hasattr(self, 'expr'):
        #     return repr(self.expr)
        res = self.query.sql(display=False, reduce=False, correlated=True)\
            .replace('DISTINCT \n  NULL', 'NULL')\
            .replace('\n', '\n      ')
        return f"EXISTS ({res})"
    def asCols(self):
        # if hasattr(self, 'expr'):
        #     return Columns({'exists_' + self.expr.getTables()[0].alias: self})
        return Columns({'exists_' + self.query.alias: self})
    def __eq__(self, other):
        if type(other) is ExistsExpr:
            return self.query == other.query
            # if hasattr(self, 'expr') and hasattr(other, 'expr'):
            #     return self.expr == other.expr
            # elif hasattr(self, 'query') and hasattr(other, 'query'):
            #     return self.query == other.query
        return False
    def getTables(self):
        # if hasattr(self, 'expr'):
        #     return self.expr.getTables()
        return self.tables


class Columns(dict):
    
    joincond = AndExpr()
    groupbys = L()
    leftjoin = False
    foreign_keys = L()
    # primary = None
    
    def __init__(self, *args, **kwargs):
        # don't update the joinconds, the only place where Columns itself is manually instantiated is the unit function
        dict.__init__(self)
        for col in args:
            updateUnique(self, col)
    
    def __getattr__(self, attrname):
        # if attrname == 'and_lt_ts_gt_ts_sub_ts':
            # pass            
        
        if attrname in self:
            
            # res = copy(self)
            # for key, expr in self.items():
                # del res[key]
            
            # res[attrname] = self[attrname]
            
            res = Columns({attrname: self[attrname]})
            res.joincond = self.joincond
            res.groupbys = self.groupbys
            
            # for key, derived in L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0]):
                # res[key] = derived
            
            
            # def primary_getattr():
                # return self.primary()
            # res.primary = self.primary
            return res
        return object.__getattribute__(self, attrname)
    
    def __repr__(self):
        if not self.joincond.children and not self.groupbys:
            return dict.__repr__(self) #+ f"|primary(), {self.primary.values()}"
        return f'Columns(dict= {dict.__repr__(self)}, joincond= {self.joincond}, groupbys= {self.groupbys})'#, primary= {self.primary})'
    
    def __mod__(self, other):
        res = updateUnique(self, other, makecopy=True)
        res.joincond &= other.joincond
        return res
    
    def __matmul__(self, other):
        return (self.asQuery() @ other.asQuery()).asCols()
    
    def __rshift__(self, func):
        return func(self)
    
    def primaryExpr(self):
        res = self.getTables()[0].primaryExpr()
        return Columns({None: res})
    
    @property
    def p(self):
        self.asQuery().p
    
    def findattr(self, attrname):
        try:
            return getattr(self, attrname)
        except AttributeError:
            for key, expr in self.items():
                if attrname in key:
                    return getattr(self, key)
        raise AttributeError(self, attrname)
    
    # def joinM(self):
    #     res = copy(self)
    #     newjcs = res.columns.joincond
    #     res.columns = self.columns.clear()
    #     res += self.columns.asQuery()
    #     return res
    
    def clear(self):
        res = copy(self)
        res.joincond = AndExpr()
        res.groupbys = L()
        return res
    
    def deepcopy(self):
        res = deepcopy(self)
        # is this even necessary?
        for key in copy(list(res.__dict__.keys())):
            if '_saved' in key:
                del res.__dict__[key]
        return res
    
    def asExpr(self):
        return self.values()[0]
    
    def asQuery(self):
        return LambdaQuery.query.Query(self.clear(), self.joincond, self.groupbys)#, leftjoin=self.leftjoin)
    
    def addCols(self, other):
        return (self.asQuery() + other.asQuery()).asCols()
    
    @classmethod
    def makeTable(cls, instance=None):
        # THIS IS BAD DESIGN, GET RID OF IT LATER
        cls.table = Table(cls.table.tablename, cls.table.alias, tableclass=cls, instance=instance)
    
    @classmethod
    def query(cls, cond=None):
        return cls.queryAll(cond=cond)
    
    @classmethod
    def queryAll(cls, cond=None, gbpy=True):
        newinstance = cls()
        # res = newinstance.asQuery().filter(cond).groupby(newinstance.primary.values())
        
        # if the joincond contains a joincond, it's derived and so we must get of the groupby
        # an actual joincond that was filtered for specifically
        # problem when you write Award.query(lambda x: x.project.nhm)
        # is that the groupby won't be added until after the filter is applied, so the project won't have a groupby
        
        # if res.joincond.getJoins().filter(lambda x: x.iswhere):
            # res.groupbys -= newinstance.primary.values()
        
        # return res
        # the groupby at the end actually isn't needed, they get passed on anyway by the asQuery
        res = newinstance.asQuery()
        # res.alias = cls.__name__.lower()
        res = res.filter(cond)._groupby(newinstance.groupbys)
        return res
    
    def getTables(self):
        return self.values().getTables()
    
    def getForeign(self):
        return L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0])
    # def getForeign(self):
        # return L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0]).fmap(lambda x: x[1]).getTables()
    
    def modify(self, mfunc):
        for key, expr in self.items():
            self[key] = mfunc(expr)
        # newprimary = copy(self.primary)
        # for pkey, pexpr in self.primary.items():
        #     newprimary[pkey] = mfunc(pexpr)
        # self.primary = newprimary
    
    def fmap(self, mfunc):
        res = copy(self)
        res.modify(mfunc)
        return res
    
    def firstCol(self):
        return getattr(self, self.keys()[0])
    
    def asCols(self):
        return copy(self)
    
    def keys(self):
        return L(*dict.keys(self))
    
    def values(self):
        return L(*dict.values(self))
    
    def items(self):
        return L(*dict.items(self))
        
    def getKey(self, value):
        for key, expr in self.items():
            if expr == value:
                return key
        # raise KeyError("Expr not found")
        return memloc(value)
    
    def setExists(self):
        for key, expr in self.items():
            self[key] = FuncExistsExpr(expr)
            # self[key] = ExistsExpr(expr=expr)
        return self
    
    def delKey(self, key):
        res = copy(self)
        del res[key]
        return res
    
    def delExpr(self, delexpr):
        res = copy(self)
        for key, expr in self.items():
            if expr == delexpr:
                del res[key]
            break
        return res
    
    # def primary(self):
    #     # will get overwritten
    #     return Columns({None:self.groupbys[0]})
    #     # return self.firstCol()
    
    # def primarynames(self):
    #     return self.primary.keys()
    
    
    # %% ^━━━━━━━━━━━━━━━━━ DEFINING KEYS ━━━━━━━━━━━━━━━━━━━━━^
    
    def baseExpr(self, fieldname, attrname,
                 primary=False, foreign=None, etype=int, ref=None, backref=None):
        
        self[attrname] = BaseExpr(fieldname, self.__class__.table)
        selfclass = self.__class__
        # self.__class__.table.instance = self
        
        # these are all defined every ttime it's instantiated, which is bad design. Better to define once. 
        
        if primary:
            selfclass.primary = attrname
            self.primary = getattr(self, attrname)
            self.groupbys = L(self[attrname])
            
            @LambdaQuery.functions.injective()
            @rename(selfclass.__name__.lower())
            def primary_key_cols(self):
                # try:
                return selfclass.query().filter(lambda x: getattr(x, attrname) == self.findattr(attrname))
                # except AttributeError:
                #     raise KeyError("No such primary key: ", self, attrname)
            setattr(Columns, selfclass.__name__.lower(), primary_key_cols)
        
        if foreign:
            selfclass.foreign_keys += attrname
            
            if ref is None: ref = foreign.__name__.lower()
            @LambdaQuery.functions.injective(ref)
            def primary_key(self):
                return foreign.query(lambda x: x.primaryExpr() == getattr(self, attrname))
            setattr(selfclass, ref, primary_key)
            
            if backref is None: backref = selfclass.__name__.lower() + 's'
            @LambdaQuery.functions.Kleisli
            def foreign_key(self, cond=None):
                return selfclass.query(lambda x: getattr(x, attrname) == self.primaryExpr()).filter(cond)
            setattr(foreign, backref, foreign_key.func)
            
            @LambdaQuery.functions.Kleisli
            def foreign_key_cols(self, cond=None):
                # try:
                jfield = (self.keys() ^ selfclass().keys())[0]
                return selfclass.query(lambda x: getattr(x, jfield) == getattr(self, jfield)).filter(cond)
                # except IndexError:
                    # raise KeyError("No such foreign key: ", self, selfclass.__name__.lower())
            setattr(Columns, selfclass.__name__.lower() + 's', foreign_key_cols.func)
    
    
    def setPrimary(self, *args):
        self.primary = L(*args).fmap(lambda x: getattr(self, x)).fold(lambda x, y: x % y)
        self.groupbys = L(*args).fmap(lambda x: self[x])
        
        
    # %% ^━━━━━━━━━━━━━ PUBLIC FUNCTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^
    
    def label(self, newname):
        # MUTATES
        multiple = False
        res = copy(self)
        if len(res) > 1:
            multiple = True
            # raise AttributeError("Columns must have one column only to label")
        for key, expr in res.items():
            del res[key]
            if multiple: 
                res[newname + '_' + key] = expr
            else:
                res[newname] = expr
        return res
    
    @property
    def one(self):
        # say this if you know that the row will be unique, for example with an abtest enrollment, or with exists
        res = copy(self)
        resgroupbys = copy(res.groupbys)
        resgroupbys.pop()
        res.groupbys = resgroupbys
        return res


    def join(self, other, cond):
        if inspect.isclass(other):
            other = other.query()
        res = other.filter(cond)#lambda x: cond(self, x))
        res.groupbys = self.groupbys + res.groupbys
        return res

from misc import *
import functions
import query


# need a way to connect the table to the default row - this will happen when we instantiate the row
# no more tags - they all happen at the end

class Table(object):
    
    leftjoin = False
    groupbys = L()
    ordervar = L()
    limitvar = None
    
    # for rerouting, messy that it's here though
    parents = L()
    tableclass = None
    instance = None
    # classes = {}
    
    def __init__(self, tablename, alias, tableclass=None, instance=None):
        self.tablename = tablename
        self.alias = alias
        self.tableclass = tableclass
        
    def __repr__(self):
        return self.tablename + " AS " + self.abbrev
    
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
        # res = copy(self)
        # res.leftjoin = True
        # return res
        return lens(self).leftjoin.set(True)
    
    def primarynames(self):
        return self.tableclass().primary().values().fmap(lambda x: x.fieldname)


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
        return type(self) is BaseExpr and self.table.primarynames()[0] == self.fieldname
    
    def getTables(self):
        return self.children.getTables()
    
    def asCols(self):
        return Columns({None: self})
    
    def asQuery(self):
        return query.Query(Columns(), AndExpr(self.getAnds()))
    
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
            and self.children[0].table.tablename != self.children[1].table.tablename
    
    def isagg(self):
        return self.children.filter(lambda x: x.isagg()).any()
    
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
        else:
            return self.table.columns[self.fieldname]
        


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
    


class AndExpr(Expr):
    
    def __init__(self, exprs=L()):
        self.children = exprs.bind(lambda x: x.getAnds())
    def __repr__(self):
        if not self.children:
            return "EmptyAndExpr" #or TRUE
        else:
            return " AND ".join(self.children.fmap(repr))
    def getAnds(self):
        return self.children
    def _filter(self, basetable, extratables=L()):
        return AndExpr(self.children.filter(lambda x: not (x.getTables() - L(basetable) - L(*extratables))))
    def __le__(self, other):
        return self.children <= other.children
    def setWhere(self):
        # MUTATES
        for expr in self.children:
            expr.iswhere = True
        return self
        # return lens.children.each_().iswhere.set(True)(self)
        
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


class AggExpr(FuncExpr):
    def isagg(self):
        return True


class BinOpExpr(FuncExpr):
    def __init__(self, opname, expr1, expr2):
        func = lambda x, y: f'({x} {opname} {y})'
        func.__name__ = opname
        super(BinOpExpr, self).__init__(func, expr1, expr2)
    def __eq__(self, other):
        # THIS IS THE FUCKING RANDOMNESS I INTRODUCED
        if self.func.__name__ in ['='] and isinstance(other, BinOpExpr):
            return self.func.__name__ == other.func.__name__ \
                and self.children.sort(lambda x: str(x)) == other.children.sort(lambda x: str(x))
        else:
            return self.__dict__ == other.__dict__
        # else:
            # return object.__eq__(self, other)


class EqExpr(BinOpExpr):
    def __init__(self, expr1, expr2):
        super(EqExpr, self).__init__('=', expr1, expr2)


class ConstExpr(Expr):
    
    allowedTypes = [int, str, float, bool, 
                    dt.datetime, dt.timedelta, pd.Timestamp, np.float64, type(None)]
    
    def __init__(self, value):
        if type(value) not in ConstExpr.allowedTypes:
            raise TypeError("Inappropriate type of ConstExpr")
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
        elif isinstance(self.value, dt.datetime) or isinstance(self.value, pd.Timestamp):
            return "\'" + self.value.strftime("%Y-%m-%d %H:%M:%S") + "\' :: TIMESTAMP"
        elif isinstance(self.value, str):
            return f"\'{self.value}\'"
        else:
            return f"{self.value}"


class Columns(dict):
    
    joincond = AndExpr()
    groupbys = L()
    
    def __init__(self, *args, **kwargs):
        # don't update the joinconds, the only place where Columns are manually instantiated is the unit function
        dict.__init__(self)
        for col in args:
            updateUnique(self, col)    
    
    def __getattr__(self, attrname):
        if attrname in self:
            res = Columns({attrname: self[attrname]})
            res.joincond = self.joincond
            res.groupbys = self.groupbys
            res.primary = self.primary
            return res
        return object.__getattribute__(self, attrname)
    
    def __repr__(self):
        if not self.joincond.children and not self.groupbys:
            return dict.__repr__(self)
        return f'Columns(dict= {dict.__repr__(self)}, joincond= {self.joincond}, groupbys= {self.groupbys})'
    
    def __mod__(self, other):
        return updateUnique(self, other, makecopy=True)
    
    def __matmul__(self, other):
        return (self.asQuery() @ other.asQuery()).asCols()
    
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
        return query.Query(self.clear(), self.joincond, self.groupbys)
    
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
        res = newinstance.asQuery().filter(cond)
        # if the joincond doesn't contain an actual joincond that was filtered for specifically
        if not res.joincond.getJoins().filter(lambda x: not x.iswhere):
            res.groupbys += newinstance.primary().values()
        return res
    
    def getTables(self):
        return self.values().getTables()
    def getSaved(self):
        return L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0]).fmap(lens[1])
    def getForeign(self):
        return L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0]).fmap(lens[1]).getTables()
    
    def modify(self, mfunc):
        for key, expr in self.items():
            self[key] = mfunc(expr)
    
    def fmap(self, mfunc):
        res = copy(self)
        res.modify(mfunc)
        return res
    
    def firstCol(self):
        return getattr(self, self.keys()[0])
    
    def asCols(self):
        return self
    
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
    
    def primary(self):
        # will get overwritten
        return self.firstCol()
    
    def primarynames(self):
        return self.primary().keys()
    
    
    # %% ^━━━━━━━━━━━━━━━━━ DEFINING KEYS ━━━━━━━━━━━━━━━━━━━━━^
    
    def baseExpr(self, fieldname, attrname,
                 primary=False, foreign=None, etype=int, ref=None, backref=None):
        
        self[attrname] = BaseExpr(fieldname, self.__class__.table)
        selfclass = self.__class__
        
        if primary:
            
            def self_primary():
                return getattr(self, attrname)
            self.primary = self_primary
            
            @functions.injective()
            def primary_key_cols(self):
                # try:
                return selfclass.query(lambda x: getattr(x, attrname) == getattr(self, attrname))
                # except AttributeError:
                #     raise KeyError("No such primary key: ", self, attrname)
            setattr(Columns, selfclass.__name__.lower(), primary_key_cols)
            
            @functions.Kleisli
            def foreign_key_cols(self, cond=None):
                # try:
                joinfield = (self.keys() ^ selfclass().keys())[0]
                return selfclass.query(lambda x: getattr(x, joinfield) == getattr(self, joinfield))
                # except IndexError:
                    # raise KeyError("No such foreign key: ", self, selfclass.__name__.lower())
            setattr(Columns, selfclass.__name__.lower() + 's', foreign_key_cols.func)
            
            
        if foreign:
            
            if ref is None: ref = foreign.__name__.lower()
            @functions.injective(ref)
            def primary_key(self):
                return foreign.query(lambda x: x.primary() == getattr(self, attrname))
                    # .groupby(self.groupbys)# + self.primary().values())
                # self @= res
                # return res
            setattr(selfclass, ref, primary_key)
            
            if backref is None: backref = selfclass.__name__.lower() + 's'
            @functions.Kleisli
            def foreign_key(self, cond=None):
                res = selfclass.query(lambda x: getattr(x, attrname) == self.primary()).filter(cond)#.groupby(self.groupbys)
                # if not self.groupbys:
                #     res.groupbys += self.primary().values()
                # else:
                #     res.groupbys += self.groupbys
                return res
            setattr(foreign, backref, foreign_key.func)
    
    
    def setPrimary(self, *args):
        def multi_primary():
            return L(*args).fmap(lambda x: getattr(self, x)).fold(Columns.__mod__)
        self.primary = multi_primary
        
        
    # %% ^━━━━━━━━━━━━━ PUBLIC FUNCTIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^
    
    def label(self, newname):
        # MUTATES
        if len(self) > 1:
            raise AttributeError("Columns must have one column only to label")
        expr = self.asExpr()
        del self[self.keys()[0]]
        self[newname] = expr
        return self


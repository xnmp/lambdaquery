from expr import *
from do_notation import *

class Query(Table, Monad):
    
    alias = 'query'
    
    def __init__(self, columns, joincond=AndExpr(), groupbys=L()):
        self.columns = columns
        self.joincond = joincond
        self.groupbys = groupbys
    
    def __repr__(self):
        return f"Query(columns= {self.columns}, \n      joincond= {self.joincond}, \n      groupbys= {self.groupbys}, \n      memloc= '{memloc(self)}')"
    
    def __rshift__(self, func):
        return func(self)
    
    def __call__(self, ffunction):
        return self.filter(ffunction)
    
    # def __getattr__(self, attrname):
    #     if attrname in object.__getattribute__(self, 'columns'):
    #         res = copy(self)
    #         res.columns = getattr(object.__getattribute__(self, 'columns'), attrname)
    #         return res
    #     else:
    #         return object.__getattribute__(self, attrname)
    
    def __add__(self, other):
        return addQuery(self, other)
        
    def __matmul__(self, other):
        return addQuery(self, other)
    
    def __radd__(self, other):
        return self
    
    def primarynames(self):
        return self.groupbys.fmap(lambda x: x.fieldname)
    
    def modify(self, mfunc, *args):
        # note that this isn't the same as fmap
        
        if 'joincond' in args or not args:
            sjoincond = copy(self.joincond)
            sjoincond.modify(mfunc)
            self.joincond = sjoincond
        
        if 'columns' in args or not args:            
            scolumns = copy(self.columns)
            scolumns.modify(mfunc)
            self.columns = scolumns
        
        if 'groupbys' in args or not args:
            sgroupbys = copy(self.groupbys)
            sgroupbys.modify(mfunc)
            self.groupbys = sgroupbys
            # self.groupbys >>= lens.modify(mfunc)
            
        # self.joincond.modify(mfunc)
        # self.columns.modify(mfunc)
        # self.groupbys.modify(mfunc)
    
    def modified(self, mfunc):
        res = copy(self)
        for attrname in ['joincond', 'columns', 'groupbys']:
            attr = getattr(res, attrname)
            attr = attr.fmap(mfunc)
            setattr(res, attrname, attr)
        return res
    
    # def clearCols(self):
    #     # res = copy(self)
    #     # res.columns = self.columns.clear()
    #     # return res        
    #     return lens.columns.modify(Columns.clear)(self)
    
    @property
    def tablename(self):
        return self.abbrev
    
    def allExprs(self):
        return self.groupbys + self.columns.values() + self.joincond.children
    
    def setSource(self, newtable, oldtable=None):
        self.modify(lambda x: x.setSource(newtable, oldtable))
    
    def getTables(self):
        return self.allExprs().getTables()
    
    def isagg(self):
        return self.columns.values().filter(lambda x: x.isagg()).any()
    
    def asQuery(self):
        return self
    
    def subQueries(self):
        return self.getTables().filter(lambda x: not type(x) is Table)
    
    def joinM(self):
        res = copy(self)
        newjcs = res.columns.joincond
        res.columns = self.columns.clear()
        res += self.columns.asQuery()
        
        # undo the columns added
        # for key in res.columns.keys():
        #     if key not in self.columns.keys():
        #         del res.columns[key]
        
        return res
    
    
    # %% ^━━━━━━━━━━━━━━ PUBLIC FUNCTIONS ━━━━━━━━━━━━━━━^
        
    def aggregate(self, aggfunc=None, iswindow=False, existsq=False, lj=True, distinct=False):
        # Aggregate a query into a single row
        if aggfunc is not None:
            return self.fmap(aggfunc).aggregate()
        res = Columns()
        for key, expr in self.columns.items():
            dummylabel = 'agg_' + memloc(expr)
            res[key] = BaseExpr(dummylabel, self)
            del self.columns[key]
            self.columns[dummylabel] = expr
        return res
    
    
    def distinct(self, *args):
        res = self.columns.deepcopy()
        if type(res) is Columns:
            for key in res.keys():
                res[key] = BaseExpr(key, self)
        else:
            for key, expr in res.items():
                res[key] = BaseExpr(expr.fieldname, self)
                self.columns = BaseExpr('*', self.columns.getTables()[0]).asCols()
        return Query.unit(res)
    
    
    def sample(self, n=1000):
        return self.sort('RANDOM()').limit(n).distinct()
    
    
    def sort(self, *args):
        # res = copy(self)
        # res.ordervar += L(*args)
        # return res
        return lens.ordervar.modify(lambda x: x + L(*args))(self)
    
    
    def limit(self, n=50):
        # res = copy(self)
        # res.limitvar = n
        return lens.limitvar.set(n)(self)
    
    
    def fmap(self, ffunc, *args, **kwargs):
        res = copy(self)
        res.columns = ffunc(self.columns, *args, **kwargs)
        return res.joinM()
        # return lens.columns.modify(lambda x: ffunc(x, *args, **kwargs))(self).joinM()
    
    
    def exists(self):
        return self.lj().asCols().firstCol().notnull_()
    
    
    def groupby(self, exprlist=None):
        if exprlist is None and not self.groupbys:
            exprlist = L(self.primary().values())
        res = copy(self)
        res.groupbys += L(*exprlist)
        return res
        # return self >> lens.groupbys.modify(lambda x: x + L(*exprlist))
    
        
    def bind(self, bfunc):
        res = copy(self)#.joinM()
        # hi = bfunc(self.columns)
        assert not res.columns.joincond.children
        # res.columns = hi.asCols()        
        qnew = bfunc(self.asCols())#bfunc(self.columns) 
        res += qnew
        if qnew.columns.keys():
            res.columns = qnew.columns
        res.groupbys += self.columns.primary().values() + qnew.groupbys
        return res.joinM()
    
    
    # def isDerived(self):
    #     self.joincond.getJoins()
    #     self.columns.getForeign()
    
    
    @classmethod
    def unit(cls, *args, filter=None, **kwargs):
        # a = args[0].asQuery()
        # b = args[1].asQuery()
        # hh = a + b
        inargs = L(*kwargs.items()).fmap(lambda x: x[1].label(x[0]))        
        res = L(*args, *inargs).fmap(lambda x: x.asQuery()).sum()
        if filter is not None:
            res = res.filter(filter)
        # assert res.columns.
        # res.joinM()
        return res
    
    
    def asCols(self):
        res = copy(self.columns)
        res.joincond = self.joincond
        res.groupbys = self.groupbys
        return res
        # return (lens.joincond.set(self.joincond) & lens.groupbys.set(self.groupbys))(res.columns)
    
    
    def filter(self, cond=None):
        res = copy(self)
        if not cond:
            return res
        newconds = cond(res.columns)
        res.joincond += AndExpr(newconds.values()).setWhere()
        res.joincond += newconds.joincond
        return res.joinM()
    
    
    # def joinOn(self, cond=None):
    #     return self.filter
    
    
    def lj(self):
        # a question: should subqueries be left joined?
        res = copy(self)
        # res.leftjoin = True
        # for tab in res.groupbys.getTables()[1:]:
        #     tab.leftjoin = True
        for tab in res.columns.getTables():            
            tab.leftjoin = True
        res.modify(lambda x: x.setWhere(False), 'joincond')
        return res



# %% ^━━━━━━━━━━━━━━━━━━━━━ THE HEAVY LIFTER ━━━━━━━━━━━━━━━━━━━━━━━^
    
def addQuery(self, other):
    # THIS IS THE MOST CRUCIAL PART OF THE WHOLE LIBRARY
    # ONLY a query -> query, if you want to add anything else turn it into a query first
    # the crucial thing to make it work it that you need to mutate "other", 
    # It's actually the ONE thing you actually want to mutate
    
    res, othercopy = copy(self), copy(other)
    
    # hi = res.columns.getSaved().filter(lambda x: x.getTables()[0] in other.joincond.getTables())
    # res.joincond &= hi.fold(Columns.__and__, mzero=Columns()).joincond
    
    tables0, tables1 = self.joincond.getTables(), other.joincond.getTables()
    jtables = tables0 & tables1
    
    for tab1 in tables1:
        skiptable = False
        cond1 = other.joincond._filter(tab1, jtables)
        
        
        for tab0 in tables0.filter(lambda x: x.__dict__ == tab1.__dict__):
            
            cond0 = res.joincond._filter(tab0, jtables)
            # mcond0 = cond0.setSource(Expr.table, tab0)
            # mcond1 = cond1.setSource(Expr.table, tab1)
            
            # leftover0, leftover1 = mcond0 - mcond1, mcond1 - mcond0
            # orcond = mcond0 | mcond1
            
            if not identifyTable(cond0, cond1, tab0, tab1): continue
            
            if tab1 is not tab0:
                other.setSource(tab0, tab1)
                res.setSource(tab0, tab1)
                moveJoins(cond0, cond1, tab0, tab1, res, other)
                skiptable = True
            else:
                # TODO: something should go here but not sure what
                pass
                
        if not skiptable:
            jtables += L(tab1)
            res.joincond &= cond1
    
    #this is if we want it to be completely no frills, but bug free
    # res.joincond &= other.joincond 
    
    res.columns %= other.columns
    res.groupbys += other.groupbys
    return res


def identifyTable(cond0, cond1, tab0, tab1):
        # mcond0 = cond0.setSource(Expr.table, tab0)
        # mcond1 = cond1.setSource(Expr.table, tab1)
        # leftover0, leftover1 = mcond0 - mcond1, mcond1 - mcond0
        # orcond = mcond0 | mcond1
        
        # tab1 and tab0 are both joined to the same table by the same thing
        # eqtables = orcond.children.filter(Expr.isJoin).getTables()
        # if not (Expr.table in eqtables and eqtables.len() > 1): continue
        # if (leftover0.children or tab0.leftjoin) and (leftover1.children or tab1.leftjoin): continue
                
        andcond = cond0 & cond1
        
        # orcond.baseExprs().filter(lambda x: x.table in (tab0, tab1)).groupby(lambda x: x.fieldname).filter(lambda x: x.value.len() > 1).fmap(lambda x: x.value)
        # print(orcond)
        
        # hi0 = andcond.baseExprs().filter(lambda x: x.table == tab0)
        # hi1 = andcond.baseExprs().filter(lambda x: x.table == tab1)
        # if 'milestone' in tab0.tablename and tab1 is not tab0:
        
        for expr0 in andcond.baseExprs().filter(lambda x: x.table == tab0):
            for expr1 in andcond.baseExprs().filter(lambda x: x.table == tab1):
                if expr0.fieldname == expr1.fieldname:
                    if expr0.getEqs(andcond) ^ expr1.getEqs(andcond):      
                        return True
        return False
        # return not leftover0.children.exists() and not leftover1.children.exists()


def moveJoins(cond0, cond1, tab0, tab1, res, other):
    # MUTATES
    # given two conditions, replace the tables, or them, and add the joinconds to tab0 and tab1
    # subtract the joinconds and add remainders to case whens in the selects
    
    cond1 = cond1.setSource(tab0, tab1)
    leftover1 = cond1 - cond0
    leftover0 = cond0 - cond1
    
    @baseFunc
    def addJoins(expr, cond):
         return Expr._ifen_(expr, cond) if expr.table == tab0 else expr
    
    if leftover0.children:
        res.modify(lambda x: addJoins(x, leftover0), 'columns')
        res.joincond >>= lens.children.modify(lambda x: x - leftover0.children)
    
    if leftover1.children:
        other.modify(lambda x: addJoins(x, leftover1), 'columns')
        other.joincond >>= lens.children.modify(lambda x: x - leftover1.children)
        # other >>= lens.columns.modify(lambda x: x - leftover0.children)

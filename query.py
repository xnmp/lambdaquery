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
    
    # def __eq__(self, other):
    #     return self.columns.__dict__ == other.columns.__dict__ \
    #             and self.joincond.__dict__ == other.joincond.__dict__ \
    #             and self.groupbys.__dict__ == other.groupbys.__dict__
    
    def __add__(self, other):
        return addQuery(self, other)
        
    def __matmul__(self, other):
        return addQuery(self, other)
    
    def __radd__(self, other):
        return self
    
    def __or__(self, other):
        res = copy(self)
        res.joincond &= other.joincond
        res.columns %= other.columns
        res.groupbys += other.groupbys
        return res
    
    def getPrimary(self):
        for key, expr in self.columns.items():
            if key == self.columns.primary.keys()[0]:
                return getattr(self.columns, key)
        raise KeyError("No Primary Key", self)
    
    # def getPrimary(self):
    #     return self.columns.items().filter(lambda x: x[1] in self.groupbys)\
    #                .fmap(lambda x: Columns({x[0]: x[1]})).fold(Columns.__mod__)
    
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
        # ALWAYS RETURN A GODDAMN COPY
        return copy(self)
    
    def subQueries(self):
        return self.getTables().filter(lambda x: not type(x) is Table)
    
    def joinM(self):
        res = copy(self)
        newjcs = res.columns.joincond
        res.columns = self.columns.clear()
        res @= self.columns.asQuery()
        
        # undo the columns added
        # for key in res.columns.keys():
        #     if key not in self.columns.keys():
        #         del res.columns[key]
        
        return res
    
    def dependents(self):
        # all tables joined by primary key to a table of its columns
        res = L()
        for table in self.getTables():    
            jclist = self.joincond.children.filter(lambda x: self.columns.getTables() <= x.getTables())
            if table.isJoined(jclist):
                res += table
        return res
    
    
    def ungroupby(self):
        # MUTATES
        grouptable = self.groupbys[-1].table
        pnames = grouptable.primarynames()
        
        for gpbyexpr in reversed(self.groupbys):
            if gpbyexpr.fieldname in pnames and gpbyexpr.table == grouptable:
                self.groupbys.pop()
            # elif self.columns.values().filter(lambda x: x.isagg()).getTables() <= 
            elif gpbyexpr.isPrimary():
                break
    
    
    # %% ^━━━━━━━━━━━━━━ PUBLIC FUNCTIONS ━━━━━━━━━━━━━━━^
    
    def aggregate(self, aggfunc=None, iswindow=False, existsq=False, lj=True, distinct=False):
        # Aggregate a query into a single row
        if aggfunc is not None:
            return self.fmap(aggfunc).aggregate()
        res = Columns()
        newsource = copy(self)
        for key, expr in newsource.columns.items():
            dummylabel = key + '_' + memloc(expr)
            # dummylabel = 'agg_' + key #+ memloc(expr)
            res[key] = BaseExpr(dummylabel, newsource)
            del newsource.columns[key]
            newsource.columns[dummylabel] = expr
            # SUPER IMPORTANT POINT: REMOVE A GROUP BY HERE
            newsource.ungroupby()
        
        if len(res) == 1 and not newsource.groupbys:
            newsource = deepcopy(newsource)
            res = Columns({res.keys()[0]: SubQueryExpr(newsource)})
        
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
        # oldgroupbytables = self.groupbys.bind(lambda x: x.fmap(lambda y: y.getTables()))
        # oldgroupbytables = self.groupbys.getTables()
        res = Query.unit(res)
        res.groupbys = self.groupbys
        # res.groupbys = self.groupbys.fmap(lambda x: x.setSource(res, oldgroupbytables))
        return res
    
    
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
        res.columns = ffunc(self.asCols(), *args, **kwargs)
        return res.joinM()
        # return lens.columns.modify(lambda x: ffunc(x, *args, **kwargs))(self).joinM()
    
    
    def exists(self):
        return self.lj().asCols().firstCol().notnull_().one
    
    
    def groupby(self, exprlist=None):
        if not self.groupbys and exprlist is None:
            exprlist = L(self.columns.primary)
        if not isinstance(exprlist, list):
            exprlist = L(exprlist)
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
        res @= qnew
        if qnew.columns.keys():
            res.columns = qnew.columns
        res.groupbys += qnew.groupbys
        return res.joinM()
    
    
    # def isDerived(self):
    #     self.joincond.getJoins()
    #     self.columns.getForeign()
    
    
    @classmethod
    def unit(cls, *args, filter=None, limit=None, sort=None, **kwargs):
        # a = args[0].asQuery()
        # b = args[1].asQuery()
        # hh = a + b
        kwargs = L(*kwargs.items()).fmap(lambda x: x[1].label(x[0]))
        res = L(*args, *kwargs).fmap(lambda x: x.asQuery()).fold(lambda x, y: x @ y)
        
        if filter is not None: res = res.filter(filter)
        if limit is not None: res = res.limit(limit)
        if sort is not None: res = res.sort(sort(res.columns).values())
        # res.joinM()
        
        return res
    
    
    def asCols(self):
        res = copy(self.columns)
        res.joincond = self.joincond
        res.groupbys = self.groupbys
        return res
        # return self.columns >> lens.joincond.set(self.joincond) & lens.groupbys.set(self.groupbys)
    
    
    @property
    def one(self):
        res = self.asCols()
        resgroupbys = copy(res.groupbys)
        resgroupbys.pop()
        res.groupbys = resgroupbys
        return res
    
    
    def join(self, cond):
        return self.filter(cond, join=True)
    
    
    def filter(self, cond=None, join=False):
        res = copy(self)
        if not cond: return res
        
        newconds = cond(res.asCols())
        
        # for key, derived in res.columns.getForeign():            
        #     if derived.groupbys == newconds.groupbys and derived.getTables()[0] in newconds.getTables():
        #         # this mutates the derived col, be really careful
        #         derived.joincond &= AndExpr(newconds.values())
        
        # newjc = AndExpr(newconds.values()).setWhere()
        # if not join: 
        # newjc = newjc.setWhere()
        
        # res @= AndExpr(newconds.values()).setWhere()
        # res @= newconds.joincond
        
        res.joincond &= AndExpr(newconds.values()).setWhere()
        res.joincond &= newconds.joincond
        
        return res #.joinM()
    
    
    # def joinOn(self, cond=None):
    #     return self.filter(cond=cond)
    
    
    def lj(self):
        # a question: should subqueries be left joined?
        res = copy(self)
        # res.leftjoin = True
        # for tab in res.groupbys.getTables()[1:]:
        #     tab.leftjoin = True
        for tab in res.columns.getTables():            
            tab.leftjoin = True
        res.modify(lambda x: x.setWhere(False), 'joincond')
        
        # for key, col in self.columns.getForeign(): #L(*self.__dict__.items()).filter(lambda x: '_saved' in x[0])
        
        # for k, v in self.columns.__dict__.items():
        #     if '_saved' in k:
        #         v.asExpr().table.leftjoin = True        
        
        # tricky part with dependent tables
        for table in self.dependents():
            ifcond = res.joincond.children.filter(lambda x: x.getTables() == L(table))
            if ifcond:
                table.leftjoin = True
            
        ifcond = res.joincond.children.filter(lambda x: x.getTables() <= res.dependents())
        if ifcond:
            for key, expr in self.columns.items():
                rescolumns = copy(res.columns)
                rescolumns[key] = Expr._ifen_(expr, AndExpr(ifcond))
                res.columns = rescolumns
                resjoincond = copy(res.joincond)
                resjoincond.children -= ifcond
                res.joincond = resjoincond
                
        return res


    @property
    def p(self):
        print(self.sql(reduce=False))
        
    
    # explicit group by
    def groupbyExplicit(self, *args):
        return self.groupby(*L(*args)\
                   .fmap(lambda x: x(self.columns).values() if hasattr(x, '__call__') else x.values()))
    

# %% ^━━━━━━━━━━━━━━━━━━━━━ THE HEAVY LIFTER ━━━━━━━━━━━━━━━━━━━━━━━^
    
def addQuery(self, other, addcols='both'):
    # THIS IS THE MOST CRUCIAL PART OF THE WHOLE LIBRARY
    # ONLY a query -> query, if you want to add anything else turn it into a query first
    # most important point to make it work is that you need to mutate "other", 
    # It's actually the ONE thing you actually WANT to mutate
    
    res, othercopy = copy(self), copy(other)
    
    # hi = res.columns.getSaved().filter(lambda x: x.getTables()[0] in other.joincond.getTables())
    # res.joincond &= hi.fold(Columns.__and__, mzero=Columns()).joincond
    
    tables0, tables1 = self.joincond.getTables(), other.joincond.getTables()
    jtables = tables0 & tables1
    
    for tab1 in tables1:
        skiptable = False
        cond1 = other.joincond._filter(tab1, jtables)
        
        tables0.filter(lambda x: x == tab1)
        
        for tab0 in tables0.filter(lambda x: x.__dict__ == tab1.__dict__):
            cond0 = res.joincond._filter(tab0, jtables)
            
            if identifyTable(cond0, cond1, tab0, tab1):
                
                if tab1 is not tab0 and tab0 not in jtables \
                    and (tab1 not in other.groupbys.getTables() or (tab1 in other.groupbys.getTables() and tab0 in self.groupbys.getTables())):
                                        
                    # pp0 = AndExpr(self.joincond.children.filter(lambda x: tab0 in x.getTables()))
                    # pp1 = AndExpr(other.joincond.children.filter(lambda x: tab1 in x.getTables()))
                    
                    moveJoins(cond0, cond1, tab0, tab1, res, other)
                    skiptable = True
                else:
                    # TODO: something should go here but not sure what
                    pass
                # break
                
        if not skiptable:
            jtables += L(tab1)
            res.joincond &= cond1
    
    # this is if we want it to be completely no frills, but bug free
    # res.joincond &= other.joincond
    if addcols == 'both':
        res.columns %= other.columns
    elif addcols != 'left':
        res.columns = other.columns
    
    # the below actually makes sense - it's like we're taking a cross join
    res.groupbys += other.groupbys
    return res


def identifyTable(cond0, cond1, tab0, tab1):
        # mcond0 = cond0.setSource(Expr.table, tab0)
        # mcond1 = cond1.setSource(Expr.table, tab1)
        # leftover0, leftover1 = mcond0 - mcond1, mcond1 - mcond0
        # if leftover0: False
        # orcond = mcond0 | mcond1
        
        # tab1 and tab0 are both joined to the same table by the same thing
        # eqtables = orcond.children.filter(Expr.isJoin).getTables()
        # if not (Expr.table in eqtables and eqtables.len() > 1): continue
        # if (leftover0.children or tab0.leftjoin) and (leftover1.children or tab1.leftjoin): continue
        
        # short circuit it if we want it to be no frills
        return False
        
        
        andcond = cond0 & cond1
        
        # orcond.baseExprs().filter(lambda x: x.table in (tab0, tab1)).groupby(lambda x: x.fieldname).filter(lambda x: x.value.len() > 1).fmap(lambda x: x.value)
        # print(orcond)
        
        # if tab1 is not tab0:
        #     pass
        
        # hi0 = andcond.baseExprs().filter(lambda x: x.table == tab0)
        # hi1 = andcond.baseExprs().filter(lambda x: x.table == tab1)
        # if 'milestone' in tab0.tablename and tab1 is not tab0:
        
        for expr0 in andcond.baseExprs().filter(lambda x: (x.table == tab0)):# & x.isPrimary()):
            for expr1 in andcond.baseExprs().filter(lambda x: (x.table == tab1)):# & x.isPrimary()):
                if expr0.fieldname == expr1.fieldname:
                    if expr0.getEqs(andcond) ^ expr1.getEqs(andcond):
                        # if tab1 is not tab0:
                            
                        return True
                        
        return False
        # the old condition
        # return not leftover0.children.exists() and not leftover1.children.exists()


def moveJoins(cond0, cond1, tab0, tab1, res, other):
    # MUTATES
    # given two conditions, replace the tables, or them, and add the joinconds to tab0 and tab1
    # subtract the joinconds and add remainders to case whens in the selects
    
    other.setSource(tab0, tab1)
    # res.setSource(tab0, tab1)
    cond1 = cond1.setSource(tab0, tab1)
    leftover0 = cond0 - cond1
    leftover0.children = leftover0.children.filter(lambda x: not x.isJoin())
    leftover1 = (cond1 - cond0)
    leftover1.children = leftover1.children.filter(lambda x: not x.isJoin())
    
    @baseFunc
    def addJoins(expr, cond):
         return Expr._ifen_(expr, cond) #if expr.table == tab0 else expr
    
    if leftover0.children:
        res.modify(lambda x: addJoins(x, leftover0), 'columns')
        res.joincond >>= lens.children.modify(lambda x: x - leftover0.children)
    
    if leftover1.children:
        other.modify(lambda x: addJoins(x, leftover1), 'columns')
        other.joincond >>= lens.children.modify(lambda x: x - leftover1.children)
    
    
# only move if con0 and cond1 are actual filter conds, not joinconds

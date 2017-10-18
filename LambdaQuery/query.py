from LambdaQuery.expr import *
from LambdaQuery.do_notation import *

class Query(Table, Monad):
    
    alias = 'query'
    
    def __init__(self, columns, joincond=AndExpr(), groupbys=L(), leftjoin=False):
        self.columns = columns
        self.joincond = joincond
        self.groupbys = groupbys
        self.leftjoin = leftjoin
    
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
    
    def __getitem__(self, cols):
        if not isinstance(cols, str):
            return self.fmap(lambda x: L(*cols).fmap(lambda y: getattr(x, y)).fold(lambda y, z: y % z))
        return self.fmap(lambda x: getattr(x, cols))
    
    def getPrimary(self):
        for key, expr in self.columns.items():
            if key == self.columns.primary.keys()[0]:
                return getattr(self.columns, key)
        raise KeyError("No Primary Key", self)
    
    def primaryExpr(self):
        return BaseExpr(self.columns.keys()[0], self)
        # return self.groupbys[0]
    
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
    
    def allDescendants(self):
        return self.allExprs().bind(lambda x: L(x) + x.descendants())
    
    def setSource(self, newtable, oldtable=None):
        self.modify(lambda x: x.setSource(newtable, oldtable))
    
    def getTables(self):
        return self.allExprs().getTables()
    
    def isagg(self):
        return (self.columns.values() + self.joincond).filter(lambda x: x.isagg()).any()
    
    def isOuter(self):
        return self.subQueries().exists()
    
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
        # res = L()
        # for table in self.getTables():    
        #     jclist = self.joincond.children.filter(lambda x: self.columns.getTables() <= x.getTables())
        #     if table.isJoined(jclist) and table not in self.groupbys.getTables():
        #         res += table
        # return res
        res = L()
        for k, v in self.columns.__dict__.items():
            if '_saved' in k:
                res += v.getTables()
        
        # ALSO: any table that's joined to it or its dependents, 
        # and which isn't something that is a dependent of something before it in the groupby list
        res += (self.columns.getTables()).bind(lambda x: x.derivatives)
        
        return res
    
    def queryTables(self):
        return (self.columns.getTables() + self.groupbys.getTables()).bind(lambda x: L(x) + x.derivatives)
    
    def ungroupby(self):
        # MUTATES
        grouptable = self.groupbys[-1].table
        
        primarygpby = False
        for gpbyexpr in reversed(self.groupbys):
            if gpbyexpr.isPrimary() and gpbyexpr.table == grouptable:
                selfgroupbys = copy(self.groupbys)
                selfgroupbys.pop()
                self.groupbys = selfgroupbys
                primarygpby = True
            elif gpbyexpr.table == grouptable and not primarygpby:
                selfgroupbys = copy(self.groupbys)
                selfgroupbys.pop()
                self.groupbys = selfgroupbys
                break
            else:
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
        
        if len(res) == 1 and not newsource.groupbys and not res.values().filter(lambda x: type(x.getRef()) is WindowExpr):
            newsource = deepcopy(newsource)
            res = Columns({res.keys()[0]: SubQueryExpr(newsource)})
        
        return res
    
    
    def agg(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs)
    
    def apply(self, *args, **kwargs):
        return self.aggregate(*args, **kwargs).asQuery()
    
    def distinct(self, *args):
        res = self.columns.deepcopy()
        source = copy(self)
        if type(res) is Columns:
            if not source.columns.values().filter(lambda x: x.isPrimary()).any():
                source.groupbys = source.columns.values()
            for key in res.keys():
                res[key] = BaseExpr(key, source)
            res.groupbys = res.values()
        else:
            for key, expr in res.items():
                res[key] = BaseExpr(expr.fieldname, source.columns.getTables()[0])#, source)
            # source.columns = BaseExpr('*', source.columns.getTables()[0]).asCols()
            source.columns = source.columns.getTables()[0].primaryExpr().asCols()
            res.groupbys = source.groupbys.fmap(lambda x: x.setSource(source, x.getTables()))
        return Query.unit(res)
    
    
    def sample(self, n=1000):
        return self.sort('RANDOM()').limit(n).distinct()
    
    
    def sort(self, *args):
        res = copy(self)
        if not hasattr(args[0], '__call__'):
            res.ordervar += L(*args)
        else:
            res.ordervar += args[0](self.columns).values()        
        return res
        
        # return lens.ordervar.modify(lambda x: x + cond(self))(self)
    
    
    def limit(self, n=50):
        # res = copy(self)
        # res.limitvar = n
        return lens.limitvar.set(n)(self)
    
    
    def fmap(self, ffunc, *args, **kwargs):
        res = copy(self)
        res.columns = ffunc(self.asCols(), *args, **kwargs)
        return res.joinM()
        # return lens.columns.modify(lambda x: ffunc(x, *args, **kwargs))(self).joinM()
    
    
    def exists(self, col=False, _func='notnull_'):
        
        alias = self.columns.__class__.__name__.lower()
        
        # # as a subquery (shouldn't be able to move into the subquery here)
        # res = self.lj().fmap(lambda x: x.firstCol())
        # res.alias = alias
        # res.ungroupby()
        # expr = Expr._notnull_(BaseExpr(res.columns.keys()[0], res))
        # res = Columns({'exists_' + alias: expr})
        # return res
        
        # as an exists expr
        # return ExistsExpr(self).asCols()
        
        # as a straight up (screws up one-to-one ness)
        res = self.lj().asCols().firstCol().__getattr__(_func)().one.label('exists_' + alias).setExists()
        return res
        
        # as a bool any
        # return self.lj().fmap(lambda x: x.firstCol()).any()
    
    def notExists(self):
        return self.exists(_func='isnull_')
    
    def _groupby(self, exprlist=None):
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
        
        # res.columns = hi.asCols()
        qnew = bfunc(self.asCols())#bfunc(self.columns)
        assert not res.columns.joincond.children
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
        try:
            aa = args[0].asQuery()
            bb = args[1].asQuery()
            hh = addQuery(aa, bb)    
        except:
            pass
        
        kwargs = L(*kwargs.items()).fmap(lambda x: x[1].label(x[0]))
        res = L(*args, *kwargs).fmap(lambda x: x.asQuery()).fold(lambda x, y: x @ y)
        
        if filter is not None: res = res.filter(filter)
        if limit is not None: res = res.limit(limit)
        if sort is not None: res = res.sort(sort(res.columns).values())
        # res.joinM()
        
        return res
    
    
    def asCols(self, makecopy=True):
        res = copy(self.columns) if makecopy else self.columns
        res.joincond = self.joincond
        res.groupbys = self.groupbys
        # res.leftjoin = self.leftjoin
        return res
        # return self.columns >> lens.joincond.set(self.joincond) & lens.groupbys.set(self.groupbys)
    
    
    @property
    def one(self):
        res = self.asCols()
        resgroupbys = copy(res.groupbys)
        resgroupbys.pop()
        res.groupbys = resgroupbys
        return res
    
    
    def filter(self, cond=None):
        res = copy(self)
        
        if not cond: return res
        
        # newconds = cond(res.asCols())
        
        # we need to leave out the makecopy to save the derived
        # leaving out the makecopy mutates the columns which is why we have to joinm at the end
        newconds = cond(res.asCols(makecopy=False))
        
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
        
        return res.joinM()
    
    
    def lj(self):
        res = copy(self)
        
        # if res.columns.getTables() <= res.groupbys.getTables():
        #     # a question: should subqueries be left joined? only if it's one of the base tables
        #     res.leftjoin = True
        #     return res
        
        ljtables = res.columns.fmap(lambda x: x.getRef()).getTables() + self.dependents()
        for tab in ljtables:
            tab.leftjoin = True
        
        res.modify(lambda x: x.setWhere(False) if x.getTables() ^ ljtables else x, 'joincond')
        for subq in res.subQueries():
            subq.modify(lambda x: x.setWhere(False) if x.getTables() ^ ljtables else x, 'joincond')
        
        # res.modify(lambda x: x.setWhere(False), 'joincond')
        # ifcond = res.joincond.children.filter(lambda x: x.getTables() <= res.dependents())
        
        # tricky part with dependent tables
        # if ifcond:
        
        depts = res.dependents().filter(lambda x: x in res.getTables())
        if depts:
            notnullcond = depts.fmap(lambda x: Expr._notnull_(x.primaryExpr()))
            for key, expr in self.columns.items():
                rescolumns = copy(res.columns)
                rescolumns[key] = Expr._ifen_(expr, AndExpr(notnullcond))
                res.columns = rescolumns
        return res
    
    
    def groupby(self, *args):
        # explicit group by - add it one level back
        newgroups = L(*args).bind(lambda x: x(self.columns).values() if hasattr(x, '__call__') else x.values())
        res = copy(self)
        res.ungroupby()
        res.groupbys += newgroups + (self.groupbys - res.groupbys)
        # res.groupbys = newgroups + res.groupbys
        return res
        
    def group(self, *args):
        args = L(*args).fmap(lambda x: (lambda y: getattr(y, x)) if type(x) is str else x)
        return self.fmap(lambda x: args.fmap(lambda y: y(x)).fold(lambda y, z: y % z)).distinct()
    
# class FixedTable(Table):
#     def __init__(self, q0):
#         self.query = q0
#         self.alias = q0.alias
#     def __repr__(self):
#         return repr(self.query)


# %% ^━━━━━━━━━━━━━━━━━━━━━ THE HEAVY LIFTER ━━━━━━━━━━━━━━━━━━━━━━━^

def addQuery(self, other, addcols='both', debug=False):
    
    res, othercopy = copy(self), copy(other)
    
    tables0, tables1 = self.joincond.getTables(), other.joincond.getTables()
    jtables = tables0 & tables1
    
    # # start with the ones that are in common:
    # for tab1 in jtables:
    #     cond0 = res.joincond._filter(tab1, jtables)
    #     cond1 = other.joincond._filter(tab1, jtables)
    #     if cond1 - cond0 and (cond0 | cond1).children.filter(lambda x: x.isJoin()):
    #         moveJoins(cond0, cond1, tab1, tab1, res, other)
    
    # now for the ones that AREN'T in common:
    skiptable = False
    for tab1 in tables1 - tables0:
        cond1 = other.joincond._filter(tab1, jtables)
        for tab0 in tables0.filter(lambda x: x.isEq(tab1)):
            cond0 = res.joincond._filter(tab0, jtables)
            
            if identifyTable(cond0, cond1, tab0, tab1, res, other) and \
                (tab0 not in res.groupbys.getTables() 
                 or (res.groupbys.fmap(lambda x: x.setSource(tab1, tab0)) == other.groupbys)):
                
                moveJoins(cond0, cond1, tab0, tab1, res, other)
                # other.setSource(tab0, tab1)
                
                skiptable = True
                jtables += L(tab0)
                break
        if not skiptable:
            jtables += L(tab1)
            res.joincond &= cond1
    
    # short circuit it
    # res.joincond &= other.joincond
    
    if addcols == 'both':
        res.columns %= other.columns
    elif addcols != 'left':
        res.columns = other.columns
    res.groupbys += other.groupbys
    return res


def identifyTable(cond0, cond1, tab0, tab1, res, other):
    # tab1 and tab0 are both joined to the same table by the same thing
    # the extras are just where conds
    
    # cond0 = res.joincond._filter(tab0, other.joincond.getTables())
    # cond1 = other.joincond._filter(tab1, res.joincond.getTables())
    
    andcond = cond0 & cond1
    
    cond1 = cond1.setSource(tab0, tab1)
    leftover0 = cond0 - cond1
    leftover1 = cond1 - cond0
    
    for expr0 in andcond.baseExprs().filter(lambda x: (x.table == tab0)):# & x.isPrimary()):
        for expr1 in andcond.baseExprs().filter(lambda x: (x.table == tab1)):# & x.isPrimary()):
            if expr0.fieldname == expr1.fieldname:
                if expr0.getEqs(andcond) ^ expr1.getEqs(andcond):                    
                    return leftover0.getTables() <= L(tab0) and leftover1.getTables() <= L(tab0) + other.getTables()
    return False


def moveJoins(cond0, cond1, tab0, tab1, res, other):
    # MUTATES
    # given two conditions, replace the tables, or them, and add the joinconds to tab0 and tab1
    # subtract the joinconds and add remainders to case whens in the selects
    
    jtables = res.joincond.getTables() & other.joincond.getTables()
    
    other.setSource(tab0, tab1)
    # cond1.setSource(tab0, tab1)
    # res.setSource(tab0, tab1)
    cond1 = other.joincond._filter(tab0, jtables)
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
    
    if leftover1.children:# <= 1:
        other.modify(lambda x: addJoins(x, leftover1), 'columns')
        other.joincond >>= lens.children.modify(lambda x: x - leftover1.children)
    
    
# only move if cond0 and cond1 are actual filter conds, not joinconds

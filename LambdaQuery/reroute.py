from LambdaQuery.expr import *
from LambdaQuery.misc import *
from LambdaQuery.query import addQuery
from itertools import product

def addParents(self, reset=False, debug=False):
    if reset:
        resetParents(self)
    for table in self.getTables():
        table.parents += L(self)
        addParents(table, debug=debug)

def resetParents(self):
    self.parents = L()
    for table in self.getTables():
        resetParents(table)

def ancestors(self):
    return copy(self.parents) + self.parents.bind(ancestors)

def toReroute(self):
    # if one of its parents is a child of another parent
    # if len(self.parents) <= 1:
        # return False
    for parent1, parent2 in product(self.parents, self.parents):
        parent1 in ancestors(parent2) or parent2 in ancestors(parent1)
        if parent1 in ancestors(parent2) or parent2 in ancestors(parent1):
            return True
    return False

def getRerouteTables(self):
    addParents(self, reset=True)
    return self.getTables().filter(toReroute)

def hasEqs(table, inner, outer):
    # (redundant) if the inner table is entirely replaceable 
    # ie if every baseexpr of the table to be moved out appears in an eqexpr
    # TODO: if one side of the equals is a funcexpr, then the func must be injective
    eqexprs = inner.joincond.children\
        .filter(lambda x: isinstance(x, EqExpr) 
                          and x.getTables().len() > 1
                          and (x.children[0].table == table or
                               x.children[1].table == table)
                )
    haseqs = inner.allExprs().bind(lambda x: L(x) + x.descendants())\
                             .filter(lambda x: type(x) is BaseExpr)\
                             .filter(lambda x: x.table == table) <= AndExpr(eqexprs).descendants()
    return haseqs

@baseFunc
def replaceTable(expr, basetable, parenttable, outertable=None):
    """
    (redundant)
    for all baseexprs in "parenttable", if its sourcetable is "basetable"
    then replace it with something that it's equal to, and return the replacement
    outertable is there to add a joincond at the end
    """
    if expr.table == basetable:
        res = expr.getEqs(parenttable.joincond)[0] # guaranteed to return something
        
        if outertable:
            # make a joincond on the outside
            dummylabel = 'out_move_' + parenttable.columns.getKey(res)
                        
            # TODO: only do this if outertable doesn't already have an equivalent condition
            # existing = expr.getEq(AndExpr(outertable.joincond.getJoins().filter(lambda x: parenttable in x.getTables())))
            # if not (existing and EqExpr(existing.getRef(), expr) in parenttable.joincond.children):

            parenttable.columns[dummylabel] = res
            outertable.joincond.children += EqExpr(BaseExpr(dummylabel, parenttable), expr)
        return res
    return expr





# %%  _______   ________  _______    ______   __    __  ________  ______  __    __   ______
# %% /       \ /        |/       \  /      \ /  |  /  |/        |/      |/  \  /  | /      \
# %% $$$$$$$  |$$$$$$$$/ $$$$$$$  |/$$$$$$  |$$ |  $$ |$$$$$$$$/ $$$$$$/ $$  \ $$ |/$$$$$$  |
# %% $$ |__$$ |$$ |__    $$ |__$$ |$$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$$  \$$ |$$ | _$$/
# %% $$    $$< $$    |   $$    $$< $$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$$$  $$ |$$ |/    |
# %% $$$$$$$  |$$$$$/    $$$$$$$  |$$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$ $$ $$ |$$ |$$$$ |
# %% $$ |  $$ |$$ |_____ $$ |  $$ |$$ \__$$ |$$ \__$$ |   $$ |    _$$ |_ $$ |$$$$ |$$ \__$$ |
# %% $$ |  $$ |$$       |$$ |  $$ |$$    $$/ $$    $$/    $$ |   / $$   |$$ | $$$ |$$    $$/
# %% $$/   $$/ $$$$$$$$/ $$/   $$/  $$$$$$/   $$$$$$/     $$/    $$$$$$/ $$/   $$/  $$$$$$/


# %% ^━━━━━━━━━━━━━━━ MOVING IN ━━━━━━━━━━━━━━━━━━^

def canMoveIn(expr, inner, outer):
    
    if inner.groupbys.getTables().filter(lambda x: x.leftjoin):
        # return expr in inner.joincond.children
        return expr.getTables() <= inner.baseTables()
    
    if inner.iswindow():
        return expr in inner.joincond.children
    
    if expr in inner.joincond.children:
        return True
    
    # if it's only part of a single subquery
    willmove = outer.getTables().filter(lambda x: x.isQuery() and expr.getTables() ^ x.getTables()).len() <= 1
    
    # the tables of expr are a subset of inner's tables, 
    # plus everything that's joined by primary key to one of inner's tables
    canmove = expr.getTables() <= outer.joincond.children \
                .filter(lambda x: x.isJoin()
                        and ((x.children[0].isPrimary() and 
                              x.children[1].table in inner.getTables())
                             or
                             (x.children[1].isPrimary() and 
                              x.children[0].table in inner.getTables())
                             )
                       ).getTables() + inner.getTables()
    
    res = canmove and willmove and not expr.isagg() and not expr.isExist()
    #and not (L(expr) + expr.descendants()).fmap(lambda x: x.exists()).any()
    return res


def moveIn(expr, inner, outer):
    inner.joincond.children += expr#.getRef(oldtables=L(inner))
    outer.joincond.children -= expr

# def moveIn(table, inner, outer):
#     # aggregate exprs can get moved in, but the groupbys must move as well    
#     # outer.joincond.children.filter(lambda x: table in x.getTables()).modify(lambda x: x.getRef())
    
#     # DON'T MOVE IN ANYTHIN THAT'S CONNECTED TO A SUBQUERY, 
#     # UNLESS you're already groupby by the primary key of whatever 
    
#     # joinconds just get moved wholesale: this is allowed because of the canmovein condition
    
#     # if not table.isTable():
    
#     res = outer.joincond.children\
#                      .filter(lambda x: table in x.getTables())\
#                      .fmap(lambda x: (x, not (x.getRef().getTables() - inner.groupbys.getTables() - inner.columns.getTables()) 
#                              ))

#     for expr in outer.joincond.children\
#                      .filter(lambda x: table in x.getTables())\
#                      .filter(lambda x: not (x.getRef().getTables() - inner.groupbys.getTables() - inner.columns.getTables()) 
#                              ):
#                      # .filter(lambda x: not x.getTables().filter(lambda y: not y.isTable()).exists())
#                               # .filter(lambda x: x.getTables() <= inner.getTables()):
        
        
#         inner.joincond.children += expr.getRef()
#         outer.joincond.children -= expr


def moveInHaving(inner, outer, debug=False):
    # move in the HAVING conditions, this facilitates a cleanup later on
    # move it in if the inner query is the only one of its tables.
    
    for outexpr in outer.joincond.children.filter(lambda x: x.getTables() <= (inner.getTables() + L(inner))
                                               and not x.isagg() # and x.getTables()[0] == inner 
                                               and x.getRef().isagg()
                                               ):
        
        havingexpr = outexpr.getRef(oldtables=L(inner))
        # TODO: the descendants of havingexpr are already grouped by
        # havingexpr.descendants.getTables() <= inner.groupbys
        
        if debug:
            print("MOVED_IN():", havingexpr)
            print(outer.sql(reduce=False, debug=True))
        
        inner.joincond.children += havingexpr
        outer.joincond.children -= outexpr
        
        # get rid of references that were only there to provide for the aggregate joincond (broken)
        # if delete:
        # for key, inexpr in inner.columns.items():
        #     # if the expr doesn't get refered to by the outer table
        #     if inexpr in havingexpr.descendants() and not inner.parents.bind(lambda x: x.allDescendants()).filter(lambda x: x.getRef() == inexpr).exists():
        #         del inner.columns[key]


def moveInAll(inner, outer, debug=False):
    """
    the loop is to account for the case when a table isn't joined by primary key, 
    but it's joined to something that's joined by primary key 
    """
    finish = False
    moved = False
    # i=0
    
    while not finish:
        # print(i)
        finish = True
        for expr in outer.joincond.children:
            if canMoveIn(expr, inner, outer):
                finish = False
                moveIn(expr, inner, outer)
                if debug:
                    print("MOVED_IN():", expr)
                    moved = True
    
    if debug and moved:
        print(outer.sql(reduce=False, debug=True))


# %% ^━━━━━━━━━━━━ MOVING OUT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

def canMoveOut(table, inner, outer):
    
    if inner.groupbys.getTables().filter(lambda x: x.leftjoin):
        return table in outer.baseTables() and table not in inner.baseTables()
    
    # isisolated = inner.joincond._filter(table, getRerouteTables(outer)).getTables() <= L(table)
        
    # we ALWAYS want it IN unless the table appears in more than one subquery, in this case it's like we're "factorizing" it
    # shouldmove = table.parents.filter(lambda x: outer in x.parents).len() > 1
    
    # NEW: just move if it's either an outer groupby or joined to an outer groupby, and the same is not true for inner
    willmove = table in outer.groupbys.getTables() + outer.getTables().filter(lambda x: x.isJoined(outer.joincond.children))
    isgroupedinner = table in inner.groupbys.getTables() #+ inner.getTables().filter(lambda x: x.isJoined(inner.joincond.children))
    
    # can't move out anything that appears in an agg expr
    canmove = not inner.allExprs().filter(lambda x: x.isagg()).getTables()\
                <= inner.allExprs().filter(lambda x: table in x.getTables()).getTables()
    
    return willmove and not inner.leftjoin and (not isgroupedinner) and canmove # and isisolated 


def moveOut(table, inner, outer):
    
    for expr in inner.joincond.children.filter(lambda x: table in x.getTables()):
        outer.joincond.children += expr#.getRef(oldtables=L(inner))
        inner.joincond.children -= expr
    
    # # mutates HARD
    # inner.groupbys.modify(lambda x: replaceTable(x, table, inner))
    # inner.columns.modify(lambda x: replaceTable(x, table, inner))
    # inner.joincond.modify(lambda x: replaceTable(x, table, inner, outer))
    # # get rid of tautological exprs of the form x = x
    # inner.joincond.children = inner.joincond.children.filter(lambda x: not x.isRedundant())
    

def moveOutAll(inner, outer, debug=False):
    moveout = True
    while moveout:
        moveout = False
        for table in getRerouteTables(outer).filter(lambda x: x in inner.getTables()):
            if canMoveOut(table, inner=inner, outer=outer):
                moveOut(table, inner=inner, outer=outer)
                if debug:
                    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
                    print("MOVEOUT", table)
                    print(outer.sql(reduce=False, debug=True))
                    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')    
                moveout = True
                break



# %% ^━━━━━━━━━━━━━━━━━ REROUTING ━━━━━━━━━━━━━━━━━━━━^

def canReroute(expr, basetable, target, outer=None):
    # the target is the inner query
    # if the tables of expr are a subset of the tables of the target... and not aggregate and not constant
    # and we don't reroute exists exprs
    isexists = expr.isExist() or (type(expr) is FuncExpr and expr.func.__name__ in ['notnull_','isnull_'])
    
    return basetable in expr.getTables() and expr.getTables() <= target.getTables() \
           and type(expr) is not ConstExpr and (not expr.isagg()) \
           and not isexists and not expr.isBool() #and type(expr) is BaseExpr


def reroute(expr, basetable, target, outer=None):
    """
    target is the "intermediate" table
    for an expr, if it can be rerouted, then make it as a select of that target, 
    and return another expr that references it
    """
    if canReroute(expr, basetable, target):
        # print('REROUTED():', expr)
        dummylabel = target.columns.getKey(expr)
        if 'reroute_' not in dummylabel and dummylabel not in target.columns:
            dummylabel = 'reroute_' + dummylabel
            target.columns[dummylabel] = expr
            if not isGrouped(expr, target) and type(expr) is BaseExpr:
                # DON'T KNOW
                # add it as a groupby if it's not already
                target.groupbys += expr.table.primaryExpr()
        return BaseExpr(dummylabel, target)
    elif not expr.getTables() ^ target.getTables():
        return expr
    return expr.fmap(lambda x: reroute(x, basetable, target))


def rerouteAll(table, inner, outer):
    outer.joincond.modify(lambda x: reroute(x, basetable=table, target=inner))
    outer.columns.modify(lambda x: reroute(x, basetable=table, target=inner))
    outer.groupbys.modify(lambda x: reroute(x, basetable=table, target=inner))


def canRerouteTable(basetable, inner, outer):
    
    # all baseexprs of outer containing basetable
    allexprs = outer.allDescendants()\
        .filter(lambda x: type(x) is BaseExpr and basetable in x.getTables())
    
    isgrouped = allexprs.fmap(lambda x: isGrouped(x, inner)).all()
    
    # if not isgrouped:
    # print('not grouped:', basetable)
    
    # if the second condition is true then we just add basetable to the groupbys of inner
    return isgrouped or basetable in outer.groupbys.filter(lambda x: x.isPrimary()).getTables()


def isGrouped(expr, target):
    '''
    if there exists a unique value of expr for each row in target
    an expr is grouped if any of the following are true:
    1. expr is one of the target's groupbys
    2. the tables of expr are part of the target's primary grouped by tables
    2. expr is in the getEqs of the target's groupbys
    3. expr is joined by primary key to something thats's grouped
    4. expr is in the getEqs of something that's grouped
    '''
    
    # if expr is grouped by, or is joined to anything of the primary keys, 
    # everything equal to a groupby expr
    allgroups = getGroups(target)
    return expr in allgroups or expr.getTables() <= allgroups.filter(lambda x: x.isPrimary()).getTables() \
        or (not expr.isagg() and expr.getRef().isagg())


def getAllEqsRecursive(expr, jclist):
    """
    get everything in jclist that's equal to expr
    """
    jclistnew = L(expr)
    jclistold = None
    while jclistnew != jclistold:
        jclistold = copy(jclistnew)
        jclistnew += jclistnew.bind(lambda x: getAllEqs(x, jclist))
    return jclistnew

def getAllEqs(expr, jclist):
    """
    get everything in jclist that's equal to expr (1st order)
    differs from Expr.getEqs in that you're allowed something 
    like self.hide_ts == self.ts which has the same tables on both sides
    """
    res = L()
    for eqexpr in jclist.filter(lambda x: isinstance(x, EqExpr)):
        if expr == eqexpr.children[0] or eqexpr.children[0] in res:
            res += eqexpr.children[1]
        elif expr == eqexpr.children[1] or eqexpr.children[1] in res:
            res += eqexpr.children[0]
    return res

def getGroups(target):
    '''
    everything in target.joincond that has a unique value for each value of the groupbys
    a table is a primary table of target if target is grouped by the primary key of the table
    1. groupbys themselves
    2. anything equal to a groupby
    3. anything whose tables are a subset of the primary tables of target
    4. anything equal to this... etc
    '''
    
    res = target.groupbys
    resold = None
    
    while res != resold:
        resold = copy(res)
        res += res.bind(lambda x: getAllEqsRecursive(x, target.joincond.children))
        for eqexpr in getEqExprs(target):
            if eqexpr.children[0].getTables() <= res.filter(lambda x: x.isPrimary()).getTables():
                res += eqexpr.children[1]
            elif eqexpr.children[1].getTables() <= res.filter(lambda x: x.isPrimary()).getTables():
                res += eqexpr.children[0]
    return res

def getEqExprs(table):
    return table.joincond.children.filter(lambda x: type(x) is EqExpr)

# def getGroupJoins(target, groupbys):
#     # everything joincond of target that's joined to a specified set of exprs
#     return target.joincond.children.filter(lambda x: isinstance(x, EqExpr) 
#                                     and x.children[0] in groupbys or x.children[0] in groupbys) \
#                 .fmap(lambda x: x.children[1] if x.children[0] in groupbys else x.children[0])

# def getGroups(target):
    
#     # everything joined to a groupby of the target, or joined to one of those things, etc
#     # or, if the groupby is a primary key of a table, everything joined to an expr of that table
    
#     pkey_joins = target.joincond.children \
#                 .filter(lambda x: isinstance(x, EqExpr)) \
#                 .filter(lambda x: isGrouped(x.children[0], target) or isGrouped(x.children[1], target))
#     other_groupbys = pkey_joins.fmap(lambda x: x.children[1] if x.children[0] in target.groupbys else x.children[0])
#     return other_groupbys + target.groupbys

# %% ^━━━━━━━━━━━━━━━━━ OTHER ━━━━━━━━━━━━━━━^

def isInvalid(expr, isjc=False):
    # an expr that compares a table to an aggregate of that table
    # ie an expr whose children contain the table, and an expr whose getref is an aggexpr containing the same table
    # if it's a joincond and it contains a left joined table then all bets are off
    btables = expr.getTables().filter(lambda x: x.isTable())
    innertables = expr.baseExprs()\
                      .filter(lambda x: not x.table.isTable())\
                      .fmap(lambda x: x.getRef())\
                      .bind(lambda x: L(x) + x.descendants())\
                      .filter(lambda x: isinstance(x, AggExpr)).getTables()        
    
    return expr.getRef().isagg() and (btables ^ innertables or (btables.filter(lambda x: x.leftjoin) and isjc))

def aggedTables(expr):
    return expr.children\
               .bind(lambda x: L(x) + x.descendants())\
               .filter(lambda x: isinstance(x, AggExpr)).getTables()
# .fmap(lambda x: x.getRef())

def hasExists(inner, outer):
    return outer.allDescendants().filter(lambda x: inner in x.getTables() and isinstance(x, FuncExistsExpr)).exists()

def canMerge(self, subquery=False):
    # it doesn't contain an aggexpr with a child whose getref is an aggexpr
    # the nonagg columns aren't aggs
    # OR it doesn't contain 
    # ie an expr that compares a table to an aggregate of that table
    canmerge = self.allDescendants()\
                   .filter(lambda x: isinstance(x, AggExpr))\
                   .bind(lambda x: x.baseExprs())\
                   .fmap(lambda x: x.getRef())\
                   .filter(lambda x: x.isagg())\
                   .notExists()
    if not subquery and self.isagg():
        subq = self.columns.values().filter(lambda x: not x.isagg() and not x.isPrimary()).notExists()
        # subq = self.columns.values().filter(lambda x: not x.isagg() and x.getRef().isagg()).notExists()
    else:
        subq = True
    
    if self.subQueries().bind(lambda x: x.aggedTables()).len() > 1:
        return False
    
    return canmerge and self.joincond.children.filter(lambda x: isInvalid(x, isjc=True)).notExists() \
        and subq and (self.columns.values() + self.groupbys).filter(lambda x: isInvalid(x, isjc=False)).notExists()

def isSum(q0):
    return q0.allDescendants().filter(lambda y: type(y) is AggExpr and y.func.__name__ == 'sum_').exists()

def canMergeTable(tab0, tab1, subquery=False):
    if tab0.iswindow() or tab1.iswindow():
        return False
        # return tab1.joincond == tab0.joincond
    
    if (tab1 in tab0.getTables() and not canMerge(tab1)) or (tab0 in tab1.getTables() and not canMerge(tab0)):
        return False
    
    
    if tab1 not in tab0.getTables() and tab0 not in tab1.getTables():
        tab11 = copy(tab1)
        tab00 = copy(tab0)
        return (tab00 @ tab11).aggedTables() == tab00.aggedTables()
    
    go = (tab0.groupbys <= tab1.groupbys and tab0 in tab1.getTables() and not isSum(tab0) and not isSum(tab1)) or \
              (tab1.groupbys <= tab0.groupbys and tab1 in tab0.getTables() and not isSum(tab0) and not isSum(tab1)) or \
              ((isSum(tab0) or isSum(tab1)) and tab0.joincond == tab1.joincond and tab0.groupbys == tab1.groupbys) or \
              (tab0.groupbys == tab1.groupbys and not isSum(tab0) and not isSum(tab1))
              # or ((isSum(tab0) or isSum(tab1)) and (not tab0.isagg() or not tab1.isagg()) and tab0.groupbys == tab1.groupbys)
    # go &= (subquery or tab0.columns.values().filter(lambda x: not x.isagg() and not x.isPrimary()).notExists())
    
    return go and not tab1.joincond.children.filter(lambda x: isInvalid(x, isjc=True)) \
              and not tab0.joincond.children.filter(lambda x: isInvalid(x, isjc=True)) \
              and tab1.limitvar == tab0.limitvar and tab1.ordervar == tab0.ordervar
              # and not (not subquery and not tab0.isagg())


def mergeGrouped(self, debug=False, subquery=False):
    # merge tables that have the same groupbys, because only then does it make sense to merge them
    
    # checktables = self.subQueries()
    
    if canMerge(self, subquery=subquery):
        checktables = L(self) + self.subQueries()
    else:
        checktables = self.subQueries()
    
    # alltablegroups = L()
    
    alltablegroups = checktables.partition(lambda x, y: canMergeTable(x, y, subquery=subquery)).filter(lambda x: len(x) > 1)
    # if canMerge(self, subquery=subquery):
    # else:
        # alltablegroups = self.subQueries().partition(lambda x, y: canMergeTable(x, y, subquery=subquery)).filter(lambda x: len(x) > 1)
    #     alltablegroups = L(L(self) + checktables.filter(lambda x: x.groupbys.exists() #and not hasExists(x, self)
    #                                                       and x.groupbys <= self.groupbys #.fmap(lambda x: x.getRef())
    #                                                       and not x.allDescendants().filter(lambda y: type(y) is AggExpr and y.func.__name__ == 'sum_')
    #                                                       and not x.limitvar
    #                                                         )).filter(lambda x: len(x) > 1)
    
    # if not alltablegroups:
    #     alltablegroups = checktables.filter(lambda x: x.isQuery()
    #                                         and not x.allDescendants().filter(lambda y: type(y) is AggExpr and y.func.__name__ == 'sum_')
    #                                         )\
    #                        .groupby(lambda x: (x.groupbys, x.leftjoin))\
    #                        .filter(lambda x: len(x.value) > 1)\
    #                        .fmap(lambda x: x.value)
    
    
    
    for tablegroup in alltablegroups:
        
        # if self in tablegroup, then use the columns = 'left' option in addquery. 
        # then modify columns ith mergeexpr
        
        if self in tablegroup:
            merged = tablegroup.fold(lambda x, y: addQuery(x, y, addcols='left'))
            merged.modify(lambda x: mergeExpr(x, oldtables=tablegroup))
            self.__dict__.update(merged.__dict__)
        else:
            merged = tablegroup.fold(lambda x, y: x @ y)
            self.setSource(merged, oldtable=tablegroup)
        
        # if self not in tablegroup:
        
        # if self in tablegroup:
            
        #     # moveInHaving(inner=table, outer=self)
            
        #     # merge with a subquery and convert the agg exprs
        #     merged.modify(lambda x: mergeExpr(x, oldtables=tablegroup))
        #     # self.modify(lambda x: mergeExpr(x, oldtables=L(merged)))
        #     # merged.columns = self.columns
        #     self.__dict__.update(merged.__dict__)
        #     break
    
    if alltablegroups and self.subQueries():
        mergeGrouped(self)
    
    # for subtable in self.subQueries():
    #     mergeGrouped(subtable)

def mergeExpr(expr, oldtables):
    res = expr.getRef(oldtables=oldtables)
    
    
    
    # if AndExpr(expr.baseExprs().filter(lambda x: x.table.leftjoin).fmap(Expr._isnull_)).children:        
    return res

def canCleanUp(self):
    inner = self.getTables()[0]
    return len(self.getTables()) == 1 and type(inner) is not Table \
      and self.columns.values().fmap(lambda x: isinstance(x, BaseExpr)).all() \
      and not self.joincond.children # and self.groupbys.fmap(lambda x: x.getRef()) == inner.groupbys

def cleanUp(self):
    # get rid of the redundant outer query
    newquery = self.getTables()[0]
    for expr in self.joincond.children:
        newquery.joincond += expr.getRef()
    for key, expr in self.columns.items():
        newquery.columns[key] = newquery.columns[expr.fieldname]
        del newquery.columns[expr.fieldname]
    self.__dict__.update(newquery.__dict__)

def copyTable(table, parent, addcond=True, debug=False):
    # when a table can't be rerouted
    # also needed to clone join conditions into subqueries
    tablecopy = deepcopy(table)
    tablecopy.alias += '_copy'
    parent.setSource(tablecopy, oldtable=table)
    if addcond:
        # add condition to the parent that the two tables are the same
        eqexprs = tablecopy.primarynames().fmap(lambda x: EqExpr(BaseExpr(x, table), BaseExpr(x, tablecopy)))
        parent.joincond &= AndExpr(eqexprs)
        
        # if type(tablecopy) is Query:
        #     eq_gpexprs = tablecopy.groupbys.zip(table.groupbys).fmap(lambda x: EqExpr(x[0], x[1]))
        #     parent.joincond &= AndExpr(eq_gpexprs)
        

def connectTables(queries, parent, debug=False):
    
    commongps = queries.fmap(lambda x: x.groupbys).fold(lambda x, y: x ^ y)#.fmap(lambda x: x.getRefBase()))
    
    for i, q in enumerate(queries[1:]):
        for expr in commongps:
            q.columns.getKey(commongps)
            expr0 = reroute(expr, basetable=expr.getTables()[0], target=queries[i])
            expr1 = reroute(expr, basetable=expr.getTables()[0], target=q)            
            parent.joincond.children += EqExpr(expr0, expr1)
    
    # for i, parent in table.parents.enumerate():
    #     if i == 0: continue
    #     if debug: print(f'CONNECTED(): {table}')
    #     tablecopy = copy(table)
    #     parent.setSource(tablecopy, table)
    #     for gparent in parent.parents:
    #         # add a joincond
    #         gparent.joincond.children += EqExpr(table.primaryExpr(), tablecopy.primaryExpr())
    #         # now reroute it
    #         gparent.joincond.modify(lambda x: reroute(x, basetable=table, target=table.parents[i-1]))
    #         gparent.joincond.modify(lambda x: reroute(x, basetable=tablecopy, target=parent))
    #     table = tablecopy

def copyJoinconds(basetable, source, target):
    res = AndExpr(source.joincond.children.filter(lambda x: x.isJoin() and basetable in x.getTables()))
    target.joincond &= res


# %% ^━━━━━━━━━━━━━━━━━━━━━━━ REDUCE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

def reduceQuery(self, debug=False, subquery=False):
    """
    make everything appear in scope - the dependency graph has to be a tree
    this means we have to redirect everything via its aggregate queries
    """
    
    # finish = False
    # while not finish:
    #     finish = True
    
    if debug:
        print(' # %% ^━━━━━━━━━━━━━━━━━ REDUCING... ━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print(self.sql(reduce=False, debug=True))        
    
    # if the groupbys are left joined, then make the query left joined
    if self.isQuery() and self.groupbys and self.groupbys.getTables().fmap(lambda x: x.leftjoin).all():
        self.leftjoin = True        
        for table in self.groupbys.getTables():
            table.leftjoin = False
            for der in table.derivatives:
                der.leftjoin = False
    
    # STEP 1: MERGE SUBQUERIES WITH THE SAME GROUP BYS
    selfcopy = copy(self)
    
    if self.isagg() and not subquery:
        newgroupbys = self.columns.values().filter(lambda x: not x.isagg() and not x.isPrimary())
        self.groupbys = newgroupbys
        
    mergeGrouped(self, subquery=subquery)
    if debug:
        if str(selfcopy.__dict__) != str(self.__dict__):
            print(' # %% ^━━━━━━━━━━━━━━━━━ AFTER MERGING ━━━━━━━━━━━━━━━━━━━^ ')
            print(self.sql(reduce=False, debug=True))
            print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        else:
            print("NO TABLES MERGED")
    # terminate if no subqueries
    if not self.subQueries():
        if debug:
            print("NOTHING TO REDUCE... DONE")
            print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        return    
    
    
    # STEP 2: MOVING IN
    addParents(self)
    # hi = self.subQueries().filter(lambda x: not (ancestors(x) ^ self.subQueries()))    
    for newtable in self.subQueries().filter(lambda x: not (ancestors(x) ^ self.subQueries())):
        moveInAll(inner=newtable, outer=self, debug=debug)
    
    
    # STEP 3: MOVING OUT - everything that's grouped by or joined to something that's grouped by
    for newtable in self.subQueries():
        moveOutAll(inner=newtable, outer=self, debug=debug)
    
    
    # # STEP 4: COPY TABLES THAT CAN'T BE REROUTED
    # for table in getRerouteTables(self):
    #     newtable = (table.parents - self)[0]
    #     if canRerouteTable(table, inner=newtable, outer=self): continue
    #     # copyJoinconds(table, newtable, self)
    #     copyTable(table, self, addcond=True)
    #     if debug:
    #         print("COPY TABLE", table)
    #         print(self.sql(reduce=False, debug=True))
    
    
    # STEP 5: ACTUALLY REROUTE
    while getRerouteTables(self):
        for table in getRerouteTables(self):
            if debug:
                print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
                print("REROUTE", table)
            newtables = table.parents - self
            for newtable in newtables.filter(lambda x: not (ancestors(x) ^ newtables)):
                rerouteAll(table, inner=newtable, outer=self)
            if debug:
                print(self.sql(reduce=False, debug=True))
                print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    
    # warning: move in a having before you connect it will cause it to disappear sometimnes
    
    
    
    # STEP 7: CONNECT THE DISJOINTED TABLES
    addParents(self, reset=True, debug=True)
    # connect subqueries with at least one groupby in common
    tabdescendants = self.subQueries().partition(lambda x, y: x.groupbys ^ y.groupbys).filter(lambda x: len(x) > 1)
    for queries in tabdescendants:
        connectTables(queries, self, debug=debug)
        if debug:
            print("CONNECTED():", queries.fmap(lambda x: x.groupbys).fold(lambda x, y: x ^ y))
            print(self.sql(reduce=False, debug=True))
            print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    # STEP 6: MOVE IN THE "HAVING" EXPRS
    for newtable in self.subQueries():
        moveInHaving(inner=newtable, outer=self)
    
    
    # STEP 8: REDUCE CHILDREN
    for subtable in self.subQueries():
        if debug:
            print(' # %% ^━━━━━━━━━━━━━━━ REDUCING CHILD ━━━━━━━━━━━━━━━━━━━━^ ')
        reduceQuery(subtable, debug=debug, subquery=True)
    
    # STEP 6: MOVE IN THE "HAVING" EXPRS
    for newtable in self.subQueries():
        moveInHaving(inner=newtable, outer=self)
    
    
    # STEP 9: CLEAN UP REDUNDANT OUTERMOST QUERY
    if canCleanUp(self):
        cleanUp(self)
        if debug:
            print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
            print("CLEANED UP...")
            print(self.sql(reduce=False, debug=True))
            print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    
    # # STEP 10: REPEAT IF THERE'S STILL MORE TO DO
    # if getRerouteTables(self):
    #     if debug:
    #         print("REDUCING AGAIN")
    #         reduceQuery(self)
    
    
    if debug:
        print("FINISHED REDUCING")
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')

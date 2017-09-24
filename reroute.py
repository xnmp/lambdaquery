from expr import *
from misc import *
from itertools import product


def addParents(self, reset=False, debug=False):
        
    # if reset:
    #     self.parents = L()
    #     for table in self.getTables():
    #         table.parents = L()
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








# %%  _______   ________  _______    ______   __    __  ________  ______  __    __   ______
# %% /       \ /        |/       \  /      \ /  |  /  |/        |/      |/  \  /  | /      \
# %% $$$$$$$  |$$$$$$$$/ $$$$$$$  |/$$$$$$  |$$ |  $$ |$$$$$$$$/ $$$$$$/ $$  \ $$ |/$$$$$$  |
# %% $$ |__$$ |$$ |__    $$ |__$$ |$$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$$  \$$ |$$ | _$$/
# %% $$    $$< $$    |   $$    $$< $$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$$$  $$ |$$ |/    |
# %% $$$$$$$  |$$$$$/    $$$$$$$  |$$ |  $$ |$$ |  $$ |   $$ |     $$ |  $$ $$ $$ |$$ |$$$$ |
# %% $$ |  $$ |$$ |_____ $$ |  $$ |$$ \__$$ |$$ \__$$ |   $$ |    _$$ |_ $$ |$$$$ |$$ \__$$ |
# %% $$ |  $$ |$$       |$$ |  $$ |$$    $$/ $$    $$/    $$ |   / $$   |$$ | $$$ |$$    $$/
# %% $$/   $$/ $$$$$$$$/ $$/   $$/  $$$$$$/   $$$$$$/     $$/    $$$$$$/ $$/   $$/  $$$$$$/


# %% ^━━━━━━━━━━━━━━━ CONDITIONS ━━━━━━━━━━━━━━━━━━^

def hasEqs(table, inner, outer):
    # if the inner table is entirely replaceable
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


def canMoveOut(table, inner, outer):
    
    # isisolated = inner.joincond._filter(table, getRerouteTables(outer)).getTables() <= L(table)
        
    # we ALWAYS want it IN unless the table appears in more than one subquery, in this case it's like we're "factorizing" it
    # shouldmove = table.parents.filter(lambda x: outer in x.parents).len() > 1
    
    # NEW: just move if it's either an outer groupby or joined to an outer groupby, and the same is not true for inner
    willmove = table in outer.groupbys.getTables() + outer.getTables().filter(lambda x: x.isJoined(outer.joincond.children))
    isgroupedinner = table in inner.groupbys.getTables() + inner.getTables().filter(lambda x: x.isJoined(inner.joincond.children))
    
    # can't move out anything that appears in an agg expr
    canmove = not inner.allExprs().filter(lambda x: x.isagg()).getTables()\
                <= inner.allExprs().filter(lambda x: table in x.getTables()).getTables()
    
    return willmove and not inner.leftjoin and (not isgroupedinner) and canmove # and isisolated 


def canMoveIn(expr, inner, outer):
    
    # if it's only part of a single subquery
    willmove = outer.getTables().filter(lambda x: x.isQuery() and expr.getTables() ^ x.getTables()).len() <= 1
    
    # if the tables of expr are on the inside, OR if it's joined by primary key to something whose tables are on the inside
    # ie its tables are a subset of inner's tables, plus everything that's joined by primary key to one of inner's tables
    canmove = expr.getTables() <= outer.joincond.children \
                .filter(lambda x: x.isJoin()
                        and ((x.children[0].isPrimary() and 
                              x.children[1].table in inner.getTables())
                             or
                             (x.children[1].isPrimary() and 
                              x.children[0].table in inner.getTables())
                             )
                       ).getTables() + inner.getTables()
    
    return canmove and willmove and not expr.isagg()


def canReroute(expr, basetable, target):
    # the target is the inner query
    # if the tables of expr are a subset of the tables of the target... and not aggregate and not constant
    return basetable in expr.getTables() and expr.getTables() <= target.getTables() \
           and type(expr) is not ConstExpr and (not expr.isagg())

def canRerouteTable(basetable, inner, outer):
    
    allexprs = outer.allExprs().bind(lambda x: L(x) + x.descendants())\
        .filter(lambda x: type(x) is BaseExpr and basetable in x.getTables())
    
    isgrouped = allexprs.fmap(lambda x: isGrouped(x, inner)).all()
    
    # if basetable in outer.groupbys.filter(lambda x: x.isPrimary()).getTables():
    #     inner.groupbys += outer.groupbys.filter(lambda x: x.isPrimary() and basetable in x.getTables())#.fmap(lambda x: x.getRef())
    #     return True
    
    return isgrouped or basetable in outer.groupbys.filter(lambda x: x.isPrimary()).getTables()
    
    # # if basetable is in 
    # if basetable in getGroups(inner).filter(lambda x: x.isPrimary()).getTables():
    #     return True
    
    # # if all baseexprs involving the basetable are joined to the target
    # return outer.allExprs().bind(lambda x: x.descendants())\
    #     .filter(lambda x: type(x) is BaseExpr and basetable in x.getTables()) <= getGroups(target)


def isGrouped(expr, target):
    '''
    an expr is grouped if any of the following are true:
    1. expr is one of the target's groupbys
    2. the tables of expr are part of the target's primary grouped by tables
    2. expr is in the getEqs of the target's groupbys
    3. expr is joined by primary key to something thats's grouped
    4. expr is in the getEqs of something that's grouped
    '''
    
    # if expr is grouped by, or is joined to anything of the primary keys, 
    # everything equal to a groupby expr
    alleqs = target.groupbys.bind(lambda x: getAllEqsRecursive(x, target.joincond.children))
    
    allgroups = getGroups(target)
    
    return expr in allgroups or expr.getTables() <= allgroups.filter(lambda x: x.isPrimary()).getTables()
    # return expr in alleqs or expr.getTables() <= target.groupbys.getTables() + alleqs.filter(lambda x: x.isPrimary()).getTables()

def getAllEqsRecursive(expr, jclist):
    jclistnew = L(expr)
    jclistold = None
    while jclistnew != jclistold:
        jclistold = copy(jclistnew)
        jclistnew += jclistnew.bind(lambda x: getAllEqs(x, jclist))
    return jclistnew


def getAllEqs(expr, jclist):
    """
    get everything in jclist that's equal to expr
    """
    res = L()
    for eqexpr in jclist.filter(lambda x: isinstance(x, EqExpr)):
        if expr == eqexpr.children[0] or eqexpr.children[0] in res:
            res += eqexpr.children[1]
        elif expr == eqexpr.children[1] or eqexpr.children[1] in res:
            res += eqexpr.children[0]
    return res

def getGroups(target):
    
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


def canCleanUp(self):
    inner = self.getTables()[0]
    return len(self.getTables()) == 1 and type(inner) is not Table \
      and self.columns.values().fmap(lambda x: isinstance(x, BaseExpr)).all() \
      and not self.joincond.children # and self.groupbys.fmap(lambda x: x.getRef()) == inner.groupbys


# %% ^━━━━━━━━━━━━━━━━━ THE HARD PART: REROUTING ━━━━━━━━━━━━━━━^

@baseFunc
def replaceTable(expr, basetable, parenttable, outertable=None):
    """
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


def reroute(expr, basetable, target):
    """
    target is the "intermediate" table
    for an expr, if it can be rerouted, then make it as a select of that target, 
    and return another expr that references it
    """
    if canReroute(expr, basetable, target):
        dummylabel = target.columns.getKey(expr)
        if 'reroute_' not in dummylabel and dummylabel not in target.columns:
            dummylabel = 'reroute_' + dummylabel
            target.columns[dummylabel] = expr
            if not isGrouped(expr, target) and type(expr) is BaseExpr:
                # add it as a groupby if it's not already
                target.groupbys += expr.table.primaryExpr()
            
        print("REROUTED():", expr)
        
        return BaseExpr(dummylabel, target)
    elif not expr.getTables() ^ target.getTables():
        return expr
    return expr.fmap(lambda x: reroute(x, basetable, target))


def rerouteAll(table, inner, outer):
    outer.joincond.modify(lambda x: reroute(x, basetable=table, target=inner))
    outer.columns.modify(lambda x: reroute(x, basetable=table, target=inner))
    outer.groupbys.modify(lambda x: reroute(x, basetable=table, target=inner))


def moveIn(expr, inner, outer):
    inner.joincond.children += expr#.getRef(oldtables=L(inner))
    outer.joincond.children -= expr


def moveInAll(inner, outer):
    """
    the loop is to account for the case when a table isn't joined by primary key, 
    but it's joined to somethaing that's joined by primary key 
    """
    finish = False
    while not finish:
        finish = True
        for expr in outer.joincond.children:
            if canMoveIn(expr, inner, outer):
                finish = False
                moveIn(expr, inner, outer)
                print("MOVED_IN():", expr)
                print(outer.sql(reduce=False))
    

def moveInHaving(inner, outer):    
    # move in the HAVING conditions, this facilitates a cleanup later on
    # move it in if the inner query is the only one of its tables.
    
    for outexpr in outer.joincond.children.filter(lambda x: x.getTables().len() == 1 
                                               and x.getTables()[0] == inner and not x.isagg()):
        havingexpr = outexpr.getRef(oldtables=L(inner))
        # TODO: the descendants of havingexpr are already grouped by
        # havingexpr.descendants.getTables() <= inner.groupbys
        
        print("MOVED_IN():", havingexpr)
        print(outer.sql(reduce=False))
        
        inner.joincond.children += havingexpr
        outer.joincond.children -= outexpr
        
        
        # get rid of references that were only there to provide for the aggregate joincond (broken)
        for key, inexpr in inner.columns.items():
            # if the expr doesn't get refered to by the outer table
            if not outer.allExprs().bind(lambda x: L(x) + x.descendants()).filter(lambda x: x.getRef() == inexpr).exists():
                del inner.columns[key]


def mergeGrouped(self):
    # merge tables that have the same groupbys, because ponly then does it make sense to merge them
    
    oldself = copy(self)
    checktables = self.getTables()
    # if not self.isagg(): checktables += self
    
    if not self.isagg():
        alltablegroups = L(L(self) + checktables.filter(lambda x: x.groupbys.exists() 
                                                      and x.groupbys #.fmap(lambda x: x.getRef()) 
                                                        <= self.groupbys)).filter(lambda x: len(x) > 1)
        
    if self.isagg() or not alltablegroups:
        alltablegroups = checktables.filter(lambda x: x.groupbys.exists())\
                           .groupby(lambda x: (x.groupbys, x.leftjoin))\
                           .filter(lambda x: len(x.value) > 1)\
                           .fmap(lambda x: x.value)
    
    for tablegroup in alltablegroups:
        # merged = tablegroup.combine()
        
        merged = tablegroup.fold(lambda x, y: x | y)
        self.setSource(merged, oldtable=tablegroup)
        
        # print(self.sql(reduce=False))
        
        if self in tablegroup:
            # merge with a subquery and convert the agg exprs
            merged.modify(lambda x: x.getRef(oldtables=tablegroup))
            self.modify(lambda x: x.getRef(oldtables=L(merged)))
            merged.columns = self.columns
            self.__dict__.update(merged.__dict__)
            break
    
    if self.sql(reduce=False) != oldself.sql(reduce=False):
        mergeGrouped(self)


def copyTable(table, parent, addcond=True):
    # when a table can't be rerouted
    # also needed to clone join conditions into subqueries
    tablecopy = copy(table)
    tablecopy.alias += '_copy'
    parent.setSource(tablecopy, oldtable=table)
    eqexprs = tablecopy.primarynames().fmap(lambda x: EqExpr(BaseExpr(x, table), BaseExpr(x, tablecopy)))
    if addcond: parent.joincond &= AndExpr(eqexprs)


def cleanUp(self):
    # get rid of redundant the redundant outer query
    newquery = self.getTables()[0]
    for expr in self.joincond.children:
        newquery.joincond += expr.getRef()
    for key, expr in self.columns.items():
        newquery.columns[key] = newquery.columns[expr.fieldname]
        del newquery.columns[expr.fieldname]
    self.__dict__.update(newquery.__dict__)


def copyJoinconds(basetable, source, target):
    res = AndExpr(source.joincond.children.filter(lambda x: x.isJoin() & (basetable in x.getTables())))
    target.joincond &= res


def connectTables(table):
    for i, parent in table.parents.enumerate():
        if i == 0: continue
        tablecopy = copy(table)
        parent.setSource(tablecopy, table)
        for gparent in parent.parents:
            # add a joincond
            gparent.joincond.children += EqExpr(table.primaryExpr(), tablecopy.primaryExpr())
            # now reroute it
            gparent.joincond.modify(lambda x: reroute(x, basetable=table, target=table.parents[i-1]))
            gparent.joincond.modify(lambda x: reroute(x, basetable=tablecopy, target=parent))
        table = tablecopy



# %% ^━━━━━━━━━━━━━━━━━━━━━━━ REDUCE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

def reduceQuery(self):
    """
    make everything appear in scope - the dependency graph has to be a tree
    this means we have to redirect everything via its aggregate queries
    """
    
    # finish = False
    # while not finish:
    #     finish = True
    
    print(' # %% ^━━━━━━━━━━━━━━━━━ REDUCING... ━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    print(self.sql(reduce=False))    
    
    # STEP 1: MERGE SUBQUERIES WITH THE SAME GROUP BYS
    selfcopy = copy(self)
    mergeGrouped(self)
    if str(selfcopy.__dict__) != str(self.__dict__):
        print(' # %% ^━━━━━━━━━━━━━━━━━ AFTER MERGING ━━━━━━━━━━━━━━━━━━━^ ')
        print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    else:
        print("NO TABLES MERGED")
    if not self.subQueries():
        print("NOTHING TO REDUCE... DONE")
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        return
    
    # STEP 2: MOVING IN
    for newtable in self.subQueries():
        moveInAll(inner=newtable, outer=self)
    
    # for table in self.getTables():
    #     newtable = self.subQueries()[0]
    #     # newtable = (table.parents - self)[0]
    #     # if canMoveIn(table, inner=newtable, outer=self):
    
    
    # STEP 3: MOVING OUT - everything that's grouped by or joined to something that's grouped by
    
    moveout = True
    while moveout:
        
        moveout = False
        for table in getRerouteTables(self):
            # getRerouteTables(self)
            
            for newtable in table.parents - self:
                if canMoveOut(table, inner=newtable, outer=self):
                    # finish = False
                    moveOut(table, inner=newtable, outer=self)
                    
                    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
                    print("MOVEOUT", table)
                    print(self.sql(reduce=False))
                    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')    
                    moveout = True
                    import pdb; pdb.set_trace()  # breakpoint 0b3a9ad1 //
                    
                    break
            if moveout:
                break
    
    
    # STEP 4: COPY TABLES THAT CAN'T BE REROUTED
    for table in getRerouteTables(self):
        newtable = (table.parents - self)[0]
        if canRerouteTable(table, inner=newtable, outer=self): continue
        
        print("COPY TABLE", table)
        
        copyJoinconds(table, newtable, self)
        copyTable(table, self, addcond=False)
        print(self.sql(reduce=False))
    
    # STEP 5: ACTUALLY REROUTE
    for table in getRerouteTables(self):
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print("REROUTE", table)
        for newtable in table.parents - self:
            rerouteAll(table, inner=newtable, outer=self)
        print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    # STEP 6: MOVE IN THE "HAVING" EXPRS
    for newtable in self.subQueries():
        moveInHaving(inner=newtable, outer=self)
    
    # STEP 7: CONNECT THE DISJOINTED TABLES
    addParents(self, reset=True, debug=True)
    tabdescendants = self.getTables().getTables()
    for table in tabdescendants:
        connectTables(table)
    
    # STEP 8: REDUCE CHILDREN
    for subtable in self.subQueries():
        reduceQuery(subtable)
    
    # STEP 9: CLEAN UP REDUNDANT OUTERMOST QUERY
    if canCleanUp(self):
        cleanUp(self)
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print("CLEANED UP...")
        print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    # # STEP 10: REPEAT IF THERE'S STILL MORE TO DO
    # if getRerouteTables(self):
    #     print("REDUCING AGAIN")
    #     reduceQuery(self)
    
    print("FINISHED REDUCING")
    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')

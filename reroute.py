from expr import *
from misc import *
from itertools import product


def addParents(self, reset=False):
    if reset:
        for table in self.getTables():
            table.parents = L()
    for table in self.getTables():
        table.parents += L(self)
        addParents(table)


def ancestors(self):
    return copy(self.parents) + self.parents.bind(ancestors)


def toReroute(self):
    # if one of its parents is a child of another parent
    # if len(self.parents) <= 1:
        # return False
    for parent1, parent2 in product(self.parents, self.parents):
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

def canMoveOut(table, inner, outer):
    
    # DON'T NEED THIS BIT ANYMORE
    # we can't move any tables that would be moved back in
    # oldtodo: however there may be a subset of getRerouteTables(outer) that could all be moved out at the same time
    # isisolated = inner.joincond._filter(table, getRerouteTables(outer)).getTables() <= L(table)
    
    # we move tables only if they inner one is not left joined
    
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
    
    # we ALWAYS want it IN unless the table appears in more than one subquery, in this case it's like we're "factorizing" it
    willmove = table.parents.filter(lambda x: outer in x.parents).len() > 1
    
    # always move it if the outer is one of the groupbys
    canmove = table in outer.groupbys.getTables() 
    # shouldmove = table not in inner.groupbys.getTables()
    
    return haseqs and canmove and willmove and not inner.leftjoin # and isisolated


# def canMoveIn(table, inner, outer, exclude=L()):
#     # could be that a whole coalition of tables can all move in
#     # (OBSOLETE) can't move if it would create a cycle
#     # return outer.allExprs().filter(lambda x: table in x.getTables() 
#     #                                and inner in x.getTables()
#     #                                # add more conditions here
#     #                               ).notExists()
    
#     # MOST IMPORTANT is that inner is grouped by the primary key of table
#     # return table.getPrimary() in inner.groupbys()
#     # return outer.allExprs().filter(lambda x: table in x.getTables()).getTables() <= inner.getTables()
    
#     others = outer.joincond.children.filter(lambda x: table in x.getTables()).getTables()    
#     if others.filter(lambda x: not x.isTable()):
#         return False
    
    
#     # others_canmove = (others - table - exclude).fmap(lambda x: canMoveIn(x, inner, outer, exclude + table)).all()
#     # isin = table in inner.getTables()
#     oneparent = table.parents.filter(lambda x: outer in x.parents).len() <= 1
#     return oneparent and not inner.leftjoin #and others_canmove


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


def canCleanUp(self):
    return len(self.getTables()) == 1 and type(self.getTables()[0]) is not Table \
      and self.columns.values().fmap(lambda x: isinstance(x, BaseExpr)).all()


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
    # mutates HARD
    inner.groupbys.modify(lambda x: replaceTable(x, table, inner))
    inner.columns.modify(lambda x: replaceTable(x, table, inner))
    inner.joincond.modify(lambda x: replaceTable(x, table, inner, outer))
    # get rid of tautological exprs of the form x = x
    inner.joincond.children = inner.joincond.children.filter(lambda x: not x.isRedundant())


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
        
        print("REROUTED():", expr)
        print(target.sql(reduce=False))
        
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
        
        # moveIn(expr, inner, outer)
        
        # get rid of references that were only there to provide for the aggregate joincond
        for key, inexpr in inner.columns.items():
            # if the expr doesn't get refered to by the outer table
            if not outer.allExprs().bind(Expr.descendants).filter(lambda x: x.getRef() == inexpr).exists():
                del inner.columns[key]


def mergeGrouped(self):
    # merge tables that have the same groupbys, because ponly then does it make sense to merge them
    
    checktables = self.getTables()
    if not self.isagg(): checktables += self
    
    alltablegroups = checktables.filter(lambda x: x.groupbys.exists())\
                           .groupby(lambda x: (x.groupbys, x.leftjoin))\
                           .filter(lambda x: len(x.value) > 1)\
                           .fmap(lambda x: x.value)
    
    for tablegroup in alltablegroups:
        sumtables = tablegroup.sum()
        self.setSource(sumtables, oldtable=tablegroup)
        
        # print(self.sql(reduce=False))
        
        if self in tablegroup:
            # merge with a subquery and convert the agg exprs
            sumtables.modify(lambda x: x.getRef(oldtables=tablegroup))
            self.modify(lambda x: x.getRef(oldtables=L(sumtables)))
            sumtables.columns = self.columns
            self.__dict__.update(sumtables.__dict__)
            break


def copyTable(table, parent):
    # this is probably obsolete, but may be needed when we need to clone join conditions into subqueries
    tablecopy = copy(table)
    tablecopy.alias += '_copy'
    parent.setSource(tablecopy, oldtable=table)
    eqexprs = tablecopy.tableclass().primarynames().fmap(lambda x: EqExpr(BaseExpr(x, table), BaseExpr(x, tablecopy)))
    parent.joincond &= AndExpr(eqexprs)


def cleanUp(self):
    # get rid of redundant the redundant outer query
    newquery = self.getTables()[0]
    for expr in self.joincond.children:
        newquery.joincond += expr.getRef()
    for key, expr in self.columns.items():
        newquery.columns[key] = newquery.columns[expr.fieldname]
        del newquery.columns[expr.fieldname]
    self.__dict__.update(newquery.__dict__)


# %% ^━━━━━━━━━━━━━━━━━━━━━━━ REDUCE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

def reduceQuery(self):
    """
    make everything appear in scope - the dependency graph has to be a tree
    this means we have to redirect everything via its aggregate queries
    """
    
    # finish = False
    # while not finish:
    #     finish = True
    
    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    print("REDUCING...")
    print(self.sql(reduce=False))    
    
    
    # STEP 1: REDUCE CHILDREN
    for subtable in self.subQueries():
        reduceQuery(subtable)
    
    
    # STEP 2: MERGE SUBQUERIES WITH THE SAME GROUP BYS
    selfcopy = copy(self)
    mergeGrouped(self)
    
    if str(selfcopy.__dict__) != str(self.__dict__):
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print("AFTER MERGING...")
        print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    else:
        print("NO TABLES MERGED")
    
    if not self.subQueries():
        print("NOTHING TO REDUCE... DONE")
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        return
    
    
    # STEP 3: MOVING IN
    # while :
    #     newtable = self.subQueries()[0]
        
    #     # first move ones that are on the inside already
    #     for table in self.getTables().filter(lambda x: x in inner):
    #         moveIn(table, inner=newtable, outer=self)
            
    #     # now move the ones that are joined by primary key to ones on the inside
    #     for table in self.joincond.filter(lambda x: x.isJoin() and x.getTables() | newtable.getTables())\
    #                               .filter(lambda x: x.)
    #     .getTables().filter(lambda x: x in newtable):
    #         moveIn(table, )
    
    for newtable in self.subQueries():
        moveInAll(inner=newtable, outer=self)
    
    # for table in self.getTables():
    #     newtable = self.subQueries()[0]
    #     # newtable = (table.parents - self)[0]
    #     # if canMoveIn(table, inner=newtable, outer=self):
    
    
    # STEP 4: MOVING OUT
    # moveout = False
    for table in getRerouteTables(self):#self.subQueries().getTables():#
        
        for newtable in table.parents - self:
            if canMoveOut(table, inner=newtable, outer=self):
                # finish = False
                moveOut(table, inner=newtable, outer=self)
                
                print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
                print("MOVEOUT", table)
                print(self.sql(reduce=False))
                print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')    
    
    # STEP 5: REROUTE
    # this is the part that is very reliable
    for table in getRerouteTables(self):
        # finish = False
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print("REROUTE", table)
        rerouteAll(table, inner=newtable, outer=self)
        # print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    # STEP 6: MOVE IN THE "HAVING" EXPRS
    for newtable in self.subQueries():
        moveInHaving(inner=newtable, outer=self)
    
    # STEP 7: CLEAN UP REDUNDANT OUTERMOST QUERY
    if canCleanUp(self):
        cleanUp(self)
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
        print("CLEANED UP...")
        print(self.sql(reduce=False))
        print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')
    
    print("FINISHED REDUCING")
    print(' # %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^ ')



# def relabel(self, tag=0):
#     for tablegroup in self.getTables().groupby(lambda x: x.tablename):
        
#         for i, tab in tablegroup.value.enumerate():
#             tab.alias += str(i)#str(tag) + str(i)
#             # relabel(tab, tag = tag + 1)

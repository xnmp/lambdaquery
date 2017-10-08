from LambdaQuery.reroute import *


def sub_sql(self):
    return sql(self, reduce=False, subquery=True)


def tableGen(self, reduce=True, debug=False, correlated=False, subquery=False):
    
    alltables = self.getTables()
    
    if correlated:
        correlated_tables = self.groupbys.getTables()
        alltables -= correlated_tables
    else:
        correlated_tables = L()
    
    fulltables = alltables.filter(lambda x: not x.leftjoin or x in self.groupbys.getTables())
    ljtables = alltables.filter(lambda x: x.leftjoin and x not in self.groupbys.getTables())
    
    res, remainingcond, addedtables, havings = '', copy(self.joincond), L(), L()
    wheres = self.joincond.children.filter(lambda x: not x.getTables())
    cjed = {}
    
    while len(addedtables) < len(alltables):
        
        for table in fulltables + ljtables - addedtables:
            
            if cjed and table.leftjoin and not L(*cjed.keys()).filter(lambda x: x.leftjoin).any(): break
            
            # get the properties that only depend on one table to put in the where clause
            if table.leftjoin:
                wheres += remainingcond.children.filter(lambda x: table in x.getTables() 
                                                        and x.iswhere 
                                                        and not x.isJoin2() 
                                                        and not x.isagg())
            else:
                wheres += remainingcond._filter(table, correlated_tables)\
                                       .children.filter(lambda x: not x.isagg())
            havings += remainingcond.children.filter(lambda x: x.isagg())
            joins = remainingcond._filter(table, addedtables).children - wheres - havings
            
            if not addedtables:
                # first table
                res += f'\nFROM {sub_sql(table)}'
                
            elif joins.filter(lambda x: x.isJoin2()):
                if table in cjed: del cjed[table]
                # this one "works" and is proper
                jointype = 'LEFT ' if table.leftjoin and fulltables else ''                
                joinstr = str(AndExpr(joins)).replace('AND', '\n    AND')
                res += f"\n  {jointype}JOIN {sub_sql(table)} ON {joinstr}"
                
            else:
                # cross joining
                if (table in cjed and cjed[table]) or len(alltables - addedtables) == 1:
                    if table in cjed: del cjed[table]
                    jointype = 'LEFT ' if table.leftjoin and fulltables else ''
                    if joins:
                        joinstr = str(AndExpr(joins)).replace('AND', '\n    AND')
                        res += f"\n  {jointype}JOIN {sub_sql(table)} ON {joinstr}"
                    else:
                        res += f"\n  {jointype}CROSS JOIN {sub_sql(table)}"
                else:
                    cjed[table] = True
                    continue
            
            remainingcond.children -= joins + wheres + havings
            addedtables.append(table)
    
    return res, wheres, havings


def lastSeparator(self):
    return '\n' if len(self.split('\n  ')[-1]) > 200 else ''


def getGroupbys(self, havings=None, reduce=False, subquery=False, debug=False):
    exprs = self.columns.values()
    if (reduce or subquery) and self.isagg() and not exprs.fmap(lambda x: x.isagg()).all():
        groupbys = L(*range(1, exprs.filter(lambda x: not x.isagg()).len() + 1))
        groupbys += havings.bind(Expr.havingGroups).filter(lambda x: x not in exprs)
        if subquery and not debug:
            # don't include the groupbys if the outermost query is an aggregate, 
            # because we're cheating with functions like count_
            groupbys += self.groupbys.filter(lambda x: x not in exprs)
    elif self.isagg() or debug:
        groupbys = self.groupbys
    else:
        return L()
    return groupbys


def sql(self, display=True, reduce=True, subquery=False, debug=False, correlated=False):
    
    if type(self) is Table: return str(self)
    if reduce: reduceQuery(self, debug=debug)
    
    if self in self.getTables():
        print("INFINITE LOOP")
        return
    
    # ==SELECT...
    selects = self.columns.items().sort(lambda x: x[1].isagg())
    showSql = lambda p: str(p[1]) + (f' AS {p[0]}' if p[0] != '' else f'')
    select_type = '' if self.isagg() else 'DISTINCT '
    res = f'SELECT {select_type}\n  ' + selects.fmap(showSql).intersperse(', \n  ')
    
    # ==FROM...
    joinstr, wheres, havings = tableGen(self, reduce=reduce, debug=debug, correlated=correlated, subquery=subquery)
    res += joinstr
    
    # ==WHERE...
    if wheres: res += f'\nWHERE ' + wheres.intersperse2('\n    AND ')
    
    # ==GROUP BY...
    groupbys = getGroupbys(self, havings, reduce=reduce, subquery=subquery, debug=debug)
    if groupbys: res += f'\nGROUP BY ' + groupbys.intersperse(', ')
    
    # ==HAVING...
    if havings: res += f'\nHAVING ' + havings.intersperse('\n    AND ')
    
    # ==ORDER BY / LIMIT...
    if self.ordervar: res += f'\nORDER BY ' + self.ordervar.intersperse(', ')
    if self.limitvar: res += f'\nLIMIT {self.limitvar}'
    
    # ==ADJUSTMENTS
    if subquery:        
        indent = "\n             " if self.leftjoin else "\n        "
        res = f'(--━━━━━━━━━━ SUBQUERY ━━━━━━━━━━--\n{res}\n--━━━━━━━━━━━━━━━━━━━━━━━━━━━━━--\n) AS {self.abbrev}'.replace("\n", indent)        
    elif display:
        res = f'SQL() = \'\'\'\n{res}\n\'\'\''
    
    return res


# for pdb debugging
import LambdaQuery.query
@property
def p_sql(self):
    print(sql(self, reduce=False))
LambdaQuery.query.Query.p = p_sql
LambdaQuery.query.Query.sql = sql

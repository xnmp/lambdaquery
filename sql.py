from reroute import *


def tableGen(self):
    pass


def lastSeparator(self):
    return '\n' if len(self.split('\n  ')[-1]) > 200 else ''


def sql(self, display=True, reduce=True, subquery=False):
    
    # self = deepcopy(self)
    if type(self) is Table: return str(self)
    if reduce: reduceQuery(self)
    
    alltables = self.getTables()
    fulltables = alltables.filter(lambda x: not x.leftjoin)
    ljtables = alltables.filter(lambda x: x.leftjoin)
    
    remainingcond, addedtables, wheres, havings = copy(self.joincond), L(), L(), L()
    selects = self.columns.items().sort(lambda x: x[1].isagg())
    exprs = selects.fmap(lambda x: x[1])
    
    # ==SELECT...
    showSql = lambda p: str(p[1]) + (f' AS {p[0]}' if p[0] is not None else '')
    selecttype1 = '' if self.isagg() else 'DISTINCT '
    res = f'SELECT {selecttype1}\n  ' + selects.fmap(showSql).intersperse(', \n  ')
    
    # ==FROM...
    while len(addedtables) < len(alltables):
        for table in fulltables + ljtables - addedtables:
            
            # get the properties that only depend on one table to put in the where clause
            # if not table.leftjoin or not fulltables:
            #     wheres += remainingcond._filter(table).children
            # else:
            wheres += remainingcond._filter(table).children.filter(lambda x: x.iswhere and not x.isagg())
            havings += remainingcond.children.filter(lambda x: x.isagg())
            joins = remainingcond._filter(table, addedtables).children - wheres - havings
            
            if not addedtables:
                res += f'\n{lastSeparator(res)}FROM {sql(table, reduce=reduce, subquery=True)}'
            elif joins:
                jointype = 'LEFT JOIN ' if table.leftjoin and fulltables else 'JOIN '
                joinstr = str(AndExpr(joins)).replace('AND', '\n    AND')
                res += f"\n  {jointype}{sql(table, reduce=reduce, subquery=True)} ON {joinstr}"
            else:
                # if reduce: continue
                res += f'\n  CROSS JOIN {sql(table, reduce=reduce, subquery=True)}'
            
            remainingcond.children -= joins + wheres + havings
            addedtables.append(table)
            
    # ==WHERE...
    if wheres: res += f'\n{lastSeparator(res)}WHERE ' + wheres.intersperse('\n    AND ')
    
    # ==GROUP BY...
    # if self.isagg():
        # groupbys = L(*range(1, exprs.filter(lambda x: not x.isagg()).len() + 1))
    #     if subquery or not groupbys:
    #         groupbys += self.groupbys.filter(lambda x: x not in exprs)
    #     res += '\n\nGROUP BY ' + groupbys.intersperse(', ')
    groupbys = self.groupbys + havings.bind(Expr.baseExprs)
    res += f'\n{lastSeparator(res)}GROUP BY ' + groupbys.intersperse(', ')
    
    # ==HAVING...
    if havings: 
        import pdb; pdb.set_trace()  # breakpoint bf262ac7 //
        
        res += f'\nHAVING ' + havings.intersperse('\n    AND')
    
    # ==ORDER BY / LIMIT...
    if self.ordervar: res += f'\nORDER BY ' + self.ordervar.intersperse(', ')
    if self.limitvar: res += f'\nLIMIT {self.limitvar}'
    
    # ADJUSTMENTS
    if subquery:
        indent = "\n             " if self.leftjoin else "\n        "
        res = f'(--━━━━━━━━━━ SUBQUERY ━━━━━━━━━━--\n{res}\n--━━━━━━━━━━━━━━━━━━━━━━━━━━━━━--\n) AS {self.abbrev}'.replace("\n", indent)
    elif display:
        res = f'SQL() = \'\'\'\n{res}\n\'\'\''
    
    return res

import query
query.Query.sql = sql

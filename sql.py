from reroute import *


def tableGen(self, reduce=True):
    
    alltables = self.getTables()
    fulltables = alltables.filter(lambda x: not x.leftjoin)
    ljtables = alltables.filter(lambda x: x.leftjoin)
    
    res, remainingcond, addedtables, wheres, havings = '', copy(self.joincond), L(), L(), L()
    cjed = {}
    
    while len(addedtables) < len(alltables):
        
        for table in fulltables + ljtables - addedtables:
            if cjed and table.leftjoin: break
            
            # get the properties that only depend on one table to put in the where clause
            if table.leftjoin:
                wheres += remainingcond.children.filter(lambda x: x.iswhere and not x.isJoin() and not x.isagg())
            else:
                wheres += remainingcond._filter(table).children.filter(lambda x: not x.isagg())
            # wheres += remainingcond._filter(table).children.filter(lambda x: (x.iswhere or not table.leftjoin) and not x.isagg())
            havings += remainingcond.children.filter(lambda x: x.isagg())
            joins = remainingcond._filter(table, addedtables).children - wheres - havings
            
            if not addedtables:
                res += f'\n{lastSeparator(res)}FROM {sql(table, reduce=reduce, subquery=True)}'
            elif joins.filter(lambda x: x.isJoin()):
                jointype = 'LEFT JOIN ' if table.leftjoin and fulltables else 'JOIN '
                joinstr = str(AndExpr(joins)).replace('AND', '\n    AND')
                res += f"\n  {jointype}{sql(table, reduce=reduce, subquery=True)} ON {joinstr}"
            else:
                if (table in cjed and cjed[table]) or not reduce:
                    res += f'\n  CROSS JOIN {sql(table, reduce=reduce, subquery=True)}'
                else:                    
                    cjed[table] = True
                    continue
            
            cjed = {}
            remainingcond.children -= joins + wheres + havings
            addedtables.append(table)
    
    return res, wheres, havings


def lastSeparator(self):
    return '\n' if len(self.split('\n  ')[-1]) > 200 else ''


def sql(self, display=True, reduce=True, subquery=False):
    
    # self = deepcopy(self)
    if type(self) is Table: return str(self)
    if reduce: reduceQuery(self)
        
    # if not subquery and reduce: relabel(self)
    
    selects = self.columns.items().sort(lambda x: x[1].isagg())
    exprs = selects.fmap(lambda x: x[1])
    
    # ==SELECT...
    showSql = lambda p: str(p[1]) + (f' AS {p[0]}' if p[0] is not None else '')
    selecttype1 = '' if self.isagg() else 'DISTINCT '
    res = f'SELECT {selecttype1}\n  ' + selects.fmap(showSql).intersperse(', \n  ')
    
    # ==FROM...
    joinstr, wheres, havings = tableGen(self, reduce=reduce)
    res += joinstr
    
    # ==WHERE...
    if wheres: res += f'\n{lastSeparator(res)}WHERE ' + wheres.intersperse('\n    AND ')
    
    # ==GROUP BY...
    if self.isagg():
        groupbys = L(*range(1, exprs.filter(lambda x: not x.isagg()).len() + 1))        
        if subquery or not groupbys:
            # don't incllude the groupbys if the outermost query is an aggregate, 
            # because we're cheating with functions like count_
            groupbys += (self.groupbys + havings.bind(Expr.baseExprs)).filter(lambda x: x not in exprs)
        groupbys += havings.bind(Expr.havingGroups)
        if groupbys:
            res += '\nGROUP BY ' + groupbys.intersperse(', ')
            
    # GROUP BY for debugging
    # groupbys = self.groupbys + havings.bind(Expr.havingGroups)
    # res += f'\n{lastSeparator(res)}GROUP BY ' + groupbys.intersperse(', ')
    
    # ==HAVING...
    if havings: res += f'\nHAVING ' + havings.intersperse('\n    AND ')
    
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

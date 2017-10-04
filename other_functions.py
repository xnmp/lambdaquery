from LambdaQuery.tables import *
from datetime import timedelta


@Lifted
def abvar(u0, test=None, lj=True, activation=False, after=True):
    timevar = 'active_ts' if activation else 'ts'
    testquery = u0.uid.abtests(lambda x: (x.test == test))
    if after:
        testquery = testquery.filter(lambda x: (getattr(x, timevar) < u0.ts) & x.halt_ts.isnull_())
    if lj:
        testquery = testquery.lj()
    else:
        testquery = testquery.filter(lambda x: x.user.good)
    return testquery.one.variation.coalesce_('No_Variation')

@injective()
def mem(self): #contains a uid and a ts
    res = self.uid.memberships(lambda x: (self.ts > x.ts) 
                             & ((self.ts < coalesce_(x.end_ts, dt.datetime.now())) | (x.status == 'Active'))
                             & x.package.in_('Professional','Premier','Plus','Basic','Intro',
                                             # 'Corporate',
                                             'Standard','Premium')
                             ).lj()
    res.columns['package'] = Expr._coalesce_(res.columns['package'], ConstExpr('None'))
    return res

def ABQuery(testname, cond=None):
    return ABTest.query(lambda x: (x.test == testname) & (x.user.good)).filter(cond)

def onlyTimes(interval=86400, start='2017-01-01', end=dt.datetime.now()):
    return Milestone.query(lambda x: x.between(start, end))\
        .fmap(lambda x: x.ts.round_(interval=interval).label('ts')).distinct()

@injective()
def rts(self, interval=86400/4):
    return self.ts.round_(interval=interval).label('ts')

@Lifted
def plotvar(self, interval=86400/4):
    return self.ts.round_(interval=interval) % count_(self.primary())

@Lifted
def pos_(col):
    return greatest_(col, 0)
    
@Lifted
def last_before(self, action_table, cond=None, on=None):
    if on is not None:
        base = getattr(self, on)
    else:
        base = self
    res = getattr(base, action_table)()\
        .filter(cond)\
        .filter(lambda x: (x.ts < self.ts) 
                & getattr(base, action_table)()\
                  .filter(cond)\
                  .filter(lambda y: y.between(x.ts, self.ts))\
                  # .groupby(x.primary)
                  .notExists()
                ).lj().one
    return res


@Lifted
def isretained(self, interval=28):
    return self.uid.milestones(lambda x: x.between(self.ts + timedelta(days=7), 
                                                   self.ts + timedelta(days=7+interval))
                                & (x.pid != self.pid)).exists()


def first_after(self, action_table, cond=None, on=None):
    if on is not None:
        base = getattr(self, on)
    else:
        base = self
    res = getattr(base, action_table)()\
        .filter(cond)\
        .filter(lambda x: (x.ts > self.ts) 
                & getattr(base, action_table)()\
                  .filter(cond)\
                  .filter(between(self.ts, x.ts))\
                  .notExists()
                ).lj().one
    return res


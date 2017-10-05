
from LambdaQuery.example_tables import *

print(User.query().sql())

print(User.query().projects().sql())


@do(Query)
def ex5():
    p0 = yield Project.query(lambda x: (x.pid > 1000))
    returnM (
             p0.user.pw, p0.user.isenglish, count_(p0.pid)
            )
    
print(ex5().sql())

@do(Query)
def ex6():
    u0 = yield User.query(lambda x: (x.pw > "H"))
    returnM (
             u0.projects().min('ts')
            )

print(ex6().sql())

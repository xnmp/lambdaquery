
from example_tables import *

# %% ^━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━^

# ex1 = School.query()
# ex2 = School.query(lambda x: x.campus == 'south')
# ex3 = School.query().sort(lambda x: x.campus)
# ex4 = School.query().fmap(lambda x: x.name % x.campus)
# ex4 = School.query()['name','campus']

# @do(Query)
# def ex4():
#     sc0 = yield School.query()
#     returnM (
#              sc0.name,
#              sc0.campus
#             )

# @do(Query)
# def ex5():
#     sc0 = yield School.query()
#     returnM (
#              sc0.name,
#              sc0.departments().count()
#             )

# @do(Query)
# def ex6():
#     dept0 = yield Department.query()
#     returnM (
#              dept0.name,
#              dept0.school.name, 
#              dept0.school.campus
#             )

@injective()
def num_dept(self):
    return self.departments().count()

# @do(Query)
# def ex7():
#     sc0 = yield School.query(lambda x: x.num_dept > 3)
#     returnM (
#              sc0.code,
#              sc0.num_dept
#             )

# ex7 = School.query(lambda x: x.num_dept > 3).fmap(lambda x: x.code % x.num_dept)
# ex7 = School.query(lambda x: x.num_dept > 3)[['code','num_dept']]

# @do(Query)
# def ex8():
#     cs0 = yield Course.query()
#     returnM (
#              cs0.title, 
#              cs0.credits.avg_()
#             )

# @do(Query)
# def ex9():
#     sc0 = yield School.query()
#     dept0 = yield sc0.departments()
#     cs0 = yield dept0.courses()
#     returnM (
#              sc0.name,
#              dept0.name,
#              cs0.title
#             )

# %% ^━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━^

# # AGGREGATION

# ex10 = Department.query().count()

# ex11 = School.query().avg('num_dept')

# @do(Query)
# def ex12():
#     dept0 = yield Department.query()
#     cs = dept0.courses()
#     returnM (
#              dept0.name, 
#              cs.count(),
#              cs.max('credits'),
#              cs.sum('credits'),
#              dept0.courses(lambda x: x.title.like_('Introduction%')).count()
#             )

# %% ^━━━━━━━━━━━━━━━━━━  ━━━━━━━━━━━━━━━━━━━━━━━━━━━^


@do(Query)
def ex13():
    sc0 = yield School.query()
    returnM (
             sc0.name,
             sc0.programs()['degree'].count(),
            )


@do(Query)
def ex14():
    dept0 = yield Department.query()
    returnM (
             dept0.name,
             dept0.school.campus
            )


@do(Query)
def ex15():
    dept0 = yield Department.query()
    returnM (
             dept0.name,
             dept0.courses(lambda x: x.credits > 2).lj().count()
            )


@do(Query)
def ex16():
    sc0 = yield School.query()
    returnM (
             sc0.campus,
             sc0.departments().lj().count()
            )

@Lifted
def coursestart(course, start):
    return (course.no > start) & (course.no < start + 99)


@do(Query)
def ex17():
    dept0 = yield Department.query()
    returnM (
             dept0.name,
             *[dept0.courses(lambda x: x.coursestart(i*100)).count() for i in range(1,5)]
            )

@injective()
def high_credit_count(self):
    return self.courses(lambda x: x.credits > 3).count()

@do(Query)
def ex18():
    sc0 = yield School.query(lambda x: x.programs().exists())
    returnM (
             sc0.name,
             sc0.departments()['high_credit_count'].avg()
            )


@do(Query)
def ex19():
    sc0 = yield School.query(lambda x: x.programs(lambda y: y.title == x.name).count() > 5)
    returnM (
             sc0.name,
             sc0.departments()['high_credit_count'].avg()
            )

print(ex19().sql())

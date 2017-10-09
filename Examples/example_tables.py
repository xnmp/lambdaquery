from LambdaQuery import *
from datetime import timedelta


class School(Columns):
    table = Table("school", 'sc')

    def __init__(self):
        School.makeTable()
        self.baseExpr("school_code", 'code', primary=True)
        self.baseExpr("name", 'name')
        self.baseExpr("campus", "campus")
    
class Department(Columns):
    table = Table("department", "dept")
    def __init__(self):
        Department.makeTable()
        self.baseExpr("dept_code", 'code', primary=True)
        self.baseExpr("school_code", 'sc_code', foreign=School)
        self.baseExpr("name", "name")

class Program(Columns):
    table = Table("program", 'prog')
    def __init__(self):
        Program.makeTable()
        self.baseExpr("prog_code", 'code', primary=True)
        self.baseExpr("school_code", 'sc_code', foreign=School)
        self.baseExpr("title", "title")
        self.baseExpr("type", 'type')

class Course(Columns):
    table = Table("course", "course")
    def __init__(self):
        Course.makeTable()
        self.baseExpr("course_code", 'code', primary=True)
        self.baseExpr("dept_code", 'dept_code', foreign=Department)
        self.baseExpr("description", "description")
        self.baseExpr("title", "title")


# ============================================================================
# instantiate all classes so that the primary and foreign keys are all defined
# ============================================================================

for colclass in [subcls for subcls in Columns.__subclasses__()] \
    + [subsubcls for subcls in Columns.__subclasses__() for subsubcls in subcls.__subclasses__()]:
    colclass()

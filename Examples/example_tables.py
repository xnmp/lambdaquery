from LambdaQuery import *
from datetime import timedelta


class User(Columns):
    table = Table("dim_user", 'u')

    def __init__(self):
        # THIS IS NECESSARY, otherwise cls.table always refers to the same
        # table, and its parents will mutate
        User.makeTable()
        self.baseExpr("user_id", 'uid', primary=True)
        self.baseExpr("country", 'country', etype=str)
        self.baseExpr("password", "pw")
    
    @property
    def isenglish(self):
        eng_countries = ('United States', 'United Kingdom', 'Australia', 'Canada', 'New Zealand')
        return self.country.in_(*eng_countries).label('isenglish')
    
class Project(Columns):
    table = Table("dim_project", "p")

    def __init__(self):
        Project.makeTable()
        self.baseExpr("project_id", 'pid', primary=True)
        self.baseExpr("user_id", "uid", foreign=User, ref='creator')
        self.baseExpr("submit_timestamp", 'ts')

    @property
    def country(self):
        return self.user.country


# ============================================================================
# instantiate all classes so that the primary and foreign keys are all defined
# ============================================================================

for colclass in [subcls for subcls in Columns.__subclasses__()] \
    + [subsubcls for subcls in Columns.__subclasses__() for subsubcls in subcls.__subclasses__()]:
    colclass()
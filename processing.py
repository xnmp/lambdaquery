# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

from LambdaQuery.tables import *
from LambdaQuery.sql import *
from LambdaQuery.other_functions import *

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import datetime as dt
import fl_dbconnect.redshift
import fl_dbconnect.slave_stats


from datetime import timedelta
from itertools import product

# import plotly.plotly as py
# from plotly.tools import set_credentials_file

plt.style.use('ggplot')
plt.rcParams['figure.figsize'] = [13.0, 7.5]
# plt.figure.figsize
pd.options.display.float_format = '{:.3g}'.format
# pd.set_option('display.width', 400)
pd.options.display.max_rows = 30
pd.options.display.max_columns = 10
pd.set_option('max_colwidth', 20)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('use_inf_as_null', True)

# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

def make_rolling(df, var, timestamp='ts', segs=[], window=4, aggby=np.sum):
    if not isinstance(timestamp, list): timestamp = [timestamp]
    if not isinstance(var, list): var = [var]
    if not isinstance(segs, list): segs = [segs]
    df2 = df.groupby(timestamp + segs)[var].agg(aggby)\
            .unstack(segs)\
            .rolling(window=window, min_periods=1).agg(aggby)\
            .iloc[window:-1]
    if segs != []:
        df2.columns = df2.columns.set_levels(
            ['rolling_' + col for col in df2.columns.levels[0]], level=0)
    else:
        df2.columns = ['rolling_' + col for col in df2.columns]
    return df2
pd.DataFrame.make_rolling = make_rolling


def one_hot(df, cols):
    newdf = df.copy()
    for each in cols:
        dummies = pd.get_dummies(newdf[each], prefix=each, drop_first=False)
        newdf = pd.concat([newdf, dummies], axis=1)
        newdf = newdf.drop(each, axis=1)
    return newdf
    

def cancelqueries():
    cn = fl_dbconnect.redshift.DBConnection().open()
    try:
        pid = pd.read_sql('''
            SELECT pid --, user_name, query
            FROM stv_recents
            WHERE status = 'Running'
                ''', cn).iloc[0, 0]
        pd.read_sql(f"SELECT pg_cancel_backend({pid})", cn)
    except:
        print("No running queries")
    cn.close()


def getdata(query, printq=False, outfile='', redshift=True):
    print('Retreiving Data...')
    if printq:
        print(query)
    if redshift:
        cn = fl_dbconnect.redshift.DBConnection().open()
    else:
        cn = fl_dbconnect.slave_stats.DBConnection().open()
    df_orig = pd.read_sql(query, cn)
    if outfile != '':
        df_orig.to_csv('Data/' + outfile)
    cn.close()
    print('Retreived Dataset')
    return df_orig


def getdo(dofunc, overwrite=True, window=28, plot=False, outp=True, printsql=True, **kwargs):
    res = dofunc()
    res.alias = dofunc.__name__[:-1]
    if printsql:
        print(sql(res))
    res.get(overwrite=overwrite, outp=outp)
    print('Getdo: got data')
    if not plot:
        return res
    try:
        res.plot(overwrite=False, window=window, **kwargs)
        plt.savefig('/tmp/' + res.name)
    except:# (AttributeError, KeyError, IndexError, ValueError, TypeError, MemoryError):
        print("Cannot plot")
    return res


def importances(plot=False, features=[]):
    print("Calculating feature importances...")
    importances = pdp.pdp_interact(model, df, features[:2])
    pdp.pdp_interact_plot(importances, features[:2])
    importances.pdp


def eval_metrics(model, X_ts, y_ts, label="Model"):
    from sklearn.metrics import roc_curve, precision_recall_curve
    from sklearn.metrics import f1_score, precision_score, accuracy_score, recall_score
    from sklearn.metrics import roc_auc_score, average_precision_score, ndcg_score, dcg_score, log_loss
    
    preds = model.predict(X_ts)
    probs = model.predict_proba(X_ts)
    try:
        successprobs = probs[:,1]
    except:
        successprobs = probs[:,0]
    counts = pd.Series(y_ts).value_counts()
    
    prevalence = counts[1] / sum(counts)
    accuracy = accuracy_score(y_ts, preds)
    precision = precision_score(y_ts, preds)
    recall = recall_score(y_ts, preds)
    f1 = f1_score(y_ts, preds)
    entropy = log_loss(y_ts, successprobs)
    auc = roc_auc_score(y_ts, successprobs)
    aps = average_precision_score(y_ts, successprobs)

    # this just looks at the guys who were given the 8 highest probabilities and asks if they completed
    # maybe the following is better: look a random samples of 8, look at the dcg, take average over 1000 samples
    
    # dcg = 0
    # for i in range(1000):
    #     randomints = np.random.randint(0, len(y_ts), size=8)
    #     dcg += dcg_score(y_ts[randomints], successprobs[randomints], k=8)
    # dcg = dcg / 1000
    
    return pd.DataFrame(
        [prevalence, accuracy, precision, recall, f1, auc, aps, entropy],
        index = ['prevalence', 'accuracy', 'precision', 'recall', 'f1', 'auc','avg_precision','binary_crossentropy'],
        columns = [label])


# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^
#           AB TESTING
# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^


# def binomial_stats(succ1, succ2=None, fail1=None, fail2=None):
#     from Utils.maths import BayesBinomial
#     if isinstance(succ1, pd.DataFrame):
#         hh = succ1.sort_values('variation').set_index('variation')        
#     else:
#         hh = pd.DataFrame([[succ2,fail2],[succ1, fail1]], 
#                           columns=['successes','failures'], 
#                           index=['control','test'])
#     model = BayesBinomial(hh)
#     # model = Bootstrap(hh)
#     model.analyse()
#     res = copy(model.results[['sample_size','expectation_value',
#                           'worst_case_rel','expected_uplift','best_case_rel','prob_wins']])
#     res['prob_wins'] *= 100
#     return res
    
# pd.DataFrame.abtest = binomial_stats


# def etltime():
#     res = getdata(f'''SELECT max(submit_timestamp) FROM dim_project''').iloc[0,0]
#     print("ETL Time:", etl_time)

# def runTest(test_name, 
#              metric_list=metric_list_default, 
#              tlimit_list=tlimit_list_default, 
#              seg_list=seg_list_default, 
#              generator=getAB,
#              printq=False):
    
#     if not all([hasattr(m, '__name__') for m in metric_list]):
#         raise KeyError("Name your metrics")
    
#     etl_time = etltime()
    
#     with open(f'/tmp/{test_name}_results.txt', 'w') as textout:
        
#         textout.write(f'\n{test_name}\n')
#         lastseg, lastmetric = None, None
#         for s, m, t in product(seg_list, metric_list, tlimit_list):
            
#             etl_limit = Lifted(lambda x: x.ts < etl_time - t)
            
#             # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#             # get the data
#             testres = getAB(test_name, m, t, s & etl_limit)
#             print(testres.sql())
#             resout = testres.get('abtest', overwrite=True).abtest().to_string()
#             # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
            
#             segname = "All" if s is None else s.__name__
#             if segname != lastseg or lastseg is None:
#                 lastseg = segname
#                 textout.write(f'\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━ <span style="font-weight: bold;font-size: 15px;">Results for {lastseg} Users</span> ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n')
#             if m.__name__ != lastmetric or lastmetric is None:
#                 lastmetric = m.__name__
#                 textout.write(f'\n<span style="font-weight: bold;font-size: 14px;">{m.__name__}</span>\n')
#             textout.write(f'\n{m.__name__} - {t}\n')
#             textout.write(resout + '\n')
            
#     from Utils.generate_html import generate_html
#     generate_html(test_name, sourcedir='/tmp/')


# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^
        # QUERY METHODS
# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^

@patch(Query)
def get(self, outfile=None, overwrite=False, datapath="/tmp", outp=True):
    # run the query, return the result as a pandas dataframe
    # self.name = [k for k,v in locals().items() if v is self][0]
    
    if outfile is not None:
        self.name = outfile
    self.data_path = self.alias  # in case we change the name later
    self.data_date = dt.datetime.now()
    if overwrite and hasattr(self, 'data'):
        del self.data
    
    # save the query
    # queryout = copy(self)
    # if hasattr(queryout, 'reduced'):
    #     del queryout.reduced
    #     del self.reduced
    # for tab in queryout.getTables():
    #     if hasattr(tab, 'parents'):
    #         tab.parents = []
    
    # import dill
    # querydump = self.__dict__
    # if hasattr(querydump, 'data'):
    #     del querydump.data
    # dill.dump(querydump, open(f"Queries/{self.name}-{now}", "wb"))

    # cache the result, load data from memory if it's there
    
    if not hasattr(self, 'data'):
        # TODO: get the time
        try:
            if not overwrite:
                print("Loading data from disk...")
            from os import listdir
            from os.path import isfile, join
            datafiles = [f for f in listdir(datapath) if isfile(join(datapath, f))]
            for datafile in datafiles:
                # print(self.data_path, datafile)
                if self.data_path in datafile:
                    if overwrite:
                        import os
                        os.remove(f'{datapath}/{datafile}')
                        print("Overwriting old data.")
                        raise KeyError
                    self.data = pd.read_csv(f'{datapath}/{datafile}', index_col=0)
                    
                    # calculate time difference
                    qtime = pd.datetime.strptime(datafile.replace(f'{self.data_path}-', '')[:19], '%Y-%m-%d %H:%M:%S')
                    timediff = int((self.data_date - qtime).total_seconds())
                    hours =  timediff // 3600
                    if hours > 0:
                        hours = str(hours) + " hours and "
                    else: 
                        hours = ''
                    minutes = timediff % 3600 // 60
                    minutes = str(minutes) + " minutes"
                    print(f"Loaded data from disk. Data is {hours}{minutes} old")
                    break
            else:
                print("No data found on disk. Getting data from database.")
                raise KeyError
        except:
            self.data = getdata(sql(self, display=False))
            if outp:
                print("Saving data to disk...")
                self.data.to_csv(f'{datapath}/{self.data_path}-{self.data_date}.csv')
    return self.data


@patch(Query)
def plot(self, window=28, overwrite=False, aggby=np.sum):
    
    plotdata = self.get(overwrite=overwrite)
    indices, segs, plotvars = getExprTypes(self)
    
    print(indices, segs, plotvars)
    if plotvars == []:
        print("No plotvars to plot")
        return
    if indices == []:
        print("No index")
        return
    
    self.plotdata = plotdata.make_rolling(var=plotvars, segs=segs,
                          timestamp=indices, window=window, aggby=aggby)
    
    if len(self.plotdata) < 20000:
        self.plotdata.plot()
        plt.title(self.alias)
        plt.grid(which='minor', color='0.5', linewidth=0.5, alpha=0.3)
        plt.grid(which='major', color='0.5', linewidth=0.7, alpha=0.7)
        plt.show()
    else:
        print("Too many rows to plot")


def getExprTypes(q0, convertbool=False):
    plotdata = q0.data
    indices, segs, plotvars = [], [], []
    
    for key, expr in q0.columns.items():
        firstrow = plotdata.loc[0, key]
        
        if type(expr) is BaseExpr and type(expr.table) is Table \
          and (key in expr.getTables()[0].tableclass.foreign_keys or expr.isPrimary()):
            continue
        elif isinstance(firstrow, dt.datetime):
            indices.append(key)
        elif isinstance(firstrow, str):
            segs.append(key)
        elif isinstance(firstrow, bool) or isinstance(firstrow, np.bool_):
            # convert bools to integer
            if convertbool:
                plotdata[key] = np.where(plotdata[key], 1, 0)
            else:
                segs.append(key)
        elif isinstance(firstrow, int) or isinstance(firstrow, np.int64):
            numvals = len(plotdata[key].drop_duplicates())
            # if numvals < len(plotdata[key]) / 10 and numvals < 10:
                # segs.append(key)
            if plotdata[key].mean() < 1:
                aggby = np.mean
                plotvars.append(key)
            else:
                plotvars.append(key)
        elif isinstance(firstrow, float):
            if plotdata[key].mean() < 1:
                aggby = np.mean
                plotvars.append(key)
            else:
                plotvars.append(key)
    return indices, segs, plotvars


@patch(Query)
def clean(self, outlier_std=None, reclean=True):
    
    if not hasattr(self, 'cleaned') or reclean:
        res = self.get()
        
        indices, segs, plotvars = getExprTypes(self, convertbool=True)
        
        print("Categorical features:", segs)
        
        res = one_hot(res, segs)
        self.cleaned = res
        
        print("Cleaned Data")
    else:
        print("Data already cleaned")
    
    # fillna including with fancy methods
    # for colname, expr in self.columns.items():
    #     if expr.default is not None:
    #         self.data[colname].fillna(expr.default)
    
    # from fancyimpute import IterativeSVD
    
    # scale the data
    # from sklearn.preprocessing import RobustScaler
    # scaler = RobustScaler()
    # scaler.fit_transform(res[noncategorical])
    
    # remove outliers
    # from scipy import stats
    # outlier std is 5 by default
    # res = res[(np.abs(stats.zscore(res)) < outlier_std).all(axis=1)]
    
    return self.cleaned



# from xgboost import XGBClassifier

@patch(Query)
def predictwith(self, target=None, features=[], models=[], retrain=True, fcond=None):
    
    df = self.clean()
    indices, segs, plotvars = getExprTypes(self, convertbool=True)
    
    if fcond is not None:
        df = df[fcond(df)]
    
    # gather feature names
    if not features:
        features = L(*df.columns) - indices - L(target)
    
    print("Features:", features)
    print("Target:", [target])
    
    if not hasattr(self, 'model') or retrain:
        # train models
        print("Training Model...")
        from sklearn.model_selection import train_test_split
        X = df[features]
        y = df[target]
        X_tr, X_ts, y_tr, y_ts = train_test_split(X, y)
        
        from lightgbm import LGBMClassifier
        from xgboost import XGBClassifier, XGBRegressor
        clf = LGBMClassifier()#is_unbalance=True)
        # clf = XGBClassifier()#scale_pos_weight=879/121)
        clf.fit(X, y)
        self.model = clf
    
    # optimize hyperparameters (really basic)
    # adds the out of fold predictions to the data
    # saves the csv in a different file
    return eval_metrics(clf, X_ts, y_ts)

@patch(Query)
def getFeatures(self):
    indices, segs, plotvars = getExprTypes(self, convertbool=True)
    return L(*self.clean().columns) - indices

@patch(Query)
def isolate(self, target, *isolates, fcond=None, fcondname=None, sample=100000, retrain=True):
    
    df = self.clean()
    
    # if len(isolates) > 0 and hasattr(isolates[0], '__call__'):
        # fcond = isolates[0]
    
    self.predictwith(target, fcond=fcond, retrain=retrain)
    isolates = [iso for iso in isolates if isinstance(iso, str)]
    
    if fcond is not None:
        if fcondname is None:
            import inspect
            fcondname = inspect.getsourcelines(fcond)[0][0][:-1]
        df = df[fcond(df)]
    if sample is not None and sample < len(df):
        df = df.sample(sample)
    
    from pdpbox import pdp
    # if len(isolates) == 0:
    #     isolates = self.getIsolates()
    
    print("Isolates:", isolates)
    print("Calculating feature importances...")
    
    if len(isolates) == 1:
        isolate_data = pdp.pdp_isolate(self.model, df[self.getFeatures() - target], isolates[0],
                                       # num_grid_points=13, 
                                       percentile_range=(5, 95)
                                       )
        try:
            p = pdp.pdp_plot(isolate_data, isolates[0], 
                             plot_params = {'title': "Predicted probability of " + target + f"  |  Condition: {fcondname}"}
                             # plot_org_pts=True, 
                             # plot_lines=True
                             )
        except ValueError:
            print('Cannot plot, try adjusting the number of grid points')
    elif len(isolates) > 1:
        isolate_data = pdp.pdp_interact(self.model, df[self.getFeatures()], isolates, 
                                        num_grid_points=[20, 20], 
                                        percentile_ranges=[(5, 95), (5, 95)],
                                        )
        try:
            p = pdp.pdp_interact_plot(isolate_data, isolates,
                                      plot_params = {'pdp_inter': {'title': "Predicted probability of " + target + f"  |  Condition: {fcondname}"}})
        except ValueError:
            print('Cannot plot, try adjusting the number of grid points')
    # return isolate_data


@patch(Query)
def dissect(self,
            periodstart=None, 
            periodend=dt.datetime.now()):
    # compare the period to all of the other data other period
    df = self.clean()
    
    if periodstart is not None:
        # periodstart = etltime() - timedelta(hours=24)
        inperiod = (df['ts'] > periodstart) & (df['ts'] < periodend)
        df['inperiod'] = np.where(inperiod, 1, 0)
        y = df['inperiod']
    else: 
        y = df[self.getTarget()]
    
    features = [expr for expr in self.getFeatures() if 'ts' not in expr]
    X = df[features]
    
    # X_tr, X_ts, y_tr, y_ts = train_test_split(X, y)
    
    from xgboost import XGBClassifier, plot_importance
    clf = XGBClassifier()
    print("Training model...")
    clf.fit(X, y)
    
    # feature importances
    plot_importance(clf)
    return explain_weights(clf)

# %% ^━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━^



def evaluate(self):
    # options: - pdpbox object
    # plot roc
    # show evaluation metrics
    pass



@patch(Query)
def __setitem__(self):
    # uses sql alter and update
    pass


@patch(Query)
def optimize(self):
    # should it perform one big quey or multiples in different tables?
    pass


@patch(Query)
def create(self, tablename=None):
    '''
    pickle the query object, set a tiimestamp
    create the table in redshift
    write a file named "redshift table name - timestamp"
    contents is the pickled query
    store this information as an attribute of the query
    '''

    self.redshift_table = 'public.{tablename}'
    createtable(tablename, sql(self))
    import pickle
    now = datetime.datetime.now()
    pickle.dump(self, open(f"Queries/{self.name}-{now}", "wb"))

    NewType = type("NewType", (object,), self.columns)

    class NewClass(Columns):
        """create a class corresponding to the table that we just created"""
        table = Table("public." + tablename, tablename)
        def __init__(self):
            self.baseExpr()

    globals()['NewClass'] = NewClass


@patch(Query)
def getsample(self):
    # gets a sample of the query that runs much faster
    
    
    pass


@patch(Query)
def update(self):
    # updates the data
    # adds missing columns and gets latest timestamps
    # uses SQL insert
    pass



def loadqueries():
    # load all the queries

    pass


def findquery():
    # get the query correponding to a table name

    pass


@patch(Query)
def rename():
    # rename a query and rename the csv file
    pass


def rawsql(sql):
    return Query(AndExpr(), Query.unit(), rawsql=rawsql)

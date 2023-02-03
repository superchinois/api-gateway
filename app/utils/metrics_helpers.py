import pandas as pd
import statsmodels.api as sm
import datetime as dt
from os.path import exists
import math
import itertools
import functools
import numpy as np
import re

#
# METRICS HELPER FUNCTIONS
#

DATEFMT="%Y-%m-%d"
def get_today():
  return dt.date.today()
def get_dt_now():
  return dt.datetime.now()
def reduit_acc(x,y,acc):
  delta=x-y
  acc.append(delta)
  return delta

def substract_days_from_today(nb_of_days):
  delta_back=dt.timedelta(days=nb_of_days)
  toDate   = dt.date.today()
  fromDate = (toDate - delta_back).replace(day=1)
  return fromDate

def rewind_x_months(month_ago):
  DAYS_IN_MONTH=30
  def _rewind(date_from):
    return (date_from - dt.timedelta(days=month_ago*DAYS_IN_MONTH)).replace(day=1)
  return _rewind

def compute_ols(dataframe):
  '''
  Computes metrics for a dataframe containing sales data for a set 
  of itemcodes

  dataframe is the sales data frame

  '''
  def _for_itemcode(itemcode):
    qstring="itemcode=='{}'".format(itemcode)# and docdate >'2021-03-03'"
    y=dataframe.query(qstring).loc[:,["docdate","quantity"]].sort_values(by="docdate").set_index("docdate")
    results=None
    if len(y>0):
      rows, cols = y.shape
      yy = y.cumsum()
      X = pd.date_range(yy.index[0], yy.index[yy.shape[0]-1])
      x_size=len(X)
      endog = pd.Series(yy.quantity, index=X)
      endog = endog.interpolate(method='polynomial', order=0)
      endog = pd.DataFrame(endog.values.tolist(), columns=["quantity"])
      X=sm.add_constant(endog.index)
      model = sm.OLS(endog.quantity,X)
      results = model.fit()
    return results

  return _for_itemcode

def extract_dluos(dataframe):
  '''
  Creates an array where each element is an array filled with elements of form
      [itemcode
      docdate
      docnum
      numAtCard
      cardcode
      cardname
      comments
      quantity
      u_dluo
      dscription
      price
      serialnum]
  One array for each itemcode
  
  '''
  uniq_itemcodes = dataframe.itemcode.unique().tolist()
  result=[]
  for itemcode in uniq_itemcodes:
    qry_string="itemcode=='{}'".format(itemcode)
    entrees_march=dataframe.query(qry_string).sort_values(by=["docnum"], ascending=False)
    array = list(entrees_march.values.tolist())
    result.append(array)
  return result

def is_nat(ts):
  return isinstance(ts, pd._libs.tslibs.nattype.NaTType)
  #return isinstance(ts, pd.tslib.NaTType)

def compute_last_lot(itemcode, onhand, dluo_array):
  def process_dluo_array(dluo_array):
    dluo_date = dluo_array[QTY_RECEIPT_INDEX+1]
    if dluo_date is None:
      dluo_date=dt.datetime(dt.MINYEAR, 1,1)
    return dluo_array[2:QTY_RECEIPT_INDEX+1] +[dluo_date.date()]
  '''
  Returns a tuple containing a vector with dluo data in the form of [onhand, dluoArrayData] and 
  a remanining quantity of the oldest lot as a double

  '''
  QTY_RECEIPT_INDEX=7
  array = onhand + list(map(lambda x:x[QTY_RECEIPT_INDEX], dluo_array))
  remainings=[]
  functools.reduce(lambda x,y:reduit_acc(x,y, remainings), array)
  idx = list(enumerate(filter(lambda x:x>0, remainings)))
  dluo_vec=[]
  if len(idx)>0 :
    # 2:QTY_RECEIPT_INDEX+1: skip first two elements
    # last_index+2: up until the last plus one (due to slice)
    last_index=idx[-1][0]
    dluo_vec=onhand+[list(map(lambda x:process_dluo_array(x),dluo_array[slice(last_index+2)]))]
    remaining = idx[-1][1]
  else:
    dluo_vec = onhand+[[process_dluo_array(dluo_array[0])]]
    remaining = onhand[0]
  return dluo_vec, remaining

def remaining_days(dluo_vec):
  now=get_today()
  return list(map(lambda x: -(now-x[2]).days if not is_nat(x[2]) else None, dluo_vec))


# re arrange data into vector
def process_item(masterdata):
  '''
  Given a array of receptions, return a vector of following form
  [itemcode, remaining_last_lot, onhand, vector_of_receptions]
  '''
  def _process(dluo_vec):
    itemcode = dluo_vec[0][0]
    qry_string="itemcode=='{}'".format(itemcode)
    onhand = masterdata.query(qry_string).onhand.tolist()
    vec, remaining = compute_last_lot(itemcode, onhand, dluo_vec)
    return [itemcode]+[remaining]+vec
  return _process

def select_items(receptions_vector, days_ahead):
  today=dt.date.today()
  def delta_days_from_now(date):
    return (date-today).days
  def get_dluo(r):
    RECEPTION_INDEX=  3
    DLUO_INDEX     =  6
    LAST_ELT       = -1
    receptions_list = reversed(r[1][RECEPTION_INDEX])
    dluo = dt.date.min
    for r in receptions_list:
      _dluo = r[DLUO_INDEX]
      if not is_nat(_dluo):
        dluo=_dluo
    return dluo

  return filter(lambda r:abs(delta_days_from_now(get_dluo(r)))<days_ahead, receptions_vector.items())

def dluo_dummy_source(corrected_dluo_file_path):
  #corrected_dluo_file="dluo_corrected.csv"
  return pd.read_csv(corrected_dluo_file_path, sep=";", dtype={"itemcode":str})

class BestBeforeData:
  def __init__(self):
    pass
  def init_with_dataframe(self, df):
    self.sourceDf = df
  def __iter__(self):
    self.sourceIter = self.sourceDf.itertuples()
    return self.sourceIter
  def __next__(self):
    return self.sourceIter.next() 
  def __getitem__(self, key):
    return self.sourceDf[self.sourceDf.itemcode==key]
  
  def docnums(self, itemcode):
    return self[itemcode].docnum.tolist()

def fill_in_blank_dluos(receptions_vector):
  RECEPTIONS_INDEX = 3
  DLUO_INDEX       = 6
def _use_external_data(dluo_source):
  fields=["itemcode", "docnum", "dluo"]
  for row in dluo_source:
    itemcode, sdocnum, sdluo = map(lambda f:getattr(row, f), fields)
    if itemcode in receptions_vector:
      missing_dluos = enumerate(receptions_vector[itemcode][RECEPTIONS_INDEX])
      for missing in missing_dluos:
        missing_index, reception_row = missing
        docnum=reception_row[0]
        if(docnum==sdocnum):
          reception_row[DLUO_INDEX] = dt.datetime.strptime(sdluo, DATEFMT).date()
  return _use_external_data

def rget_attr(obj, attributes):
  '''
  Process attributes to get the value from attributes path
  '''
  attr_count = len(attributes)
  if attr_count==1:
    return getattr(obj, attributes[0])
  else:
    return rget_attr(getattr(obj, attributes[0]), attributes[1:])
    
def extract_metrics(forecast):
  attributes = ["rsquared", "params.x1"]
  return map(lambda x: rget_attr(forecast, x) if forecast != None else "", map(lambda x: x.split("."), attributes))

def extract_metrics_from(forecasts):
  return map(lambda f: extract_metrics(f), forecasts)

def ecoulement(row):
  now = get_dt_now()
  d1 = row.dluo1
  if is_nat(d1):
    delta=30
  else:
    futureDate = dt.datetime(d1.year, d1.month, d1.day)
    delta = (futureDate-now).days
  a_coeff = row.a if isinstance(row.a, np.float64) else 0
  delta = delta if delta > 0 else 0
  return delta*a_coeff

def add_metrics(merged, receptions_vector):
  def a_terme(row):
    delta = row.proche-row.ecoulmt
    
    if delta < 0 or row.proche==0:
      return 0
    else:
      return delta/row.proche

  def countdown(row):
    def delta_days_from_now(date):
      today=dt.date.today()
      return (date-today).days
    if is_nat(row.dluo1):
      return -1
    else:
      return delta_days_from_now(row.dluo1)
  
  merged["proche"] = merged.apply(lambda row: receptions_vector[row.itemcode][1],axis=1)
  merged["dluo1"] = merged.apply(lambda row: receptions_vector[row.itemcode][3][-1][-1],axis=1)
  merged["countdown"] = merged.apply(lambda row: countdown(row),axis=1)
  merged["vmm"] = merged.apply(lambda row: 30*row.a,axis=1)
  merged["ecoulmt"] = merged.apply(lambda row: math.floor(ecoulement(row)),axis=1)
  merged["a_terme"] = merged.apply(lambda row: a_terme(row),axis=1)
  merged["lot_count"] = merged.apply(lambda row: len(receptions_vector[row.itemcode][3]),axis=1)
  merged["recept"] = merged.apply(lambda row: reduce_dates_to_string(receptions_vector[row.itemcode][3]),axis=1)
  return merged

def remap_receptions(data, fn=lambda x:x[0][0]):
  return {fn(x):x for x in data}

def remap_docnums(extracted_dluos):
  remap_docnums = {}
  for receptions in extracted_dluos:
    for elt in receptions:
      itemcode, timestamp, docnum, *rest = elt 
      if docnum not in remap_docnums:
        remap_docnums[docnum]=timestamp.date()
  return remap_docnums

def reduce_dates_to_string(receptions_array):
  return "  ".join(["/".join([str(a[x]) for x in [0,5,6]]) for a in receptions_array])

def compute_forecasts(masterdata, itemcodes):
  keys = ["r2", "a"]
  output_cols = ["itemcode", "itemname","onhand", "revient"]
  def _compute_metrics(sales_df):
    compute_sales_for=compute_ols(sales_df)
    forecasts = list(map(lambda code:compute_sales_for(code),itemcodes))
    r=zip(itemcodes, map(list, extract_metrics_from(forecasts)))
    forecast_params = map(lambda x:{**{'itemcode':x[0]}, **{k:v for k,v in zip(keys, x[1])}}, r)
    params=pd.DataFrame(forecast_params)
    merged = pd.merge(masterdata.loc[:,output_cols], params, on="itemcode", how="inner")
    return merged
  return _compute_metrics 

def compute_receptions_vector(masterdata, receptions, added_dluo_df=None):
  extracted_dluos = extract_dluos(receptions)
  receptions_vector=remap_receptions(list(map(lambda x: process_item(masterdata)(x),extracted_dluos)), lambda x:x[0])
  if added_dluo_df:
    bbd = BestBeforeData()
    bbd.init_with_dataframe(added_dluo_df)
    fill_in_blank_dluos(receptions_vector)(bbd)
  return receptions_vector


#
# PIVOT ON ITEMS
#
def pivot_on_items(mongo_data_df):
    values=["linetotal", "revient"]
    index=["itemcode", "itemname" ,"categorie"]
    columns=["month"]
    pivot = pd.pivot_table(mongo_data_df, values, index, columns, aggfunc='sum')
    by_items_df = pd.DataFrame(pivot.fillna(0).to_records())
    by_items_df = rename_labels(by_items_df, values, mongo_data_df)
    return by_items_df

#
# PIVOT ON CATEGORIES
#
def pivot_on_categories(mongo_data_df):
    values=["linetotal", "revient"]
    index=["categorie"]
    columns=["month"]
    pivot = pd.pivot_table(mongo_data_df, values, index, columns, aggfunc='sum')
    by_cat_df = pd.DataFrame(pivot.fillna(0).to_records())
    by_cat_df = rename_labels(by_cat_df, values, mongo_data_df)
    return by_cat_df

def add_revenues_metrics(dataframe):
    columns = dataframe.columns.values.tolist()
    linetotals = list(filter(lambda l: "linetotal/" in l, columns))
    revients = list(filter(lambda l: "revient/" in l, columns))
    np1 = dataframe.loc[:,linetotals].fillna(0).to_numpy()
    np2 = dataframe.loc[:,revients].fillna(0).to_numpy()
    dataframe["freq"] = np.count_nonzero(np1, axis=1)
    dataframe["caht"] = np.sum(np1, axis=1)
    dataframe["revient"] = np.sum(np2, axis=1)
    dataframe["marg_val"] = [r.caht-r.revient for r in dataframe.itertuples()]
    return dataframe

def build_months_labels(pdPeriodList):
    date_regex=r'\d{4}-\d{2}'
    nonEmptyMonths = list(map(lambda x:re.findall(date_regex, x.strftime("%Y-%m"))[0], pdPeriodList))
    return nonEmptyMonths

def build_renamed_labels(values, months_label):
    pivot_sum_tpl="('{}', Period('{}', 'M'))"
    renamed = {pivot_sum_tpl.format(x[0], x[1]):f"{x[0]}/{x[1]}" for x in itertools.product(values, months_label)}
    return renamed

def rename_labels(dataframe, values, dfForPeriods):
    periods = dfForPeriods["month"].unique().tolist()
    months_label = build_months_labels(periods)
    renamed = build_renamed_labels(values, months_label)
    return dataframe.rename(columns=renamed)
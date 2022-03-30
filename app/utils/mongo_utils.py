# coding: utf-8
# 
import itertools
import functools
from app.dao import dao
import pandas as pd

def apply_fn(transform_fn):
  def _qry_part(value):
    return transform_fn(value)
  return _qry_part

class SapQuery:

  yearOf   = apply_fn(lambda x: f"year({x}) as annee")
  monthOf  = apply_fn(lambda x: f"month({x}) as mois")
  renameAs = lambda y: apply_fn(lambda x: f"{x} as {y}")
  identity     = lambda x: x
  inverseValue = lambda x: f"-{x}"

  def inverse_fields(fields):
    fields_to_inverse = ["quantity", "linetotal"]
    return map(lambda x: [x[0], inverseValue] if x[0] in fields_to_inverse else x, fields)

  def assoc_fields_and(fields, transform):
    return list(map(lambda f: [f, transform], fields))

  def selected_fields_str(table_labels, fields):
    def chain(x):
      return list(itertools.chain(x[0], x[1]))
    def product(x):
      return list(itertools.product(x[0], x[1]))
    def assoc_table_to_fields(db_labels, fields):
      _labels = map(lambda x: [[x]], db_labels)
      zips = zip(_labels, fields)
      return zips
    fields_array = functools.reduce(lambda x,y: itertools.chain(x,y)
                    , map(lambda x: list(map(lambda y: chain(y), x)),map(lambda x: product(x), assoc_table_to_fields(table_labels, fields))))
    return ",".join(map(lambda x: x[2](".".join(x[0:2])), fields_array))

  assoc_label_dbtable= {
  "t0": "dbo.inv1",
  "t1": "dbo.oinv",
  "t2": "dbo.oitm",
  "t3": "dbo.ocrd"
  }
  fields = [
    assoc_fields_and(["itemcode", "dscription", "quantity", "linetotal", "grossbuypr", "targettype"], identity), # t0
    assoc_fields_and(["docnum", "docentry", "docdate", "doctime", "cardname"], identity), # t1
    [["itmsgrpcod", identity],["cardcode", renameAs("supplier")]], # t2
    [["cardcode", identity]] #t3
  ]

  query = """select {invoices_fields} 
    from dbo.inv1 t0 
    join dbo.oinv t1 on t1.docentry=t0.docentry and {filter}
    join dbo.ocrd t3 on t3.cardcode=t1.cardcode and {clients}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode
    union all
    select {rinvoices_fields} 
    from dbo.rin1 t0 
    join dbo.orin t1 on t1.docentry=t0.docentry and {filter}
    join dbo.ocrd t3 on t3.cardcode=t1.cardcode and {clients}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode"""

  db_labels = list(assoc_label_dbtable.keys())

  def params_withFilter(filter):
    def inverse_fields(fields):
      fields_to_inverse = ["quantity", "linetotal"]
      return map(lambda x: [x[0], SapQuery.inverseValue] if x[0] in fields_to_inverse else x, fields)

    return {
    "invoices_fields":SapQuery.selected_fields_str(SapQuery.db_labels, SapQuery.fields),
    "rinvoices_fields":SapQuery.selected_fields_str(SapQuery.db_labels, map(lambda x: inverse_fields(x) ,SapQuery.fields)),
    "filter":filter,
    "clients":"({col_name}.qrygroup1='Y' or {col_name}.qrygroup18='Y')".format(col_name="t3")}

  def __init__(self, dao):
    self.dao = dao

  def get_data_from_sap_as_df(self, starting_docnum):
    def toIsoFormat(isoDay, sapDoctime):
      strTime = str(sapDoctime)
      minutes = strTime[-2:]
      hours = strTime[:-2].zfill(2)
      isoTime="{hours}:{minutes}:000Z".format(hours=hours, minutes=minutes)
      return f"{isoDay}T{isoTime}"

    params = SapQuery.params_withFilter("{col_name}.docnum>'{docnum}'".format(col_name="t1", docnum=starting_docnum))
    updated_data = self.dao.execute_query(SapQuery.query.format_map(params))
    updated_data["isodate"] = [toIsoFormat(row.docdate.strftime("%Y-%m-%d"), row.doctime) for row in updated_data.itertuples()]
    updated_data['isodate'] = pd.to_datetime(updated_data['isodate'])
    updated_data['docdate'] = pd.to_datetime(updated_data['docdate'])
    updated_data.fillna("", inplace=True)
    return updated_data

queryUpdateCache = SapQuery(dao)
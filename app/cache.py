from app.dao import dao
from app.cache_dao import cache_dao, mongo_sales_atDate

from flask_caching import Cache
from app.utils.mongo_utils import queryUpdateCache

import os
import pandas as pd
import datetime as dt

cache = Cache(config={'CACHE_TYPE': 'simple'})

@cache.cached(timeout=1800, key_prefix="masterlist")
def fetch_master_itemlist():
  result = dao.getMasterDataDf()
  return result

@cache.cached(timeout=3600, key_prefix="importfile")
def compute_from_importFile(directoryPath, file):
  # CONTENU DU FICHIER IMPORTATIONS
  excel_filepath = os.path.join(directoryPath, file)
  df = pd.read_excel(excel_filepath, sheet_name=0, dtype={"ETA":str, "Dossier":str})
  cols = list(map(lambda x: x, df.columns[0:8]))
  associations = ['ETA', 'DOSSIER', 'DEPOTAGE', 'PREVISIONEL','TC', 'CONTIENT', 'FRS', 'SAP']
  workingDf = df.iloc[:, [0,1,2,3,4,5,6,7]].fillna(0)
  workingDf.rename(columns={k:v for (k,v) in zip(cols, associations)}, inplace=True)
  #rowPredicate = "TC!=0 & DOSSIER!=0 & ETA != 0 & DEPOTAGE == 0"
  rowPredicate = "ETA != 0"
  dffiltered = workingDf.query(rowPredicate)
  now = dt.datetime.now()
  day = dt.datetime(2020, 1, 1)
  recentDf = dffiltered.query("ETA > '{}'".format(day))
  return recentDf

@cache.cached(timeout=3600, key_prefix="receptions")
def compute_receptions_from_date(fromDate):
  return dao.getReceptionsMarchandise(fromDate)

def bin_horaires(isodate):
    plage_horaire=list(range(6,17))
    plage_labels = [f"{str(h[0]).zfill(2)}:00-{str(h[1]).zfill(2)}:00" for h in zip(range(5,16), range(6,17))]
    heures = [dt.time(h) for h in plage_horaire]
    heures_length = len(heures)
    cursor=0
    while cursor< heures_length and isodate.time()>=heures[cursor]:
        cursor=cursor+1
    if cursor<heures_length:
        return plage_labels[cursor]
    return "outside_range"

@cache.cached(timeout=600, key_prefix="customers")
def get_customers_lead():
  last_docnum = cache_dao.last_record()["docnum"]
  updated_data = queryUpdateCache.get_data_from_sap_as_df(last_docnum)
  if not updated_data.empty:
    cache_dao.importFromDataframe(updated_data)
  params = ["hour", "minute", "second", "microsecond"]
  today = dt.datetime.today().replace(**{k:0 for k in params})
  raw_data = cache_dao.apply_aggregate(*mongo_sales_atDate(today))
  data_df = pd.DataFrame(list(raw_data))
  data_df["time"] = [str(row.isodate).split(" ")[1][0:5] for row in data_df.itertuples()]
  data_df["plage"]=[bin_horaires(row.isodate) for row in data_df.itertuples()]
  return data_df
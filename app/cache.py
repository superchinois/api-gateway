from app.dao import dao
from flask_caching import Cache

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
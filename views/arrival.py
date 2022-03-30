from flask import Blueprint
from flask import jsonify, request, current_app
from app.utils.flask_helpers import build_response
from app.dao import dao
from app.cache import compute_from_importFile
import os, json, re
import pandas as pd
import datetime as dt

arrival = Blueprint("arrival", __name__)

@arrival.route("/arrival/next/<string:itemcode>")
def getNextArrivalForItemcode(itemcode):
  excel_directory, excel_file = [current_app.config[x] for x in ["IMPORT_DIRECTORY_PATH","ITEMS_IMPORT_FILE"]]
  df_import = compute_from_importFile(excel_directory, excel_file)
  cde_query = """select t0.itemcode, t0.dscription, t0.quantity from dbo.por1 t0 
  join dbo.opor t1 on t1.docentry=t0.docentry and t1.docentry in ({})
  """
  itemsDfs=[]
  dossiers = dao.purchaseOrdersWithFileNumber()
  for doc in dossiers:
    itemsDf = dao.execute_query(cde_query.format(doc[1].docentry))
    itemsDf["DOSSIER"]=doc[0]
    itemsDfs.append(itemsDf)

  allCdeItems = pd.concat(itemsDfs)
  all_pv = pd.pivot_table(allCdeItems, index=["itemcode","dscription", "DOSSIER"],
     values=['quantity'],
  #   columns=['annee','mois'],
     aggfunc=[np.sum],
     fill_value=0)
  allDf = pd.DataFrame(all_pv.to_records())
  allDf.rename(columns={"('sum', 'quantity')":"quantity"}, inplace=True)
  col_toDisplay=["itemcode","dscription","quantity","ETA"]
  itemsOnOrder = pd.merge(allDf, df_import, on='DOSSIER', how="inner").loc[:,col_toDisplay]
  jsonResponse = itemsOnOrder.query("itemcode=='{}'".format(itemcode)).to_json(orient='records')
  return build_response(jsonResponse)

@arrival.route("/arrival/next", methods=["GET"])
def getNextArrivalItems():
  excel_directory, excel_file = [current_app.config[x] for x in ["IMPORT_DIRECTORY_PATH","ITEMS_IMPORT_FILE"]]
  excel_directory = os.sep.join([arrival.root_path, "../resources"])
  df_import = compute_from_importFile(excel_directory, excel_file)
  cde_query = """select t0.itemcode, t0.dscription, t0.quantity from dbo.por1 t0 
  join dbo.opor t1 on t1.docentry=t0.docentry and t1.docentry in ({})"""
  eta_like_date = df_import["ETA"].str.contains('[0-9]{4}-[0-9]{2}-[0-9]{2}', regex=True)
  #or DEPOTAGE.str.contains('BAE')
  filtered_df = df_import.query("SAP != 0 and (DEPOTAGE==0)", engine="python").loc[eta_like_date]
  docentries = []
  itemsDfs=[]
  for s in filtered_df.itertuples():
    entries = re.findall(r'\d{4,}', str(s.SAP))
    if len(entries) >0 :
      docentries = ",".join(["'{}'".format(x) for x in entries])
      itemsDf = dao.execute_query(cde_query.format(docentries))
      itemsDf["DOSSIER"]=s.DOSSIER
      itemsDf["ETA"]=s.ETA.split(" ")[0]
      itemsDf["TC"]=s.TC
      itemsDfs.append(itemsDf)
      
  allCdeItems = pd.concat(itemsDfs)
  jsonResponse = allCdeItems.to_json(orient='records')
  return build_response(jsonResponse)
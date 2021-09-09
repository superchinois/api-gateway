from flask import Blueprint
from flask import jsonify, request, current_app
from app.flask_helpers import build_response, send_file_response
from app.dao import dao
from app.cache import fetch_master_itemlist
from app.xlsxwriter_utils import to_size_col, build_formats_for
import app.metrics_helpers as dluo_utils
import os, re
import datetime as dt
import pytz
import pandas as pd
from itertools import chain
import json, gspread
import gspread
from oauth2client.service_account import ServiceAccountCredentials

from io import BytesIO

items = Blueprint("items", __name__)


scope = ["https://spreadsheets.google.com/feeds"
        ,"https://www.googleapis.com/auth/spreadsheets"
        ,"https://www.googleapis.com/auth/drive.file"
        ,"https://www.googleapis.com/auth/drive"]
DATE_FMT="%Y-%m-%d"

def get_timestamp():
  UTC = pytz.utc
  IST = pytz.timezone('Indian/Reunion')
  now = dt.datetime.now(IST)
  return now.strftime("%y-%m-%dT%H:%M")

def get_google_sheet_client(config):
  GOOGLE_SHEETS_KEY_FILE = config["GOOGLE_CREDENTIALS"]
  credential = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_SHEETS_KEY_FILE,scope)
  return gspread.authorize(credential)

def get_working_sheet_id(config):
  return config["SHEET_ID"]

@items.route('/items/inventory-sheets', methods=['POST'])
def add_items_to_inventory_sheet():
  if request.is_json:
    posted_data = request.get_json()
    items = posted_data["items"]
    values=["itemcode","itemname","id","building","location", "detail_location", "created_date", "updated_date"]
    extract_fields=values[slice(6)]
    rows = list(map(lambda item: list(map(lambda f: item[f] if f in item else "",extract_fields))+[get_timestamp(),""],items))
    # [for testing] Append data to a fixed google sheet
    sheet_id=get_working_sheet_id(current_app.config)
    gc=get_google_sheet_client(current_app.config)
    workbook = gc.open_by_key(sheet_id)
    worksheet = workbook.get_worksheet(0)
    response = worksheet.append_rows(rows, value_input_option='USER_ENTERED')
    return build_response(response)

@items.route('/items', methods=['POST'])
def get_items_by_codes():
  def matching_items(x, enumerated_codes):
    result = list(filter(lambda item:re.match(x[1],item[1]),enumerated_codes))
    return result

  def process_df(df):
    if len(df)>0:
      row = df[0]
      return {k:v for k,v in zip(columns, row)}
    else:
      return {}

  ITEMCODE_REG = current_app.config["ITEMCODE_REGEX"]
  CODEBARS_REG = current_app.config["CODEBARS_REGEX"]
  code_types=["itemcode", "codebars"]
  if request.is_json:
    master = fetch_master_itemlist()
    posted_data = request.get_json()
    codes = posted_data["itemcodes"]
    enumerated_codes = list(enumerate(codes))
    filtered = map(lambda x: (x[0], matching_items(x, enumerated_codes)),zip(code_types, [ITEMCODE_REG, CODEBARS_REG]))
    res=map(lambda _type: list(map(lambda x: (x,_type[0]),_type[1])),filtered)
    items = sorted(list(chain.from_iterable(res)), key=lambda x:x[0][0])
    result = []
    for item in items:
      queried = master[master[item[1]]==item[0][1]]
      result.append(queried)
    columns = result[0].columns.tolist()
    dfs_array = list((map(lambda d: d.values.tolist(), result)))
    json_result = list(map(lambda x: process_df(x), dfs_array))

    #map(lambda f:master[master[f[0]].isin(f[1])],filtered)
    #result = pd.concat(map(lambda f: master[master[f[0]].isin(f[1])],filtered))
    #return build_response(dao.dfToJson(result))
    return build_response(json.dumps(json_result))

@items.route('/items/<string:code>')
def get_item_by_code(code):
  ITEMCODE_REG = current_app.config["ITEMCODE_REGEX"]
  CODEBARS_REG = current_app.config["CODEBARS_REGEX"]
  master = fetch_master_itemlist()
  onSelectField = ""
  if re.match(CODEBARS_REG, code):
    onSelectField="codebars"
  if re.match(ITEMCODE_REG, code):
    onSelectField="itemcode"

  if len(onSelectField)>0:
    result = master.query("{}=='{}'".format(onSelectField, code))
    return build_response(dao.dfToJson(result))
  return jsonify(message="code \'{}\' is not well formatted".format(code)), 400


@items.route('/items', methods=['GET'])
def items_routing():
  search_param = request.args.get("search")
  supplier_param = request.args.get("cardcode")
  master = fetch_master_itemlist()
  if search_param:
    pattern='.*'+search_param.upper().replace(' ','.*')
    result = master[master["itemname"].str.contains(pattern)]
    return build_response(dao.dfToJson(result.query("sellitem=='Y'")))
  if supplier_param:
    items = master.query("cardcode=='{}' and sellitem=='Y'".format(supplier_param))
    return build_response(dao.dfToJson(items.sort_values(by=["itemname"])))
  return build_response(dao.dfToJson(master))

@items.route('/items/stats/<string:itemcode>', methods=['GET'])
def sales_stats(itemcode):
  fromDateIso = request.args.get("from-date")
  movingAvg = request.args.get("moving-avg")
  fromDate = dt.datetime.strptime(fromDateIso,DATE_FMT).date()
  if movingAvg:
    result = dao.getSalesStatsforItem(itemcode, fromDate, int(movingAvg))
  else :
    result = dao.getSalesStatsforItem(itemcode, fromDate)
  return build_response(result)

@items.route('/items/stats/sales/<string:itemcode>/<string:date>', methods=['GET'])
def salesForItemAtDate(itemcode, date):
  resultDf = dao.getSalesForItemAtDate(itemcode, date)
  return build_response(resultDf.to_json(orient="records"))

@items.route('/items/discounts/<string:itemcode>', methods=['GET'])
def discountPeriods(itemcode):
  fromDateIso = request.args.get("from-date")
  result = dao.getDiscountedItemsFromDate(fromDateIso)
  item_result = result.query("itemcode=='{}'".format(itemcode)).loc[:,["itemcode","discount","fromdate","todate"]]
  return build_response(item_result.to_json(orient="records"))

@items.route('/items/dluo', methods=['GET'])
def compute_dluo_metrics():
  two_yeas_in_days = 2*365
  days_within_dluo = 250
  delta_back=122
  delta_back=dt.timedelta(days=delta_back)

  toDate   = dt.date.today()
  fromDate = (toDate - delta_back).replace(day=1)
  origin_reception=(toDate - dt.timedelta(days=two_yeas_in_days)).replace(month=1, day=1).strftime(DATE_FMT)
  masterdata = fetch_master_itemlist()
  receptions = dao.getReceptionsMarchandise(origin_reception)
  receptions_vector = dluo_utils.compute_receptions_vector(masterdata, receptions)

  itemcodes = list(map(lambda x:x[0], dluo_utils.select_items(receptions_vector, days_within_dluo)))
  sales_df = dao.compute_sales_for_itemcodes_betweenDates(itemcodes, fromDate, toDate)
  dluos = dluo_utils.compute_forecasts(masterdata,itemcodes)(sales_df)
  dluo_utils.add_metrics(dluos, receptions_vector)
  displayed_order=[
    "dluo1"
    ,"itemcode"
    ,"itemname"
    ,"onhand"
    ,"proche"
    ,"lot_count"
    ,"a_terme"
    ,"ecoulmt"
    ,"countdown"
    ,"a"
    ,"r2"
    ,"vmm"
    ,"filtre"
    #,"revient"
    ,"recept"
  ]
  def _apply_formats(sheetname, worksheet, formats, sizes, rows, columns, dataframe=None):
    thresholds = [0,0.3,0.7,1]
    fmt=["good","neutral","bad"]
    for i in range(3):
        worksheet.conditional_format("H2:H{}".format(rows+1), 
                          {"type":'cell', "criteria":'between', 'minimum':  thresholds[i],
                                       'maximum':  thresholds[i+1],"format":formats[fmt[i]]})
    worksheet.conditional_format("G2:G{}".format(rows+1), 
                              {"type":'cell', "criteria":'>', "value":'1', "format":formats["error"]})
    for (col, size, fmt) in sizes:
        worksheet.set_column(col, size, formats[fmt])

  def _add_filters(df):
      df["filtre"] = df.apply(lambda row: 1 if row.ecoulmt<row.proche and row.countdown>0 else 0,axis=1)
      
  def _output_excel(writer, sheetname, dataframe, sizes, apply_formats_fn):
      r, c = dataframe.shape # number of rows and columns
      dataframe.to_excel(writer, sheetname)
      # Get the xlsxwriter workbook and worksheet objects.
      workbook  = writer.book
      formats = build_formats_for(workbook)
      worksheet = writer.sheets[sheetname]
      worksheet.freeze_panes(1,4)
      worksheet.autofilter(0,0,r,c)
      apply_formats_fn(sheetname, worksheet, formats, sizes, r, c, dataframe)

  _add_filters(dluos) 
  outDf = dluos[dluos.onhand>0].loc[:,displayed_order]

  output = BytesIO()
  with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    resize_cols = [
        ["B:B", to_size_col(1), "date1"]
        ,["D:D", to_size_col(6), "no_format"]
        ,["H:H", to_size_col(0.7), "percents"]
        ,["M:M", to_size_col(0.7), "number"]
        ,["N:N", to_size_col(0.7), "no_format"]
        ,["O:O", to_size_col(6), "no_format"]
    ]
    _output_excel(writer, "Sheet1", outDf.sort_values(by=["dluo1"]), resize_cols, _apply_formats)
  return send_file_response(output, f"dluos_{toDate.strftime(DATE_FMT)}.xlsx")


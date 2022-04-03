from flask import Blueprint
from flask import jsonify, request, current_app
from app.dao import dao
from app.cache_dao import cache_dao
from app.cache import fetch_master_itemlist, compute_receptions_from_date, cache
from app.utils.flask_helpers import build_response, send_file_response
from app.utils.xlsxwriter_utils import to_size_col, build_formats_for
from app.utils.query_utils import get_first_values, compute_months_dict_betweenDates, toJoinedString, sales_for_groupCodes
from app.utils.query_utils import build_period
import app.utils.metrics_helpers as dluo_utils
import os, re
import datetime as dt
import pytz
import pandas as pd
from itertools import chain
import json, gspread
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import functools

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

def _output_excel(writer, sheetname, dataframe, sizes, apply_formats_fn):
    r, c = dataframe.shape # number of rows and columns
    dataframe.to_excel(writer, sheetname, index=False)
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    formats = build_formats_for(workbook)
    worksheet = writer.sheets[sheetname]
    worksheet.freeze_panes(1,1)
    worksheet.autofilter(0,0,r,c)
    apply_formats_fn(sheetname, worksheet, formats, sizes, r, c, dataframe)

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
    pattern='.*'+search_param.upper().replace(' ','.*')+'.*'
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
  fromDate = dt.datetime.strptime(fromDateIso,DATE_FMT)
  nowDate = dt.datetime.now()
  if movingAvg:
    result = cache_dao.getSalesStatsforItem(itemcode, fromDate, nowDate, int(movingAvg))
  else :
    result = cache_dao.getSalesStatsforItem(itemcode, fromDate, nowDate)
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

  toDate   = dt.datetime.now()
  fromDate = (toDate - delta_back).replace(day=1)
  origin_reception=(toDate - dt.timedelta(days=two_yeas_in_days)).replace(month=1, day=1).strftime(DATE_FMT)
  masterdata = fetch_master_itemlist()
  receptions = dao.getReceptionsMarchandise(origin_reception)
  receptions_vector = dluo_utils.compute_receptions_vector(masterdata, receptions)

  itemcodes = list(map(lambda x:x[0], dluo_utils.select_items(receptions_vector, days_within_dluo)))
  sales_df = cache_dao.compute_sales_for_itemcodes_betweenDates(itemcodes, fromDate, toDate)
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

@items.route('/items/historique/<string:itemcode>', methods=['GET'])
def compute_item_sales(itemcode):
  def _apply_formats(sheetname, worksheet, formats, sizes, rows, columns, dataframe=None):
    for (col, size, fmt) in sizes:
      worksheet.set_column(col, size, formats[fmt])
  masterdata = fetch_master_itemlist()
  itemname = get_first_values(masterdata.query("itemcode=='{}'".format(itemcode)), "itemname")
  delta_back=122
  delta_back=dt.timedelta(days=delta_back)
  toDate   = dt.datetime.now()
  fromDate = (toDate - delta_back).replace(day=1)
  #periods = compute_months_dict_betweenDates(fromDate,toDate)
  #commaJoined=toJoinedString(",")
  #itemcodes = commaJoined([itemcode])
  histo_item = cache_dao.getSalesForItem(itemcode, fromDate, toDate)#dao.compute_sales_for_itemcodes(itemcodes, periods)
  output = BytesIO()
  with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    resize_cols = [
    ["A:A", to_size_col(3.95), "no_format"]
    ,["B:C", to_size_col(0.8), "no_format"]
    ,["D:H", to_size_col(1), "no_format"]
    ,["J:J", to_size_col(1.3), "percents"]
    ]
    _output_excel(writer, "Sheet1", histo_item, resize_cols, _apply_formats)

  return send_file_response(output, f"{itemname}_{toDate.strftime(DATE_FMT)}.xlsx")


@items.route('/items/receptions/<string:itemcode>', methods=['GET'])
def compute_receptions(itemcode):
  def reduit_acc(x,y,acc):
    delta=x-y
    acc.append(delta)
    return delta

  def compute_last_receptions(onhand, receptions):
    stock_quantities = [onhand] + receptions.quantity.values.tolist()
    remainings=[]
    functools.reduce(lambda x,y:reduit_acc(x,y, remainings), stock_quantities)
    idx = list(enumerate(filter(lambda x:x>0, remainings)))
    current_active_receptions=[]
    last_item_index=1
    if len(idx)>0 :
      last_index=idx[-1][0]
      last_item_index = last_index+2
    current_active_receptions = receptions.iloc[0:last_item_index,:]
    return current_active_receptions

  fromDateIso = request.args.get("from-date")
  selected_onhand = request.args.get("onhand")
  if fromDateIso:
    cache.clear()
  if not fromDateIso:
    delta_back=365*2
    fromDateIso=dluo_utils.substract_days_from_today(delta_back)
  if not selected_onhand:
    masterdata = fetch_master_itemlist()
    selected_onhand=masterdata[masterdata.itemcode==itemcode].onhand.values.tolist()[0]

  receptions = compute_receptions_from_date(fromDateIso)
  receptions = receptions.sort_values(by=["docdate"], ascending=False)
  last_receptions = compute_last_receptions(selected_onhand, receptions[receptions.itemcode==itemcode])
  output_cols=["itemcode","dscription", "quantity", "docdate", "comments", "u_dluo"]
  json_receptions=last_receptions.loc[:,output_cols].to_json(orient="records", date_format="iso")
  return build_response(json_receptions)


@items.route('/items/unsold', methods=['GET'])
def compute_unsold_items():

  def sanitize_ratio(row):
    last_quantity = row.last_quantity
    if isinstance(last_quantity, str):
      last_quantity_len=len(last_quantity)
      if last_quantity_len>0:
        return float(row.onhand)/float(row.last_quantity)
      else:
        return None
    else:
        return float(row.onhand)/float(row.last_quantity)

  def build_sanitize_date(originDate):
    def _sanitize_date(row, today):
      since_origin = (today-originDate).days+1
      DATE_FMT="%Y-%m-%d %H:%M:%S"
      if (not pd.isna(row.last_reception)):
        last_reception = str(row.last_reception)
        last_reception_len = len(last_reception)
        if last_reception_len>0 :
          date=dt.datetime.strptime(last_reception, DATE_FMT)
          return (today-date).days
        else: 
          return since_origin
      else:
        return since_origin
    return _sanitize_date

  def apply_formats_1(sheetname, worksheet, formats, sizes, rows, columns, dataframe=None):
    worksheet.conditional_format("C2:C{}".format(rows+1), 
                            {"type":'cell', "criteria":'>', "value":'{}'.format(0.5), "format":formats["bad"]})
    worksheet.conditional_format("B2:B{}".format(rows+1), 
                            {"type":'formula', "criteria":"${sheetname}.$C2>0.5".format(sheetname=sheetname), "format":formats["bad"]})
    for (col, size, fmt) in sizes:
      worksheet.set_column(col, size, formats[fmt])

  three_yeas_in_days = 3*365
  toDate   = dt.datetime.today()
  isoTodayArray = toDate.strftime(DATE_FMT).split("-")
  year=isoTodayArray[0]
  month=isoTodayArray[1]

  origin_reception=(toDate - dt.timedelta(days=three_yeas_in_days)).replace(month=1, day=1)
  sanitize_date = build_sanitize_date(origin_reception)
  groupcode=request.args.get("groupcode")
  if groupcode:
    fromDateIso = request.args.get("from-date")
    if fromDateIso:
      fromDateIsoSplit = fromDateIso.split("-")
      year = fromDateIsoSplit[0]
      month = fromDateIsoSplit[1]
    selected_period=build_period(year, [month])
    receptions = compute_receptions_from_date(origin_reception.strftime(DATE_FMT))
    filtered_master = fetch_master_itemlist().query("itmsgrpcod=={}".format(groupcode))
    output_df = dao.compute_unsold_items(filtered_master, selected_period, [groupcode], receptions) # [groupcode] because expect an array
    output_df["ratio"] = [sanitize_ratio(row) for row in output_df.itertuples()] # FORMAT AS PERCENTAGE
    output_df["days_since"]=[sanitize_date(row, toDate) for row in output_df.itertuples()]
    output_cols = ["itemcode", "itemname","ratio", "days_since", "last_reception", "last_quantity", "onhand", "cardname"]
    output_df = output_df.sort_values(["days_since", "ratio"], ascending=[1,0])
    output_df = output_df.loc[:,output_cols]
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
      resize_cols = [
           ["B:B", to_size_col(6.6), "no_format"]
          ,["C:C", to_size_col(1), "percents"]
          ,["E:E", to_size_col(2), "date1"]
      ]
      _output_excel(writer, "Sheet1", output_df, resize_cols, apply_formats_1)
    return send_file_response(output, f"invendu_{groupcode}_{toDate.strftime(DATE_FMT)}.xlsx")
  else:
    return jsonify(message="group code is missing"), 400
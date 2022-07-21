# coding: utf-8
from flask import Blueprint, request, jsonify
from flask import send_from_directory

from app.utils.flask_helpers import build_response, send_file_response
from app.utils.xlsxwriter_utils import to_size_col, build_formats_for
from app.utils.mongo_utils import queryUpdateCache
from app.utils.metrics_helpers import add_revenues_metrics, pivot_on_items, pivot_on_categories,rewind_x_months
from app.utils.query_utils import compute_months_dict_betweenDates
from app.dao import dao
from app.cache_dao import cache_dao
from app.cache import fetch_master_itemlist
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name
import datetime as dt
import pandas as pd
import os, json, functools

from io import BytesIO

def no_fmt_fn(*args, **kwargs):
  pass

bp = Blueprint("all", __name__)

@bp.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(bp.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@bp.route("/")
def index():
  return 'Hey, we have Flask in a Docker container with Blueprint!'

@bp.route('/stats')
def computeStats():
  supplier = request.args.get("cardcode")
  periodInWeeks = request.args.get("weeks")
  if supplier and periodInWeeks:
    result = None
    return build_response(dao.dfToJson(result))
  return jsonify(message="parameters cardcode and weeks are missing ..."), 400

@bp.route("/promos/<string:itemcode>")
def getPromoForCode(itemcode):
  response = []
  dfItem = dao.execute_query("""select t0.itemcode,t0.itemname, t7.price, t8.rate from dbo.oitm t0 
                            join dbo.itm1 t7 on t7.itemcode=t0.itemcode and t7.pricelist='1'
                            join dbo.ovtg t8 on t8.code=t0.vatgourpsa
                            where t0.itemcode='{}'""".format(itemcode))
  tva = 0.0
  priceHt = 0.0
  itemname=""
  if len(dfItem)>0:
    priceHt=dfItem.price.values[0]
    tva = dfItem.rate.values[0]
    itemname = dfItem.itemname.values[0]
  withTva = (1+float(tva/100.0))
  query="""select * from dbo.spp1 t0 where t0.itemcode='{}' and t0.listnum='1'""".format(itemcode)
  df=dao.execute_query(query)
  today = dt.datetime.now()
  todayString = today.strftime("%Y-%m-%d")
  qry_result = df.query("CardCode=='*1' and FromDate<='{today}' and ToDate>='{today}'".format_map({"today":todayString}))
  discountPercent = 0.0
  if len(qry_result)>0:
    discountPercent = float(qry_result.Discount.values[0])
    
  withDiscount = (1-float(discountPercent)/100.0)
  discounted = float(priceHt) * withDiscount
  discountedTTC = discounted * withTva
  
  response.append({"itemcode":itemcode, "itemname": itemname,"discountedHT":discounted,"discountedTTC":"{:.2f}".format(discountedTTC),"discount":discountPercent})
  return build_response(json.dumps(response))

@bp.route('/clients')
def search_business_partners():
  search_param = request.args.get("search")
  if search_param:
    suppliers = dao.getBusinessPartners(search_param.upper())
    return build_response(dao.dfToJson(suppliers))
  return jsonify(message="search parameter is missing..."), 400

def apply_formats_1(sheetname, worksheet, formats, sizes, rows, columns, dataframe=None):
  _values=["entree", "sortie", "retour", "entr_march"]
  _fmts = ["good","bad","warning","neutral"]
  for v,f in zip(_values,_fmts):
      worksheet.conditional_format("B2:B{}".format(rows+1), 
                              {"type":'cell', "criteria":'=', "value":'"{}"'.format(v), "format":formats[f]})
  correction_condition="AND(${sheetname}.$I2>0, ${sheetname}.$D2>${sheetname}.$F2)".format(sheetname=sheetname)
  worksheet.conditional_format("C2:C{}".format(rows+1),
          {"type":'formula', "criteria": correction_condition, "format":formats["neutral"]})
  for (col, size, fmt) in sizes:
      worksheet.set_column(col, size, formats[fmt])

def apply_formats_2(sheetname, worksheet, formats, sizes, rows, columns, dataframe=None):
  correction_condition="AND(${sheetname}.$D2=0)".format(sheetname=sheetname)
  worksheet.conditional_format("C2:C{}".format(rows+1),
          {"type":'formula', "criteria": correction_condition, "format":formats["neutral"]})
  for (col, size, fmt) in sizes:
      worksheet.set_column(col, size, formats[fmt])

def output_excel(writer, sheetname, dataframe, sizes, apply_formats_fn):
  r, c = dataframe.shape # number of rows and columns
  dataframe.to_excel(writer, sheetname)
  # Get the xlsxwriter workbook and worksheet objects.
  workbook  = writer.book
  formats = build_formats_for(workbook)
  worksheet = writer.sheets[sheetname]
  worksheet.freeze_panes(1,3)
  worksheet.autofilter(0,0,r,c)
  apply_formats_fn(sheetname, worksheet, formats, sizes, r, c, dataframe)

@bp.route('/mouvements-forces', methods=["GET"])
def stock_movements():
  date_from = request.args.get("date-from")
  if date_from :
    dateFrom=dt.datetime.strptime(date_from, "%Y-%m-%d")
  else:
    now = dt.datetime.now()
    monday=dt.datetime.strptime("{}-W{}".format(now.year,now.isocalendar()[1])+'-1',"%Y-W%W-%w")
    dateFrom = monday.strftime("%Y-%m-%d")

  siDf, rawSiDf, cashierSiDf = dao.getEntreeSortiesMarchandise(dateFrom)
  output = BytesIO()
  # Create a Pandas Excel writer using XlsxWriter as the engine.
  with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    output_excel(writer, "Sheet1", siDf, [["C:C", to_size_col(5), "no_format"]], apply_formats_1)
    output_excel(writer, "Sheet2", rawSiDf, [["E:E",63, "no_format"] ,["F:F",to_size_col(1.55), "date1"]],apply_formats_1)
    output_excel(writer, "Sheet3", cashierSiDf, [["C:C",to_size_col(3.93), "no_format"]],apply_formats_2)
  return send_file_response(output, f"si_{dateFrom}.xlsx")

@bp.route('/historique/<string:cardcode>', methods=["POST"])
def historique_client(cardcode):
  # Local application of formats for the worksheet
  def apply_fmts(sheetname, worksheet, formats, sizes, rows, columns, dataframe):
    today = dt.datetime.now()
    onsaleItems = dao.getItemsOnSale(today)
    promos = onsaleItems["itemcode"].tolist()
    df_columns=dataframe.columns
    if "last4w" in df_columns.tolist():
      last4w_col=xl_rowcol_to_cell(1, df_columns.get_loc("last4w")+1)
      correction_condition="${sheetname}.${col_letter}<2".format(sheetname=sheetname, col_letter=last4w_col)
      worksheet.conditional_format("C2:C{}".format(rows+1),
              {"type":'formula', "criteria": correction_condition, "format":formats["neutral"]})
    for (col, size, fmt) in sizes:
        worksheet.set_column(col, size, formats[fmt])
    itemcode_col = df_columns.get_loc("itemcode")
    for idx, row in enumerate(dataframe.itertuples()):
      if row.itemcode in promos:
        worksheet.write(idx+1, itemcode_col+1, dataframe.iloc[idx, itemcode_col], formats["good"])

  # End of function definition
  
  if request.is_json:
    posted_data = request.get_json()
    #cardname = posted_data["cardname"]
    output = BytesIO()
    periodInWeeks = 10
    siDf = cache_dao.getItemsBoughtByClient(cardcode, periodInWeeks)
    siDf = siDf.sort_values(["categorie", "dscription"], ascending=[1,1])
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
      output_excel(writer, "Sheet1", siDf, [["C:C", to_size_col(5), "no_format"]], apply_fmts)
    return send_file_response(output, f"historique_{cardcode}.xlsx")
  else:
    return jsonify(message="The request does not contain json data"), 400

@bp.route('/historique/<string:cardcode>/stats', methods=["POST"])
def chiffre_affaire_client(cardcode):
  def add_fields(dataframe):
    dataframe["revient"] = [row.quantity*row.grossbuypr for row in dataframe.itertuples()]
    dataframe["month"] = [row.docdate.to_period('M') for row in dataframe.itertuples()]
    return dataframe.copy()

  def add_sum_cols(dataframe, rowstart, colstart, worksheet):
    r,c = dataframe.shape
    sum_headers_cols=list(map(lambda col: xl_col_to_name(col), range(colstart, c)))
    row_start=rowstart
    row_end=row_start+r
    for current_col in sum_headers_cols:
        worksheet.write_formula(f"{current_col}{row_start-1}", f"=sum({current_col}{row_start}:{current_col}{row_end})")

  def set_output_excel(freeze_loc, autofilter_loc,start_row):
    def _output_excel(writer, sheetname, dataframe, sizes, apply_formats_fn):
      r, c = dataframe.shape # number of rows and columns
      dataframe.to_excel(writer, sheetname, index=False, startrow=start_row)
      # Get the xlsxwriter workbook and worksheet objects.
      workbook  = writer.book
      formats = build_formats_for(workbook)
      worksheet = writer.sheets[sheetname]
      worksheet.freeze_panes(*freeze_loc)
      worksheet.autofilter(*autofilter_loc,r,c-1)
      apply_formats_fn(sheetname, worksheet, formats, sizes, r, c, dataframe)
      return worksheet
    return _output_excel

  def extract_column_labels(dataframe):
    def _extract(pattern):
      columns = dataframe.columns.values.tolist()
      return list(filter(lambda label: pattern in label, columns))
    return _extract

  # END OF FUNCTION DEFINITIONS
  months_number = 6
  six_months_ago_from = rewind_x_months(months_number)
  today=dt.datetime.today()
  date_from = six_months_ago_from(today)
  qry = {"cardcode":cardcode, "docdate":{"$gte":date_from}}
  customer_raw_data = cache_dao.find_query(qry)
  #caracters_to_replace = [" ", "/", "'"]
  #cardname = functools.reduce(lambda x,y: x.replace(y, "_"), caracters_to_replace, customer_raw_data.iloc[0,:].cardname)
  # Get data from mongo
  masterdata = fetch_master_itemlist()
  customer_raw_data = add_fields(customer_raw_data).merge(
    masterdata.loc[:,["itemcode", "itemname", "categorie"]], on="itemcode", how="inner")
  #
  # PIVOT BY CATEGORIES AND ITEMS
  #
  by_items_df = pivot_on_items(customer_raw_data)
  by_cat_df   = pivot_on_categories(customer_raw_data)
  months = compute_months_dict_betweenDates(date_from, today)
  r = map(lambda y: map(lambda m: f"{str(y)}-{str(m).zfill(2)}",months[y]), months.keys())
  months = list(functools.reduce(lambda x,y:list(x)+list(y), r))
  
  extract_label = extract_column_labels(by_cat_df)
  linetotals = extract_label("linetotal/")
  revients   = extract_label("revient/")
  all_months = list(map(lambda x:f"linetotal/{x}", months))
  missing_months = set(all_months)-set(linetotals)

  for m in missing_months:
      by_cat_df[m]=0
      by_items_df[m]=0

  rrr=list(zip(linetotals, map(lambda label: label.replace("linetotal", "caht"), extract_column_labels(by_cat_df)("linetotal/"))))
  add_revenues_metrics(by_items_df)
  by_cat = add_revenues_metrics(by_cat_df.fillna(0.0))
  total_ca = by_cat.caht.sum()
  by_cat["ratio"] = [r.caht/total_ca for r in by_cat.itertuples()]
  displayed_order = ['itemcode','itemname','categorie','freq', 'caht', 'revient', 'marg_val'] + sorted(all_months, reverse=True)

  display_order   = ['categorie','ratio', 'caht', 'revient', 'marg_val'] + sorted(all_months, reverse=True)
  
  by_items_df_out = by_items_df.sort_values(by=["marg_val","freq","categorie", "itemname"], ascending=[0,0,1,1]
                    ).loc[:,displayed_order].rename(columns={k:v for k,v in rrr})
  by_cat_df_out = by_cat.loc[:,display_order].rename(columns={k:v for k,v in rrr})

  output_categories = set_output_excel((2,5), (1,0), 1)
  output_xls_items  = set_output_excel((2,4), (1,0), 1)
  output = BytesIO()
  with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    cat_sizes = [["A:A", to_size_col(2), "no_format"], ["B:B", to_size_col(1), "percents"],
                ["C:E", to_size_col(1.3), "currency"], ["F:L", to_size_col(1.5), "no_format"]]
    ws_cat = output_categories(writer, "CATEGORIES", by_cat_df_out, cat_sizes, apply_formats_1)
    add_sum_cols(by_cat_df_out, 2, 2, ws_cat)
    items_sizes=[["B:B", to_size_col(6), "no_format"], ["C:C", to_size_col(2), "no_format"], 
                ["E:G", to_size_col(1.3), "currency"], ["H:N", to_size_col(1.5), "no_format"]]
    ws_items = output_xls_items(writer, "ITEMS", by_items_df_out, items_sizes, apply_formats_1)
    add_sum_cols(by_items_df_out, 2, 4, ws_items)

  return send_file_response(output, f"ca_{cardcode}.xlsx")



@bp.route('/cache/last_updated', methods=["GET"])
def get_last_updated():
  last_updated = cache_dao.last_record()
  return build_response(json.dumps(last_updated))

@bp.route('/cache/update', methods=["POST"])
def update_cache_data():
  last_docnum = cache_dao.last_record()["docnum"]
  updated_data = queryUpdateCache.get_data_from_sap_as_df(last_docnum)
  if updated_data.empty:
    return jsonify(message="no new data"), 204
  else:
    cache_dao.importFromDataframe(updated_data)
    return build_response(dao.dfToJson(updated_data))

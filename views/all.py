# coding: utf-8
from flask import Blueprint, request, jsonify
from flask import send_from_directory

from app.flask_helpers import build_response, send_file_response
from app.xlsxwriter_utils import to_size_col, build_formats_for
from app.dao import dao
from xlsxwriter.utility import xl_rowcol_to_cell
import datetime as dt
import pandas as pd
import os, json

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
  now = dt.datetime.now()
  monday=dt.datetime.strptime("{}-W{}".format(now.year,now.isocalendar()[1])+'-1',"%Y-W%W-%w")
  output = BytesIO()
  dateFrom = monday.strftime("%Y-%m-%d")
  siDf, rawSiDf, cashierSiDf = dao.getEntreeSortiesMarchandise(dateFrom)
  # Create a Pandas Excel writer using XlsxWriter as the engine.
  with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
    output_excel(writer, "Sheet1", siDf, [["C:C", to_size_col(5), "no_format"]], apply_formats_1)
    output_excel(writer, "Sheet2", rawSiDf, [["E:E",63, "no_format"] ,["F:F",to_size_col(1.55), "date1"]],apply_formats_1)
    output_excel(writer, "Sheet3", cashierSiDf, [["C:C",63, "no_format"]],no_fmt_fn)
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
    siDf = dao.getItemsBoughtByClient(cardcode, periodInWeeks)
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
      output_excel(writer, "Sheet1", siDf, [["C:C", to_size_col(5), "no_format"]], apply_fmts)
    return send_file_response(output, f"historique_{cardcode}.xlsx")
  else:
    return jsonify(message="The request does not contain json data"), 400

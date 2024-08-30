from flask import Blueprint
from flask import jsonify, request

from app.dao import dao
from app.cadencier import format_to_excel

from app.utils.flask_helpers import build_response, send_file_response
from app.utils.xlsxwriter_utils import build_formats_for
from app.utils.metrics_helpers import substract_days_from_today
from app.cache import compute_receptions_from_date
from app.cache_dao import cache_dao

from io import BytesIO
from flask import Flask
import xlsxwriter
import datetime as dt
import pandas as pd
import numpy as np
import itertools

suppliers = Blueprint("suppliers", __name__)

@suppliers.route('/suppliers')
def search_suppliers():
  search_param = request.args.get("search")
  if search_param:
    suppliers = dao.getBusinessPartners(search_param.upper())
    suppliersResult = suppliers[suppliers["cardcode"].str.startswith("F")|suppliers["cardcode"].str.startswith("f")]
    return build_response(dao.dfToJson(suppliersResult))
  return jsonify(message="search parameter is missing..."), 400

@suppliers.route('/suppliers/<string:cardcode>')
def get_supplier_info(cardcode):
  info = dao.get_supplier_info(cardcode)
  return build_response(info)

@suppliers.route('/suppliers/onorder/status')
def search_order_status():
  supplier = request.args.get("search")
  if supplier:
    orders = dao.getOnOrders()
    ordersResult = orders[orders["cardname"].str.contains(supplier.upper())]
    return build_response(dao.dfToJson(ordersResult))
  return jsonify(message="search parameter is missing"), 400

@suppliers.route('/suppliers/orders/<string:docnum>')
def getOrder(docnum):
  lines=dao.getDocLines(docnum)
  return build_response(dao.dfToJson(lines))

@suppliers.route('/suppliers/<string:cardcode>/good-receipts', methods=["POST"])
def get_good_receipts_po(cardcode):
  pass

@suppliers.route('/suppliers/<string:cardcode>/sales/weekly', methods=["POST"])
def computeWeeklySales(cardcode):
  def inDiscountPredicate(monday, fromdate, todate):
    deltaDays=(fromdate-monday).days
    isSameWeek = deltaDays>0 and deltaDays<7
    isInDiscountPeriod = fromdate<=monday and monday<=todate
    return isSameWeek or isInDiscountPeriod
  def build_label(values, columns):
    return ["('sum', '{}', '{}')".format(x[0], x[1]) for x in itertools.product(values, columns)]

  now = dt.datetime.now().strftime("%Y-%m-%d")
  output = BytesIO()
  writer = pd.ExcelWriter(output, engine='xlsxwriter')
  # Call to cadencier
  periodInWeeks = 5
  nb_days_in_one_week = 7
  #salesDataDf = dao.getSales(cardcode, periodInWeeks)
  salesDataDf, weeks = cache_dao.getWeeklySales(cardcode, periodInWeeks)
  salesDataDf = salesDataDf.query("sellitem=='Y'").sort_values(by=["itemname"])
  salesDataDf.drop(["sellitem"], axis=1, inplace=True)
  #receipts_po = dao.getGoodReceiptsPo(cardcode, periodInWeeks)
  dateFrom = substract_days_from_today(periodInWeeks*nb_days_in_one_week)
  receipts_po = compute_receptions_from_date(dateFrom).query(f"cardcode=='{cardcode}'")
  r = c =0
  if len(receipts_po)>0:
    index_fields=["itemcode", "dscription"]
    values_fields=["quantity"]
    columns_fields=["c"]
    pivot_sales = pd.pivot_table(receipts_po, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
    outputDf=pd.DataFrame(pivot_sales.to_records())
    column_labels = receipts_po["c"].unique().tolist()
    column_labels.sort(reverse=True)
    labels_count=len(column_labels)
    ind_fields_count=len(index_fields)
    shortened_col_labels = column_labels #list(map(lambda x:x[5:],column_labels))
    columns_renamed = {k:v for k,v in zip(build_label(values_fields, column_labels), shortened_col_labels)}
    outputDf.rename(columns=columns_renamed, inplace=True)
    outputDf = outputDf.loc[:,index_fields+shortened_col_labels]
    r, c=salesDataDf.shape
  # Convert the dataframe to an XlsxWriter Excel object.
  salesDataDf.to_excel(writer, sheet_name='Sheet1')
  #format_to_excel(workbook, salesDataDf, {"date":now})
  workbook =  writer.book
  formats = build_formats_for(workbook)
  worksheet = writer.sheets["Sheet1"]
  itemname_width=63
  date_width = 14
  worksheet.set_column("B:B", itemname_width, None)
  worksheet.set_column("E:P", date_width, None)
  worksheet.freeze_panes(1,2)
  worksheet.autofilter(0,0,r,c)

  discounted_items = dao.getDiscountedItemsFromDate(weeks[-1])
  df_columns=salesDataDf.columns
  masks={}
  for w in weeks:
    monday=dt.datetime.strptime(w, "%Y-%m-%d").date()
    mask = discounted_items.apply(lambda row: inDiscountPredicate(monday, row.fromdate.date(), row.todate.date()), axis=1)
    masks[w]=mask
  # Apply formats
  if len(receipts_po)>0:
    for idx, row in enumerate(salesDataDf.itertuples()):
      itemcode = salesDataDf.index[idx]
      receipt_item = outputDf.query(f"itemcode=='{itemcode}'")
      receipt_dates = outputDf.columns
      dates = receipt_dates[2:] #receipt_date.values.tolist()
      for w in weeks:
        week_col = df_columns.get_loc(w)
        if not discounted_items.loc[masks[w]].query(f"itemcode=='{itemcode}'").empty:
          worksheet.write(idx+1, week_col+1, salesDataDf.iloc[idx, week_col], formats["good"])
        if w in dates:
          if not receipt_item.empty:
            receipt_quantity = receipt_item.iloc[0][w]
            if receipt_quantity>0 :
              worksheet.write_comment(idx+1, week_col+1, f"recu : {receipt_quantity}")

  # Close the workbook before streaming the data.
  writer.close()
  #workbook.close()
  return send_file_response(output, f"{cardcode}_{now}.xlsx")

@suppliers.route('/suppliers/import/sales/<string:cardcode>', methods=["POST"])
def computeImportSales(cardcode):
  now = dt.datetime.now().strftime("%Y-%m-%d")
  output = BytesIO()
  periodInWeeks = 16
  salesDataDf = cache_dao.getImportSales(cardcode, periodInWeeks)
  #column_name = " ".join(["quantity", receipts_po.loc[0,"c"]])
  r, c = salesDataDf.shape # number of rows and columns
  # Create a Pandas Excel writer using XlsxWriter as the engine.
  writer = pd.ExcelWriter(output, engine='xlsxwriter')
  # Convert the dataframe to an XlsxWriter Excel object.
  salesDataDf.to_excel(writer, sheet_name='Sheet1')

  # Get the xlsxwriter workbook and worksheet objects.
  workbook  = writer.book
  worksheet = writer.sheets['Sheet1']
  formats = build_formats_for(workbook)
  set_sizes_worksheet(worksheet, formats)
  worksheet.write_comment('B1', 'Mauve: onhand < total_quantity \n Jaune: onhand > 0 et pas de vente \n Vert: Reception marchandise')
  worksheet.freeze_panes(1,2)
  worksheet.autofilter(0,0,r,c)

  neutral_format=formats["neutral"]
  warning_format=formats["warning"]
  worksheet.conditional_format("B2:B{}".format(r), {"type":'formula', "criteria":'AND($Sheet1.$L2=0, $Sheet1.$J2>0)', "format":neutral_format})
  #worksheet.conditional_format("B2:B{}".format(r), {"type":'formula', "criteria":'$Sheet1.$J2=0', "format": bad_format})
  worksheet.conditional_format("B2:B{}".format(r), {"type":'formula', "criteria":'$Sheet1.$J2<$Sheet1.$L2', "format":warning_format})

  receipts_po = dao.getGoodReceiptsPo(cardcode, periodInWeeks)
  if len(receipts_po)>0:
    # Apply formats 
    for idx, row in enumerate(salesDataDf.itertuples()):
      itemcode = salesDataDf.index[idx]
      receipt_item = receipts_po.query(f"itemcode=='{itemcode}'")
      receipt_date = receipt_item.c
      if not receipt_date.empty:
        dates = receipt_date.values.tolist()
        headers = map(lambda x: " ".join(["quantity", x]), dates)
        col_numbers = map(lambda x: salesDataDf.columns.get_loc(x), headers)
        for date_idx, col in enumerate(col_numbers):
          receipt_quantity=receipt_item.query(f"c=='{dates[date_idx]}'").quantity.sum()
          worksheet.write_comment(idx+1, col+1, f"recu : {receipt_quantity}")
          #worksheet.write(idx+1, col+1, salesDataDf.iloc[idx, col], formats["good"])
          #apply_format(worksheet, idx+1, col+1, formats["good"])
  # Close the Pandas Excel writer and output the Excel file.
  writer.close()
  # Close the workbook before streaming the data.
  #workbook.close()
  return send_file_response(output, f"{cardcode}_{now}.xlsx")


def set_sizes_worksheet(worksheet, formats):
  itemname_width=63
  categorie_width=19
  total_width=15
  margin_width=12
  quantity_width=7
  ca_ht_width=18

  worksheet.set_column('B:B', itemname_width, None)
  worksheet.set_column('D:D', categorie_width, None)
  worksheet.set_column('H:I', margin_width, formats["percents"])
  worksheet.set_column('J:J', margin_width, formats["date1"])
  worksheet.set_column('K:K', margin_width, None)
  worksheet.set_column('L:N', margin_width, None)
  worksheet.set_column('O:O', total_width, formats["currency"])
  worksheet.set_column('N:AF', quantity_width,None)
  worksheet.set_column('AG:AW', ca_ht_width,formats["currency"])
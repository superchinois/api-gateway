from flask import Blueprint
from flask import jsonify, request
from app.flask_helpers import build_response, send_file_response
from app.dao import dao
from app.cadencier import format_to_excel
from app.xlsxwriter_utils import build_formats_for

from io import BytesIO
from flask import Flask
import xlsxwriter
import datetime as dt
import pandas as pd

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

  now = dt.datetime.now().strftime("%Y-%m-%d")
  output = BytesIO()
  writer = pd.ExcelWriter(output, engine='xlsxwriter')
  # Call to cadencier
  periodInWeeks = 5
  #salesDataDf = dao.getSales(cardcode, periodInWeeks)
  salesDataDf, weeks = dao.getWeeklySales(cardcode, periodInWeeks)
  salesDataDf = salesDataDf.query("sellitem=='Y'").sort_values(by=["itemname"])
  salesDataDf.drop(["sellitem"], axis=1, inplace=True)
  receipts_po = dao.getGoodReceiptsPo(cardcode, periodInWeeks)
  r, c=salesDataDf.shape
  # Convert the dataframe to an XlsxWriter Excel object.
  salesDataDf.to_excel(writer, sheet_name='Sheet1')
  #format_to_excel(workbook, salesDataDf, {"date":now})
  workbook =  writer.book
  formats = build_formats_for(workbook)
  worksheet = writer.sheets["Sheet1"]
  itemname_width=63
  date_width = 11
  worksheet.set_column("B:B", itemname_width, None)
  worksheet.set_column("F:P", date_width, None)
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
  for idx, row in enumerate(salesDataDf.itertuples()):
    itemcode = salesDataDf.index[idx]
    receipt_item = receipts_po.query(f"itemcode=='{itemcode}'")
    receipt_date = receipt_item.c
    for w in weeks:
      week_col = df_columns.get_loc(w)
      if not discounted_items.loc[masks[w]].query(f"itemcode=='{itemcode}'").empty:
        worksheet.write(idx+1, week_col+1, salesDataDf.iloc[idx, week_col], formats["good"])
    if not receipt_date.empty:
      dates = receipt_date.values.tolist()
      col_numbers = map(lambda x: salesDataDf.columns.get_loc(x), dates)
      for date_idx, col in enumerate(col_numbers):
        receipt_quantity=receipt_item.query(f"c=='{dates[date_idx]}'").quantity.sum()
        worksheet.write_comment(idx+1, col+1, f"recu : {receipt_quantity}")
  # Close the workbook before streaming the data.
  writer.save()
  workbook.close()
  return send_file_response(output, f"{cardcode}_{now}.xlsx")

@suppliers.route('/suppliers/import/sales/<string:cardcode>', methods=["POST"])
def computeImportSales(cardcode):
  now = dt.datetime.now().strftime("%Y-%m-%d")
  output = BytesIO()
  periodInWeeks = 16
  salesDataDf = dao.getImportSales(cardcode, periodInWeeks)
  receipts_po = dao.getGoodReceiptsPo(cardcode, periodInWeeks)
  column_name = " ".join(["quantity", receipts_po.loc[0,"c"]])
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
  writer.save()
  # Close the workbook before streaming the data.
  workbook.close()
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
  worksheet.set_column('L:L', total_width, None)
  worksheet.set_column('M:M', total_width, formats["currency"])
  worksheet.set_column('N:AD', quantity_width,None)
  worksheet.set_column('AE:AU', ca_ht_width,formats["currency"])
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell,xl_cell_to_rowcol
import pandas as pd
import numpy as np
from app.xlsxwriter_utils import write_labels, Label, mergectx, write_headers
import math, re
import itertools
import datetime as dt

def to_size_col(xlsize):
  return int(math.ceil(100*xlsize/9.79))

def to_size_row(xlsize):
  return int(math.ceil(100*xlsize/1.39))

HEADERS=[]
# 1 point is 1/72 inch
HEAD_HEIGHT =to_size_row(0.61)
COL_WIDTH = 13
ROW_HEIGHT = 50
FIRST_COL_WIDTH = to_size_col(1.26)

  
def build_formats(workbook, worksheet, pivot_table):
  base_options={'font_size':'14','text_wrap': True,'border':1,'valign': 'vcenter'}
  bg_color_options={'bg_color':'#dddddd'}
  base_data_options = mergectx(base_options,{'align': 'center'})
  colored_data_options = mergectx(base_data_options, bg_color_options)
  # Create a format to use in the merged range.
  #  #'fg_color': '#dddddd', #light gray
  
  data_fmt= workbook.add_format(mergectx(base_data_options, {'font_size':'18'}) )
  head_format = workbook.add_format(mergectx(base_data_options,{'rotation': 0}))
  base_fmt = workbook.add_format(base_options)
  annexe_data = workbook.add_format(mergectx(base_data_options, {'border':0, 'font_size':'18'}))
  formats={}
  formats["base_fmt"] = base_fmt
  formats["head_fmt"] = head_format
  formats["data_fmt"] =data_fmt
  formats["annexe_fmt"] = annexe_data
  return formats

def compute_header_layout(row, column_start, data_vec, formats):
  result=[]
  head_fmt = formats['head_fmt']
  for idx, header in enumerate(data_vec):
    if header is not 'All':
      #head_label=header.strftime("%Y-%m-%d")
      head_label=header
      result.append(Label(head_label, [row, column_start+idx],xls_format=head_fmt))
  return result

def write_row(worksheet,columns_data,row_start_idx,row, value_fmt,formats):
  for idx,label in enumerate(columns_data):
    last_col = label.get_col_index()
    labelname = label.get_label()
    value = row[idx]
    if value > 0:
      worksheet.write(row_start_idx, last_col,value,value_fmt)
    else :
      worksheet.write(row_start_idx, last_col,"",value_fmt)

def write_pv_to_rows(worksheet, row_start, pv, context, formats):
  data_fmt = formats["data_fmt"]
  #columns_data = context['header_labels']
  #col_start = columns_data[0].get_col_index()
  row_index=row_start
  for row in pv.itertuples():
    worksheet.set_row(row_index,ROW_HEIGHT)
    for idx, value in enumerate(row[1:]):
      applied_fmt=data_fmt
      if idx<3:
        applied_fmt=formats["base_fmt"]
      worksheet.write(row_index,idx, value,applied_fmt)
    row_index=row_index+1


def write_data_to_sheet(workbook, salesDataDf, context):
  worksheet = workbook.add_worksheet("cadencier")
  pivot_table = salesDataDf
  formats = build_formats(workbook, worksheet, pivot_table)
  headers = pivot_table.columns.values.tolist()[3:]
  nb_row_pivot = len(pivot_table)
  row_start = 0
  label_col_start = 3
  header_labels = compute_header_layout(row_start,label_col_start,headers, formats)
  context["header_labels"]=header_labels

  for idx,h in enumerate(header_labels):
    pos=label_col_start+idx
    worksheet.set_column(pos,pos,COL_WIDTH)
  worksheet.set_column(0, 0, FIRST_COL_WIDTH)
  worksheet.set_column(1, 1, to_size_col(5.71))
  worksheet.set_column(2, 2, to_size_col(1.30))
  worksheet.write(0, 0, "itemcode")
  worksheet.write(0, 1, "itemname")
  worksheet.write(0, 2, "stock au {}".format(context["date"]), formats["base_fmt"])
  write_headers(worksheet, row_start, header_labels, HEAD_HEIGHT)
  row_data_start = 1
  write_pv_to_rows(worksheet,row_data_start,pivot_table,context,formats)

  nb_total_rows = row_start + row_data_start + nb_row_pivot
  nb_total_col = 1 + label_col_start +len(header_labels)

  worksheet.print_area(0,0,nb_total_rows,nb_total_col)
  a4_format = 9
  worksheet.set_paper(a4_format)
  worksheet.set_portrait()
  worksheet.fit_to_pages(1,1)

def assign_date(row):
  monday=dt.datetime.strptime("{}-W{}".format(row.year,row.week)+'-1',"%Y-W%W-%w")
  previous_monday=monday - dt.timedelta(days=7)
  result_monday=monday
  if row.docdate<monday:
    result_monday = previous_monday
  return result_monday.strftime("%Y-%m-%d")
#
# data : array of array
# filter_fn : function to filter the dataframe
#
def pivot_data(dataframe, filter_fn=lambda x: x):
  filtered_df = filter_fn(dataframe)
  filtered_df["c"] = filtered_df.apply(lambda row: assign_date(row), axis=1)
  pvtable = pd.pivot_table(filtered_df, index=["itemcode","itemname","onhand"],
     values=['quantity'],
     columns=['c'],
     aggfunc=[np.sum],
     fill_value=0)
  pvtable.sort_index(axis=0, level=1,inplace=True)
  return pvtable

def format_to_excel(workbook, salesDataDf, context):
  now = dt.datetime.now().strftime("%Y-%m-%d")
  pv = pivot_data(salesDataDf)
  flatpv = pd.DataFrame(pv.to_records())
  cols=flatpv.columns.values.tolist()
  result = list(filter(lambda x: "sum" in x,cols))
  result.sort(reverse=True)
  finalDf = flatpv.loc[:,cols[slice(0,3)]+result]
  datePat = r'\d{4}-\d{2}-\d{2}'
  a=map(lambda x:re.findall(datePat, x), result)
  dates = list(itertools.chain(*a))
  renamed_cols = {k:v for k,v in zip(result, dates)}
  finalDf.rename(columns=renamed_cols,inplace=True)
  finalDf.sort_values(by=["itemname"], inplace=True)
  write_data_to_sheet(workbook, finalDf, context)
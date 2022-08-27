#!/usr/bin/env python
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell, xl_col_to_name
import csv
import math

class Label:
  def __init__(self, name, position, is_data=True, xls_format=None):
    self.name = name
    self.position = position
    self.is_data = is_data
    self.xls_format = xls_format

  def set_xls_format(self, format):
    self.xls_format = format

  def to_be_merged(self):
    if len(self.position)>2:
      return True
    else:
      return False

  def is_col_writable(self):
    if len(self.position)==2:
      return True
    else:
      return False

  def get_a1_position(self):
    first = xl_rowcol_to_cell(self.position[0], self.position[1])
    if self.to_be_merged():
      second = xl_rowcol_to_cell(self.position[2], self.position[3])
      return ":".join([first, second])
    else:
      return first

  def get_col_index(self):
    return self.position[1]

  def get_label(self):
    return self.name

  def get_format(self):
    return self.xls_format

  def is_data(self):
    return is_data

  def __str__(self):
    return "{}:{}".format(self.name, self.get_col_index())

#
# Utils functions
#
def read_csv(path, delimiter=';'):
  result = []
  with open(path, 'r') as data:
      reader = csv.reader(data, delimiter=delimiter)
      for row in reader:
          result.append(row)
  return result

def mergectx(default, other_options):
  context = default.copy()
  context.update(other_options)
  return context

def write_headers(worksheet, row_start, labels, row_height):
  worksheet.set_row(row_start,row_height)
  write_labels(worksheet, labels)

def write_labels(worksheet, labels):
  for head in labels:
    p = head.position
    if head.is_data:
      worksheet.write(p[0], p[1], head.get_label(),head.get_format())
    else:
      worksheet.merge_range(head.get_a1_position(),head.get_label(), head.get_format())

def to_size_col(xlsize):
  return int(math.ceil(100*xlsize/9.79))

def to_size_row(xlsize):
  return int(math.ceil(100*xlsize/1.39))

def apply_format(worksheet, row, col, cell_format):
  worksheet.conditional_format(row,col,row, col, {'type': 'no_errors','format': cell_format})

def build_formats_for(workbook):
  # Add some cell formats.
  format1 = workbook.add_format({'num_format': '#,##0.00'})
  format2 = workbook.add_format({'num_format': '0.0%;[RED] -0.0%', 'align':'center'})
  format3 = workbook.add_format({'num_format': '#,##0.00 [$€-40C];[RED]-#,##0.00 [$€-40C]', 'align':'center'})
  error_fmt = workbook.add_format({'bg_color': '#cc0000','font_color': '#ffffff'})
  neutral_format=workbook.add_format({'bg_color': '#ffffcc','font_color': '#996600'})
  warning_format=workbook.add_format({'bg_color':'#f2cbf8','font_color': '#7c007c'})
  bad_format=workbook.add_format({'bg_color': '#ffcccc','font_color': '#cc0000'})
  good_format=workbook.add_format({'bg_color': '#ccffcc','font_color': '#006600'})
  date_format1 = workbook.add_format({'num_format':'dd-mmm'})

  fmt_array=[None, format1, format2, format3, neutral_format, warning_format, bad_format, good_format, error_fmt,date_format1]
  fmt_names=["no_format","number", "percents", "currency", "neutral", "warning", "bad", "good", "error", "date1"]
  formats={k:v for k,v in zip(fmt_names, fmt_array)}
  return formats

def set_output_excel(freeze_loc, autofilter_loc,start_row, to_excel_index=False):
  def _output_excel(writer, sheetname, dataframe, sizes, apply_formats_fn):
    r, c = dataframe.shape # number of rows and columns
    dataframe.to_excel(writer, sheetname, index=to_excel_index, startrow=start_row)
    # Get the xlsxwriter workbook and worksheet objects.
    workbook  = writer.book
    formats = build_formats_for(workbook)
    worksheet = writer.sheets[sheetname]
    worksheet.freeze_panes(*freeze_loc)
    worksheet.autofilter(*autofilter_loc,r,c)
    apply_formats_fn(sheetname, worksheet, formats, sizes, r, c, dataframe)
    return worksheet
  return _output_excel

def add_count_non_zero(worksheet, dataframe, rowstart, colstart, col_mask=0):
  r,c = dataframe.shape
  sum_headers_cols=list(map(lambda col: xl_col_to_name(col), range(colstart, c-col_mask)))
  row_start=rowstart
  row_end=row_start+r
  for current_col in sum_headers_cols:
      worksheet.write_formula(f"{current_col}{row_start-1}", 
        f"=countif({current_col}{row_start}:{current_col}{row_end}, \">0\")")
#!/usr/bin/env python
import xlsxwriter
from xlsxwriter.utility import xl_rowcol_to_cell
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
  format2 = workbook.add_format({'num_format': '0%'})
  format3 = workbook.add_format({'num_format': '#,##0.00 [$€-40C];[RED]-#,##0.00 [$€-40C]'})
  neutral_format=workbook.add_format({'bg_color': '#ffffcc','font_color': '#996600'})
  warning_format=workbook.add_format({'bg_color':'#f2cbf8','font_color': '#7c007c'})
  bad_format=workbook.add_format({'bg_color': '#ffcccc','font_color': '#cc0000'})
  good_format=workbook.add_format({'bg_color': '#ccffcc','font_color': '#006600'})
  date_format1 = workbook.add_format({'num_format':'dd-mmm'})
  
  fmt_array=[None, format1, format2, format3, neutral_format, warning_format, bad_format, good_format, date_format1]
  fmt_names=["no_format","number", "percents", "currency", "neutral", "warning", "bad", "good", "date1"]
  formats={k:v for k,v in zip(fmt_names, fmt_array)}
  return formats

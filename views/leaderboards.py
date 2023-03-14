# coding: utf-8
from flask import Blueprint, request, jsonify
from flask import send_from_directory
from app.cache import get_customers_lead
from app.utils.flask_helpers import build_response, send_file_response
from app.utils.mongo_utils import queryUpdateCache
from app.dao import dao
from app.cache_dao import cache_dao, mongo_sales_atDate
import datetime as dt
import pytz
import pandas as pd
import os, json
import functools, ast
from io import BytesIO

leaderboards = Blueprint("leaderboards", __name__)
tz = pytz.timezone('Indian/Reunion')

def set_pivot_df(values, index, columns=[]):
    def _pivot(dataframe):
        pivot = pd.pivot_table(dataframe, values, index, columns, aggfunc='sum')
        return pivot
    return _pivot

def rename_pivot_headers(renamed_cols, pivot, fillna_value=0):
    _df = pd.DataFrame(pivot.to_records()).fillna(0)
    return _df.rename(columns=renamed_cols)

def customers_leaderboard(dataframe):
    # by customers
    values=["linetotal"]
    index=["cardcode","cardname", "time", "plage"]
    pivot_fn = set_pivot_df(values, index)
    customers_df = pivot_fn(dataframe).sort_values(by="linetotal", ascending=False)
    return customers_df

def set_dataframe_for_docnum(dataframe):
    def get_docnums(cardcode, cardname):
        cleaned_cardname = cardname.replace("'", "\\'")
        return dataframe.query(f"cardcode=='{cardcode}' and cardname=='{cleaned_cardname}'").docnum.unique().tolist()
    return get_docnums

@leaderboards.route("/leaderboards/customers", methods=["GET"])
def show_customers_leaderboard_of_the_day():
    def reset_datetime(aDate):
        params = ["hour", "minute", "second", "microsecond"]
        return aDate.replace(**{k:0 for k in params})

    today = reset_datetime(dt.datetime.today())
    atDate = request.args.get("at-date")
    if atDate :
        DATE_FMT="%Y-%m-%d"
        today = reset_datetime(dt.datetime.strptime(atDate, DATE_FMT))

    data_df = get_customers_lead(today)
    extract_docnums = set_dataframe_for_docnum(data_df)
    custo_df = customers_leaderboard(data_df)
    leaderboard = pd.DataFrame(custo_df.to_records(), columns=["cardcode", "cardname", "time", "plage","linetotal"])
    leaderboard["docnums"] = [extract_docnums(r.cardcode, r.cardname) for r in leaderboard.itertuples()]
    data = leaderboard.to_dict(orient="records")
    response={}
    response["meta"]={"timestamp":tz.fromutc(dt.datetime.now()).strftime("%Y-%m-%dT%H:%M")}
    response["customers"]=data
    #json_data = leaderboard.to_json(orient="records", date_format="iso")

    return build_response(json.dumps(response))


def extract_parameters_from(request):
    def _reduce(acc, val):
        extracted = request.args.get(val)
        if extracted is not None:
            acc[val]=ast.literal_eval(extracted)
        return acc
    return _reduce

def set_to_value(dataframe):
    ISO_FMT="%Y-%m-%dT%H:%M:%SZ"
    def _from(label):
        if label=="isodate":
            return dataframe[label].to_pydatetime().strftime(ISO_FMT)
        return str(dataframe[label])
    return _from

def nth(index):
    def _take_from(x):
        return x[index]
    return _take_from

def update_and_return(label_fn, value_fn):
    def _update(acc, element):
        acc.update({label_fn(element): value_fn(element)})
        return acc
    return _update

def build_api_response():
    pass

def reset_to_midnight(date: dt.datetime) -> dt.datetime:
    params = ["hour", "minute", "second", "microsecond"]
    return date.replace(**{k:0 for k in params})

def date_from_str(date_str):
    ISO_FMT="%Y-%m-%d"
    return dt.datetime.strptime(date_str, ISO_FMT)

def today():
    return reset_to_midnight(dt.datetime.now())


def set_operations_order(ops_list):
    ops_order=["filter", "sort", "range"]
    return list(filter(lambda x: x in ops_list, ops_order))

def filter_date(dataframe, meta):
    def _filter(filter_params):
        date_str=filter_params["date"]
        return dataframe.query("isodate>='{}'".format(date_str)),meta
    return _filter

def paginate(dataframe, meta):
    def _paginate(paginate_params):
        page, perPage = [paginate_params[k] for k in ["page", "perPage"]]
        start_range = page*perPage
        end_range = (page+1)*perPage
        return dataframe.iloc[start_range:end_range,:],meta
    return _paginate

def set_range(dataframe,meta):
    def _range(range_params):
        start_range, end_range = range_params
        range_count=end_range-start_range+1
        meta["Content-Range"]="{}-{}/{}".format(start_range, end_range, len(dataframe))
        return dataframe.iloc[start_range:end_range+1,:],meta
    return _range

def set_sort(dataframe, meta):
    def _sort(sort_params):
        field, ascending_str = sort_params
        ascending  = True if ascending_str.lower() == "asc" else False
        return dataframe.sort_values(by=field, ascending=ascending), meta
    return _sort

def process_filter(cache_dao):
    def _process(dataframe, meta):
        def _filter(filter_params):
            if "date" in filter_params:
                date_arg = date_from_str(filter_params["date"])

                my_query={"docdate":{"$eq": date_arg}}
                if "cardname" in filter_params:
                    cardname=filter_params["cardname"]
                    my_query = {"$and":[{"docdate":{"$eq":date_arg}},
                                        {"cardname":{ '$regex' : '.*'+cardname+'.*', '$options' : 'i' }}]}
                invoices_rows = cache_dao.find_query(my_query)
                if len(invoices_rows)>0:
                    values=["linetotal"]
                    index=["cardcode","cardname", "isodate", "docnum"]
                    pivot_fn = set_pivot_df(values, index)
                    invoices_df = pd.DataFrame(pivot_fn(invoices_rows).to_records())
                    invoices_df.rename(columns={"docnum":"id"}, inplace=True)
                    return invoices_df, meta
            return pd.DataFrame(), meta
        return _filter
    return _process

def augment_response(response, metadata):
    if "Content-Range" in metadata:
        response.headers["Content-Range"]=metadata["Content-Range"]
    return response

@leaderboards.route("/leaderboards/invoices", methods=["GET"])
def get_invoices():
    query_labels=["filter", "sort", "range", "meta"]
    ops_order=query_labels[0:3]
    parameters = functools.reduce(extract_parameters_from(request), query_labels, {})
    meta={}
    ordered_ops = set_operations_order(parameters.keys())
    operations={"filter":process_filter(cache_dao), "range":set_range, "sort":set_sort}
    working_df=pd.DataFrame()
    for op in ordered_ops:
        op_fn = operations[op](working_df, meta)
        working_df, meta = op_fn(parameters[op])

    json_result = working_df.to_json(orient="records", date_format="iso")
    response = build_response(json_result)
    response = augment_response(response, meta)
    return response

@leaderboards.route("/leaderboards/invoices/<int:docnum>", methods=["GET"])
def get_one_invoice(docnum):
    docnum_df = cache_dao.find_query({"docnum":{"$eq":docnum}})
    values=["linetotal"]
    index=["cardcode","cardname", "isodate"]
    pivot_fn = set_pivot_df(values, index)
    document_labels = ["docnum","isodate","cardname"]
    doc_row_labels = ["itemcode","dscription","quantity","linetotal", "itmsgrpcod"]
    if len(docnum_df)>0:
        accu_fn = update_and_return(lambda x:x, set_to_value(docnum_df.iloc[0,:]))
        extract = functools.reduce(accu_fn, document_labels, {})
        extract["doctotal"]=str(docnum_df["linetotal"].sum())
        extract["items"] = docnum_df.loc[:,doc_row_labels].to_dict(orient="records")
    json_data=json.dumps(extract)
    return build_response(json_data)
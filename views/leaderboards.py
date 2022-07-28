# coding: utf-8
from flask import Blueprint, request, jsonify
from flask import send_from_directory

from app.utils.flask_helpers import build_response, send_file_response
from app.utils.mongo_utils import queryUpdateCache
from app.dao import dao
from app.cache_dao import cache_dao
import datetime as dt
import pandas as pd
import os, json
from io import BytesIO

leaderboards = Blueprint("leaderboards", __name__)
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
    index=["cardcode","cardname", "time"]
    pivot_fn = set_pivot_df(values, index)
    customers_df = pivot_fn(dataframe).sort_values(by="linetotal", ascending=False)
    return customers_df

def mongo_sales_atDate(at_date):
    fields = ["_id","cardname", "itemcode", "dscription"
             , "quantity", "linetotal", "isodate", "docnum"
             , "cardcode"]
    pipeline = [
        {"$match": {"docdate": at_date}}
        ,{"$project": {k:v for k,v in zip(fields, [0]+[1]*(len(fields)-1))}}
    ]
    options={}
    return pipeline, options

@leaderboards.route("/customers", methods=["GET"])
def show_customers_leaderboard_of_the_day():
    today = dt.datetime.now()
    raw_data = cache_dao.apply_aggregate(*mongo_sales_atDate(today))
    data_df = pd.DataFrame(list(raw_data))
    data_df["time"] = [str(row.isodate).split(" ")[1][0:5] for row in data_df.itertuples()]
    json_data = data_df.to_json(orient="records", date_format="iso")
    return build_response(json_data)
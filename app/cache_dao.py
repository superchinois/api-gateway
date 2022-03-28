# coding: utf-8
import pymongo
import pandas as pd
import datetime as dt
import itertools
import numpy as np


from app.cache import fetch_master_itemlist

def build_label(values, columns):
    return ["('sum', '{}', '{}')".format(x[0], x[1]) for x in itertools.product(values, columns)]

def getMondayOf(dtDate) -> dt.datetime:
    isocalendar = dtDate.isocalendar()
    year=isocalendar[0]
    week=isocalendar[1]
    monday=dt.datetime.strptime("{}-W{}".format(year,week)+'-1',"%Y-W%W-%w")
    previous_monday=monday - dt.timedelta(days=7)
    result_monday=monday
    if dtDate<monday:
        result_monday = previous_monday
    return result_monday

def getStartDateOfPeriod(periodInWeeks) -> dt.datetime:
    today = dt.datetime.now()
    timedelta = dt.timedelta(weeks=periodInWeeks)
    return getMondayOf(today-timedelta)

def sales_within_period(cardcode, periodInWeeks):
    start_date = getStartDateOfPeriod(periodInWeeks)
    query = {"$and":[{"supplier": cardcode}, {"docdate":{"$gt":start_date}}]}
    return query

def last_docnum(from_date: str, date_format: str):
    start_date=dt.datetime.strptime(from_date,date_format)
    pipeline = [
        {"$match":{"docdate":{"$gt":start_date}}},
        {"$project":{"docnum":1, "week_num":{"$isoWeek":"$docdate"}}},
        {"$sort":{"docnum":pymongo.DESCENDING}},
        {"$limit":1}
    ]
    options={}
    return pipeline, options

class CacheDao:
    keys = ["MONGO_URI", "MONGO_DATABASE", "MONGO_COLLECTION"]
    URI, DB, COLLECTION = keys
    DATE_FMT="%Y-%m-%d"

    def __init__(self) -> None:
        pass
    
    def init_app(self, app) -> None:
        self.config={k:v for k,v in map(lambda k: (k, app.config[k]), self.keys)}

    def get_collection_from_db(self, client):
        return client[self.config[self.DB]][self.config[self.COLLECTION]]

    def find_query(self, query) -> pd.DataFrame:
        return self.with_collection(lambda data: data.find(query))

    def apply_aggregate(self, pipeline, options):
        return self.with_collection(lambda data: data.aggregate(pipeline, options))

    def with_collection(self, computation) -> pd.DataFrame:
        with pymongo.MongoClient(self.config[self.URI]) as mg_client:
            collection_data = self.get_collection_from_db(mg_client)
            result = computation(collection_data)
            return pd.DataFrame(list(result))

    def getWeeklySales(self, cardcode, periodInWeeks):
        sales_df = self.find_query(sales_within_period(cardcode, periodInWeeks))
        sales_df["c"] = [getMondayOf(row.docdate).strftime(self.DATE_FMT) for row in sales_df.itertuples()]
        display_cols=["itemcode","itemname","onhand","onorder", "sellitem"]
        cols=["quantity"]
        dates = sales_df["c"].unique().tolist()
        dates.sort(reverse=True) # sorted from recent to older
        pvtable = pd.pivot_table(sales_df, index=["itemcode"],
                values=cols,
                columns=['c'],
                aggfunc=[np.sum],
                fill_value=0)

        dates_displayed = dates #list(map(lambda x:x[-5:], dates)) # from iso fmt, takes all but the year
        renamed={k:v for k,v in zip(build_label(cols, dates),dates_displayed)}
        salesDdff = pd.DataFrame(pvtable.to_records())
        salesDdff.rename(columns=renamed, inplace=True)
        master = fetch_master_itemlist()
        masterFiltered = master.query("cardcode=='{}'".format(cardcode))
        joinedData = pd.merge(masterFiltered, salesDdff, on=["itemcode"], how="inner")
        result = joinedData.loc[:,display_cols+dates_displayed]
        return result.set_index("itemcode"), dates_displayed



cache_dao = CacheDao()

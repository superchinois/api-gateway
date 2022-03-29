# coding: utf-8
import pymongo
import pandas as pd
import datetime as dt
import itertools
import numpy as np
import json
from app.query_utils import convertSerieToDataArray
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

def sales_for_item_between_dates(itemcode, fromDate, toDate):
    query={"$and":[{"itemcode":itemcode}, {"docdate":{"$gte":fromDate}}, {"docdate":{"$lte": toDate}}]}
    return query

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

    def getSalesStatsforItem(self, itemcode, fromDate, toDate, movingAvg=0):
        df = self.find_query(sales_for_item_between_dates(itemcode, fromDate, toDate))
        index_fields=["docdate", "itemcode"]
        values_fields=["quantity", "linetotal"]
        columns_fields=[]
        pivotDf = pd.pivot_table(df, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
        outputDf=pd.DataFrame(pivotDf.to_records())
        columns_renamed={"('sum', 'quantity')":'quantity', "('sum', 'linetotal')":'linetotal'}
        outputDf.rename(columns=columns_renamed, inplace=True)
        df = outputDf.sort_values(by=["docdate"])
        result = {}
        output_cols = ["docdate","quantity","linetotal"]
        value_to_plot="quantity"
        qstring="itemcode=='{}'"
        masterdata = fetch_master_itemlist()
        itemcodes=[itemcode]
        for code in itemcodes:
            ydata=df.query(qstring.format(code)).loc[:,output_cols].set_index("docdate")
            start = ydata.index[0]
            end = toDate
            X = pd.date_range(start, end, freq='D')
            Y = pd.Series(ydata[value_to_plot], index=X)
            item_data = masterdata.query(qstring.format(code))
            data = convertSerieToDataArray(Y)
            if movingAvg>1:
                data = convertSerieToDataArray(Y.fillna(0.0).rolling(window=movingAvg).mean())
            result[code] = {"itemname": item_data.itemname.tolist()[0],"data": data}
        return json.dumps(result[itemcode])

    def getImportSales(self, cardcode, periodInWeeks):
        display_cols=["itemname", "sellitem", "categorie", "vente", "achat", "revient","marge_theo", "marge_sap", "onhand", "onorder"]
        cols=["quantity", "ca_ht", "linegrossbuypr"]
        df=self.find_query(sales_within_period(cardcode, periodInWeeks))
        # Force value 0.0 into discprcnt column
        #for idx in range(len(df)):
        #  row=df.iloc[idx]
        #  if np.isnan(row.discprcnt):
        #    df.loc[idx,'discprcnt']=0.0
        df["ca_ht"] = df["linetotal"] #*(1-df["discprcnt"]/100)
        df["c"] = [getMondayOf(row.docdate).strftime(self.DATE_FMT) for row in df.itertuples()]
        df["linegrossbuypr"] = df["quantity"]*df["grossbuypr"]
        pvtable = pd.pivot_table(df, index=["itemcode"],
                    values=cols,
                    columns=['c'],
                    aggfunc=[np.sum],
                    fill_value=0)
        ddff = pd.DataFrame(pvtable.to_records())
        all_columns = ddff.head().columns.values.tolist()
        column_groups=list(map(lambda col: filter(lambda x: col in x, all_columns), cols))
        column_groups=[sorted(x, reverse=True) for x in column_groups]
        ddf=ddff.set_index("itemcode")
        # compute sum over period for quantity, linetotal, linegrossbuypr
        for colidx, colname in enumerate(cols):
          ddf["total_{}".format(colname)] = ddf.loc[:,list(column_groups[colidx])].sum(axis=1)

        masterdata = fetch_master_itemlist()
        displayed=display_cols+list(map(lambda x: "total_{}".format(x), cols[:2]))+list(itertools.chain.from_iterable(column_groups[:2]))

        joinedData = masterdata.set_index("itemcode").query("cardcode=='{}'".format(cardcode)).join(ddf).fillna(0)
        joinedData["marge_theo"]=(joinedData["vente"]-joinedData["revient"])/joinedData["revient"]
        joinedData["marge_sap"]=(joinedData["total_ca_ht"]-joinedData["total_linegrossbuypr"])/joinedData["total_linegrossbuypr"]

        outputData = joinedData.loc[:,displayed]
        renamed = {k:"".join(k[1:-1].split(",")[1:]).replace("'","").strip() for k in list(itertools.chain.from_iterable(column_groups[:2]))}
        outputData.rename(columns=renamed, inplace=True)

        return outputData



cache_dao = CacheDao()

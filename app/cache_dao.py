# coding: utf-8
import pymongo
import pandas as pd
import datetime as dt
import itertools
import functools
import numpy as np
import json
from app.utils.query_utils import convertSerieToDataArray, build_pivot_labels,compute_months_dict_betweenDates
from app.utils.metrics_helpers import set_pivot_df
from app.dao import dao

fetch_master_itemlist = dao.getMasterDataDf

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
    query = {"$and":[{"supplier": cardcode}, {"docdate":{"$gte":start_date}}]}
    return query

def customer_sales_within_period(cardcode, periodInWeeks):
    start_date = getStartDateOfPeriod(periodInWeeks)
    query = {"$and":[{"cardcode": cardcode}, {"docdate":{"$gte":start_date}}]}
    return query

def last_docnum(from_date: dt.datetime):
    pipeline = [
        {"$match":{"docdate":{"$gt":from_date}}},
        {"$project":{"docnum":1, "timestamp":{"$dateToString": {"format":"%Y-%m-%dT%H:%M:%S", "date":"$isodate"}}}},
        {"$sort":{"docnum":pymongo.DESCENDING}},
        {"$limit":1}
    ]
    options={}
    return pipeline, options

def get_historical_sales_for_itemcode(itemcode:str, from_date: dt.datetime, to_date: dt.datetime):
    pipeline = [
        {"$match":{"$and": [{"itemcode": itemcode}, {"docdate":{"$gte":from_date}}, {"docdate":{"$lte":to_date}}]}},
        {"$project":{"cardname":1, "quantity":1, "year":{"$year":"$docdate"}, "month":{"$month":"$docdate"}}},
    ]
    options={}
    return pipeline, options

def get_historical_sales_between(from_date: dt.datetime, to_date: dt.datetime):
    pipeline = [
        {"$match":{"$and": [{"docdate":{"$gte":from_date}}, {"docdate":{"$lt":to_date}}]}},
        {"$group": { "_id":"$itemcode", "quantity": { "$sum": "$quantity" }, "linetotal":{"$sum":"$linetotal"}}},
    ]
    options={}
    return pipeline, options

def sales_for_item_between_dates(itemcode, fromDate, toDate):
    query={"$and":[{"itemcode":itemcode}, {"docdate":{"$gte":fromDate}}, {"docdate":{"$lte": toDate}}]}
    return query

def sales_for_items_between_dates(itemcodes, fromDate, toDate):
    query={"$and":[{"itemcode":{"$in": itemcodes}}, {"docdate":{"$gte":fromDate}}, {"docdate":{"$lte": toDate}}]}
    return query

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

class CacheDao:
    keys = ["MONGO_URI", "MONGO_DATABASE", "MONGO_COLLECTION"]
    URI, DB, COLLECTION = keys
    DATE_FMT="%Y-%m-%d"

    def __init__(self) -> None:
        pass
    
    def init_app(self, app) -> None:
        self.config={k:v for k,v in map(lambda k: (k, app.config[k]), self.keys)}
        self._client = pymongo.MongoClient(self.config[self.URI])
    def get_client(self):
        return self._client

    def get_collection_from_db(self, client):
        return client[self.config[self.DB]][self.config[self.COLLECTION]]

    def find_query(self, query) -> pd.DataFrame:
        foundData = self.with_collection(lambda data: data.find(query))
        return pd.DataFrame(list(foundData))

    def apply_aggregate(self, pipeline, options):
        foundData = self.with_collection(lambda data: data.aggregate(pipeline))
        return foundData

    def with_collection(self, computation) -> pd.DataFrame:
        mg_client = self.get_client() #with pymongo.MongoClient(self.config[self.URI]) as mg_client:
        collection_data = self.get_collection_from_db(mg_client)
        result = computation(collection_data)
        return result

    def deleteFromQuery(self, query) -> None:
        result = self.with_collection(lambda data: data.delete_many(query))
        print(result.deleted_count)

    def last_record(self):
        sinceDate=dt.datetime.now()-dt.timedelta(weeks=8)
        docnum_obj = list(self.apply_aggregate(*last_docnum(sinceDate)))
        if len(docnum_obj)>0:
            value = docnum_obj[0].pop('_id', None)
        return docnum_obj[0]

    def importFromDataframe(self, dataframe):
        data_to_import = dataframe.to_dict("records")
        self.with_collection(lambda data: data.insert_many(data_to_import))

    def getSalesForItem(self, itemcode, fromDate, toDate):
        return self.find_query(sales_for_item_between_dates(itemcode, fromDate, toDate))

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
        df = self.getSalesForItem(itemcode, fromDate, toDate)
        if not df.empty:
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
            return result[itemcode]
        else:
            return {}

    def getImportSales(self, cardcode, periodInWeeks):
        def get_reception_data(itemcode, itemcodes_received, filtered_receptions):
            values=["docdate", "quantity"]
            if itemcode not in itemcodes_received:
                return [[""]*len(values)]
            else:
                received = filtered_receptions.query("itemcode=='{}'".format(itemcode)).loc[:,values]
                return [[d.to_pydatetime(), q] for d,q in received.values.tolist()]
    
        display_cols=["itemname", "sellitem", "categorie", "vente", "achat", "revient","marge_theo", "marge_sap", "rdate", "rqty", "onhand", "onorder"]
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
        pivot_table = set_pivot_df(cols, ["itemcode"],['c'])
        pvtable = pd.pivot_table(df, index=["itemcode"],
                    values=cols,
                    columns=['c'],
                    aggfunc="sum",
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

        toDate   = dt.datetime.today()
        two_yeas_in_days = 2*365
        origin_reception=(toDate - dt.timedelta(days=two_yeas_in_days)).replace(month=1, day=1).strftime(self.DATE_FMT)
        receptions = dao.getReceptionsMarchandise(origin_reception).loc[:,["itemcode", "docdate", "quantity"]].sort_values(by=["docdate"], ascending=False)
        received_itemcodes = set(receptions.itemcode.values)
        last_receptions = pd.DataFrame([[code]+get_reception_data(code, received_itemcodes, receptions)[0] for code in joinedData.index], columns=["itemcode", "rdate", "rqty"])
        outputData = joinedData.join(last_receptions.set_index("itemcode"), on="itemcode").loc[:,displayed]
        #renamed = {k:"".join(k[1:-1].split(",")[1:]).replace("'","").strip() for k in list(itertools.chain.from_iterable(column_groups[:2]))}
        renamed = {k:" ".join(eval(k))  for k in list(itertools.chain.from_iterable(column_groups[:2]))}
        outputData.rename(columns=renamed, inplace=True)
        return outputData

    def compute_sales_for_itemcodes_betweenDates(self, itemcodes, fromDate, toDate):
        result_df = self.find_query(sales_for_items_between_dates(itemcodes, fromDate, toDate))
        df=result_df
        index_fields=["docdate", "itemcode"]
        values_fields=["quantity", "linetotal"]
        columns_fields=[]
        pivotDf = pd.pivot_table(df, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
        outputDf=pd.DataFrame(pivotDf.to_records())
        columns_renamed={"('sum', 'quantity')":'quantity', "('sum', 'linetotal')":'linetotal'}
        outputDf.rename(columns=columns_renamed, inplace=True)
        return outputDf

    def getItemsBoughtByClient(self, cardcode, periodInWeeks):
        df = self.find_query(customer_sales_within_period(cardcode, periodInWeeks))
        masterdata = fetch_master_itemlist()

        df["c"]=[getMondayOf(row.docdate).strftime(self.DATE_FMT) for row in df.itertuples()]
        index_fields=["itemcode", "dscription"]
        values_fields=["quantity"]
        columns_fields=["c"]
        NB_WEEKS=4
        pvdf = pd.pivot_table(df, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
        outputDf=pd.DataFrame(pvdf.to_records())
        column_labels = df["c"].unique().tolist()
        column_labels.sort(reverse=True)
        labels_count=len(column_labels)
        ind_fields_count=len(index_fields)
        shortened_col_labels = list(map(lambda x:x[5:],column_labels))
        columns_renamed = {k:v for k,v in zip(build_label(values_fields, column_labels), shortened_col_labels)}
        added_displayed_cols = ["occurences", "total","moy"]
        outputDf.rename(columns=columns_renamed, inplace=True)
        if NB_WEEKS<labels_count:
          last_weeks_label="last{}w".format(NB_WEEKS)
          outputDf[last_weeks_label]=[functools.reduce(lambda a,b:a+b, map(lambda x: 1 if row[x]>0 else 0, range(ind_fields_count+labels_count-NB_WEEKS,ind_fields_count+labels_count))) for row in outputDf.itertuples(index=False)]
          added_displayed_cols = [last_weeks_label] + added_displayed_cols
        outputDf["occurences"]=[functools.reduce(lambda a,b:a+b, map(lambda x: 1 if row[x]>0 else 0, range(ind_fields_count,ind_fields_count+labels_count))) for row in outputDf.itertuples(index=False)]
        outputDf["total"]=outputDf.loc[:,shortened_col_labels].sum(axis=1)
        outputDf["moy"]=outputDf["total"]/outputDf["occurences"]
        outputDf = outputDf.sort_values(["occurences"], ascending=[0])
        out_cols=[*index_fields, "onhand","pcb_achat",*shortened_col_labels,*added_displayed_cols,"categorie"]
        merged = pd.merge(outputDf.loc[:,index_fields+shortened_col_labels+added_displayed_cols], masterdata.loc[:,["itemcode", "onhand","categorie", "pcb_achat"]],
          on=["itemcode"])
        return merged.loc[:,out_cols]

    def compute_sales_for_itemcode(self, itemcode, fromDate, toDate):
        df = pd.DataFrame(list(self.apply_aggregate(*get_historical_sales_for_itemcode(itemcode, fromDate, toDate))))
        periods = compute_months_dict_betweenDates(fromDate,toDate)
        index_fields = ["cardname"]
        values_fields = ['quantity']
        columns_fields = ['year','month']
        pvtable = pd.pivot_table(df, index=index_fields,
        values=values_fields,
        columns=columns_fields,
        aggfunc="sum",
        fill_value=0)
        df1 = pd.DataFrame(pvtable.to_records())
        date_labels=[]
        for (y,m) in periods.items():
            date_labels = date_labels + ["{}-{}".format(str(x[0]),str(x[1]).zfill(2)) for x in itertools.product([y], m)]

        quantity_labels = []
        for (y,m) in periods.items():
            quantity_labels = quantity_labels + build_pivot_labels(["quantity"], [y], m)

        df1.rename(columns={k:v for (k,v) in zip(quantity_labels, date_labels)},inplace=True)
        df_columns = df1.columns.values.tolist() 
        for date in date_labels:
            if date not in df_columns:
                df1[date]=0

        dates = df1.columns.tolist()[1:]
        dates.sort(reverse=True)
        df1["total"]=df1.loc[:,date_labels].sum(axis=1)
        cols = ["total"]+date_labels
        ALL_CLIENTS_LABEL = " TOTAL CLIENTS"
        row_to_insert = pd.Series([ALL_CLIENTS_LABEL]+[df1[x].sum() for x in cols], index=["cardname"]+cols)
        result_df = pd.concat([df1, row_to_insert.to_frame().T], ignore_index=True)
        result_df["freq"]=result_df.loc[:,date_labels].gt(0).sum(axis=1)
        past_months=dates[1:]
        result_df["moy"]=result_df.loc[:,past_months].replace(0, np.nan).mean(axis=1, skipna=True)
        result_df["moy"].replace(0, np.nan, inplace=True)
        result_df["remplis."]=(result_df[dates[0]]-result_df["moy"])/result_df["moy"]
        result_df=result_df.sort_values(["freq","total"], ascending=[0,0])

        output_cols = ["cardname","total","freq"]+dates+["moy", "remplis."]
        return result_df.loc[:, output_cols]


cache_dao = CacheDao()

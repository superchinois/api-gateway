# coding: utf-8
import xlsxwriter
import pymssql
import pandas as pd
import datetime as dt
from functools import reduce
import os, re
import json
import sqlite3
import itertools
import numpy as np
from app.query_utils import compute_months_dict_betweenDates, querybuilder, build_query_cash, convertSerieToDataArray
from app.query_utils import build_query_over, build_period, build_pivot_labels


master_data_query="""select t0.itemcode, t0.itemname,t0.codebars,t7.Price as vente, t8.rate, 
t0.salfactor2 as 'pcb_vente', t0.purfactor2 as 'pcb_achat', t0.purfactor3 as 'pcb_pal', t0.onhand, t0.onorder, 
t0.cardcode, t1.cardname, t2.ItmsGrpNam as categorie, t0.sellitem ,t5.Price as achat, t6.Price as revient 
from sbo_sis.dbo.OITM t0 
left join sbo_sis.dbo.OCRD t1 on t1.CardCode=t0.cardcode 
full join sbo_sis.dbo.OITB t2 on t0.ItmsGrpCod=t2.ItmsGrpCod 
left join sbo_sis.dbo.ITM1 t5 on t0.ItemCode=t5.ItemCode and t5.PriceList='3' 
left join sbo_sis.dbo.ITM1 t6 on t0.ItemCode=t6.ItemCode and t6.PriceList='4' 
left join sbo_sis.dbo.ITM1 t7 on t0.ItemCode=t7.ItemCode and t7.PriceList='1' 
left join sbo_sis.dbo.OVTG t8 on t8.code=t0.vatgourpsa
"""

def items_by_client(cardcode, periodInWeeks):
  qry="""select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week,t0.itemcode, t0.dscription, 
  t0.quantity as quantity, t1.docdate, t2.onhand from dbo.inv1 t0 
  join dbo.oinv t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
  and t1.cardcode='{client}'
  join dbo.oitm t2 on t2.itemcode=t0.itemcode
  union all
  select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week,t0.itemcode, t0.dscription, 
  -t0.quantity as quantity, t1.docdate, t2.onhand from dbo.rin1 t0 
  join dbo.orin t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
  and t1.cardcode='{client}'
  join dbo.oitm t2 on t2.itemcode=t0.itemcode
  """
  return qry.format(client=cardcode, period=periodInWeeks)

def stock_query(db, dateFrom):
  stock_qry="""select '{type}' as type, t1.docnum, t0.itemcode, t2.itemname,
     t1.docdate, t1.doctime, t3.cardname, t2.onhand, {operator}t0.quantity as quantity, t1.comments, t4.u_name 
     from dbo.{table_ligne} t0 
     join dbo.{table} t1 on t1.DocEntry=t0.DocEntry and {date_condition}
     join dbo.ousr t4 on t4.userid=t1.usersign
     join dbo.OITM t2 on t2.ItemCode=t0.ItemCode
     join dbo.OCRD t3 on t3.CardCode=t2.cardcode"""
  fields=["type", "operator", "table_ligne", "table"]
  map_params = {k:v for (k,v) in map(lambda x: (x,db[x]), fields)}
  map_params["date_condition"]= "t1.docdate>='{}'".format(dateFrom)
  return stock_qry.format_map(map_params)

def build_label(values, columns):
    return ["('sum', '{}', '{}')".format(x[0], x[1]) for x in itertools.product(values, columns)]

def assign_date(row):
  monday=dt.datetime.strptime("{}-W{}".format(row.year,row.week)+'-1',"%Y-W%W-%w")
  previous_monday=monday - dt.timedelta(days=7)
  result_monday=monday
  if row.docdate<monday:
    result_monday = previous_monday
  return result_monday.strftime("%Y-%m-%d")

class SapDao:
  keys=["SAP_DB_SERVER_IP","MSSQL_USER","MSSQL_PASS","SIS_DB_NAME","SQLITE_TAGS_DB", "SUPPLIERS_INFO_FILE"]
  SQLITE_TAGS_DB=4
  SUPPLIERS_INFO_FILE=5
  tags_db_k = keys[SQLITE_TAGS_DB]
  suppliers_info_file_k = keys[SUPPLIERS_INFO_FILE]
  def __init__(self):
    pass

  def init_app(self, app):
    self.config={k:v for k,v in map(lambda k: (k, app.config[k]), self.keys)}
  
  def print_config(self):
    for k in self.keys:
      print(self.config[k])

  def make_mssql_connect(self):
    server, user, password, db_name = map(lambda x: self.config[x], self.keys[slice(4)])
    return pymssql.connect(server, user, password, db_name)

  def execute_query(self, query):
  # Execute sql query with pandas
    with self.make_mssql_connect() as msconn:
      df = pd.read_sql_query(query, msconn)
    return df

  def getSqliteConnection(self, sqlite_filepath):
    return sqlite3.connect(sqlite_filepath)

  def executeSqliteQuery(self, query):
    with self.getSqliteConnection(self.config[self.tags_db_k]) as conn:
      return pd.read_sql_query(query, conn)

  def dfToJson(self, dataframe):
    return dataframe.to_json(orient='records', date_format='iso')

  def execute_query_toJson(self, query):
    return self.dfToJson(self.execute_query(query))

  def execute_sqliteQuery_toJson(self, query):
    return self.dfToJson(self.executeSqliteQuery(query))

  def getMasterdataJson(self):
    return self.dfToJson(self.getMasterDataDf())

  def getMasterDataDf(self):
    return self.execute_query(master_data_query)

  def getBusinessPartners(self, partial_name):
    return self.execute_query("""select t0.cardcode, t0.cardname, t0.u_tournee, t0.u_transp, t0.mailcity
      from dbo.ocrd t0 where t0.cardname like '%{}%'""".format(partial_name))

  def get_supplier_info(self, cardcode):
    contacts = pd.read_csv(self.config[self.suppliers_info_file_k], sep=";").fillna("")
    emails = contacts[contacts["cardcode"]==cardcode]["email"].values.tolist()
    labeled_emails = map(lambda e:{"label":e, "value":e}, emails)
    return json.dumps({"order_emails":list(labeled_emails)})

  def normalize_comments(self, comment, sep=" "):
    if comment: 
      return comment.replace('\r', sep)
    else:
      return comment

  def isTargetDocExist(self, docnum):
    query="""select t0.itemcode,t0.dscription,t0.quantity,t0.trgetentry,t0.targettype from dbo.por1 t0 
    join dbo.opor t1 on t1.docentry=t0.docentry and t1.docnum='{}'""".format(docnum)
    df=self.execute_query(query)
    return all(df.trgetentry)


  def getOnOrders(self):
    orders = self.execute_query("""select t0.docnum, t0.cardcode, t0.cardname, t0.docdate, t0.docstatus, t0.comments from dbo.opor t0 
  where t0.docstatus='O' and abs(datediff(wk, getdate(),t0.docdate))<3""")
    orders["comments"] = orders.apply(lambda row: self.normalize_comments(row.comments), axis=1)
    orders.sort_values(by=["cardname"], inplace=True)
    return orders

  def getDocLines(self, docnum):
    lines = self.execute_query("""select t0.linenum, t0.itemcode, t0.dscription, t0.serialnum, t0.u_dluo,t0.price, 
      t0.factor1, t0.factor2, t0.quantity, t0.visorder
      from dbo.por1 t0 join dbo.opor t1 on t1.docentry=t0.docentry and t1.docnum='{}'""".format(docnum))
    lines.fillna("", inplace=True)
    return pd.DataFrame(lines.to_records(index=False))


  def purchaseOrdersWithFileNumber(self):
    qry="""select t0.docdate,t0.cardname, t0.docentry,t0.docstatus, t0.printed, t0.comments from dbo.opor t0 
    join dbo.ocrd t1 on t1.cardcode=t0.cardcode and t1.groupcode='104'
    where t0.docdate>'{}' and t0.docstatus='O'
    """.format("20200101")
    df = self.execute_query(qry)
    df["comments"] = df.apply(lambda row: self.normalize_comments(row["comments"]), axis=1)
    df["targetentry"]=df.apply(lambda row:self.isTargetDocExist(row.docentry), axis=1)
    regexDI_grp=r'(\bDI[ ]{0,1})(\d{4,})'
    #regexDI=r'\bDI[ ]{0,1}\d{4,}'
    dossier=[]
    for i in range(len(df)):
      row = df.iloc[i]
      if row["comments"] and row["targetentry"]==False:
        matched = re.findall(regexDI_grp, row.comments)
        if len(matched)>0:
          dossier.append([matched[0][1], row])
    sorted_dossier = sorted(dossier, key=lambda x:x[0])
    return sorted_dossier

  def getBatchNumbersAndQuantities(self, itemcodes):
    qry="""select t0.itemcode,t0.itemname,t0.batchnum, t0.whscode, t0.expdate, t0.quantity, 
    t0.cardname, t0.createdate, t0.sysnumber from dbo.oibt t0 where t0.itemcode in ({}) and t0.whscode='02' 
    and t0.quantity>0"""
    batchData = self.execute_query(qry.format(",".join(itemcodes)))
    return batchData

  def invoices_cash_weeks(self):
    return """select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week,
    t1.docdate, t0.itemcode, t0.quantity, t0.linetotal, t0.grossbuypr, t1.discprcnt from dbo.inv1 t0 
    join dbo.oinv t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode and t2.cardcode='{cardcode}'
    union all
    select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week,
    t1.docdate, t0.itemcode, -t0.quantity, -t0.linetotal, t0.grossbuypr, t1.discprcnt from dbo.rin1 t0 
    join dbo.orin t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode and t2.cardcode='{cardcode}'
    """

  def getSales(self, cardcode, periodInWeeks):
    qry="""select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week, t1.docdate,
    t0.itemcode, t2.itemname, t0.quantity,t2.onhand from dbo.inv1 t0 
    join dbo.oinv t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode and t2.cardcode='{cardcode}'"""
    # union all
    # select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week, t1.docdate,
    # t0.itemcode, t2.itemname, -t0.quantity,t2.onhand from dbo.inv1 t0 
    # join dbo.oinv t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
    # join dbo.oitm t2 on t2.itemcode=t0.itemcode and t2.cardcode='{cardcode}'
    # """
    df=self.execute_query(qry.format_map({"cardcode":cardcode, "period":periodInWeeks}))
    df["c"]=[assign_date(row) for row in df.itertuples(index=False)]
    return df

  def getWeeklySales(self, cardcode, periodInWeeks):
    display_cols=["itemname","onhand","onorder", "sellitem"]
    cols=["quantity"]
    salesDf = self.getSales(cardcode, periodInWeeks)
    dates = salesDf["c"].unique().tolist()
    dates.sort(reverse=True) # sorted from recent to older
    pvtable = pd.pivot_table(salesDf, index=["itemcode"],
            values=cols,
            columns=['c'],
            aggfunc=[np.sum],
            fill_value=0)
    salesDdff = pd.DataFrame(pvtable.to_records())
    dates_labels = build_label(["quantity"], dates)
    salesDdf = salesDdff.set_index("itemcode")
    masterdata = self.getMasterDataDf()
    joinedData = masterdata.set_index("itemcode").query("cardcode=='{}'".format(cardcode)).join(salesDdf).fillna(0)
    renamed={k:v for k,v in zip(dates_labels, dates)}
    joinedData.rename(columns=renamed, inplace=True)
    return joinedData.loc[:,display_cols+dates], dates

  def getImportSales(self, cardcode, periodInWeeks):
    display_cols=["itemname", "sellitem", "categorie", "vente", "achat", "revient","marge_theo", "marge_sap", "onhand", "onorder"]
    cols=["quantity", "ca_ht", "linegrossbuypr"]
    df=self.execute_query(self.invoices_cash_weeks().format_map({"cardcode":cardcode, "period":periodInWeeks}))
    # Force value 0.0 into discprcnt column
    for idx in range(len(df)):
      row=df.iloc[idx]
      if np.isnan(row.discprcnt):
        df.loc[idx,'discprcnt']=0.0
    df["ca_ht"] = df["linetotal"]*(1-df["discprcnt"]/100)
    df["c"] = df.apply(lambda row: assign_date(row), axis=1)
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

    masterdata = self.getMasterDataDf()
    displayed=display_cols+list(map(lambda x: "total_{}".format(x), cols[:2]))+list(itertools.chain.from_iterable(column_groups[:2]))

    joinedData = masterdata.set_index("itemcode").query("cardcode=='{}'".format(cardcode)).join(ddf).fillna(0)
    joinedData["marge_theo"]=(joinedData["vente"]-joinedData["revient"])/joinedData["revient"]
    joinedData["marge_sap"]=(joinedData["total_ca_ht"]-joinedData["total_linegrossbuypr"])/joinedData["total_linegrossbuypr"]

    outputData = joinedData.loc[:,displayed]
    renamed = {k:"".join(k[1:-1].split(",")[1:]).replace("'","").strip() for k in list(itertools.chain.from_iterable(column_groups[:2]))}
    #renamed2={k.replace("quantity","q"):v for k,v in renamed.items()}
    outputData.rename(columns=renamed, inplace=True)

    return outputData

  def getGoodReceiptsPo(self, cardcode, periodInWeeks):
    qry="""select year(t1.docdate) as year, month(t1.docdate) as month, datepart(wk, t1.docdate) as week,
    t1.docdate, t0.itemcode, t0.quantity from dbo.pdn1 t0 
    join dbo.opdn t1 on t1.docentry=t0.docentry and abs(datediff(wk, getdate(), t1.docdate))<={period}
    join dbo.oitm t2 on t2.itemcode=t0.itemcode and t2.cardcode='{cardcode}'
    """
    df=self.execute_query(qry.format_map({"cardcode":cardcode, "period":periodInWeeks}))
    df["c"]=[assign_date(row) for row in df.itertuples(index=False)]
    return df

  def getEntreeSortiesMarchandise(self, dateFrom):
    select=[
    {"type":"entree", "table":"OIGN", "table_ligne":"IGN1", "operator":""},
    {"type":"sortie", "table":"OIGE", "table_ligne":"IGE1", "operator":"-"},
    {"type":"entr_march", "table":"OPDN", "table_ligne":"PDN1", "operator":""},
    {"type":"retour", "table":"ORPD", "table_ligne":"RPD1", "operator":"-"}
    ]
    index_fields=["itemcode", "itemname", "onhand"]
    values_fields = ["quantity"]
    columns_fields = ["type"]
    column_labels=[db["type"] for db in select]
    columns_renamed = {v:k for k,v in zip(column_labels, build_label(values_fields, column_labels))}
    dfs = []
    for db in select :
      df = self.execute_query(stock_query(db, dateFrom))
      dfs.append(df)
    concatenatedDf = pd.concat(dfs)
    concatenatedDf["comments"]=[self.normalize_comments(row.comments) for row in concatenatedDf.itertuples()]
    
    pv = pd.pivot_table(concatenatedDf, index=index_fields,
              values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)

    pivotDf=pd.DataFrame(pv.to_records())
    pivotDf.rename(columns=columns_renamed, inplace=True)
    if ("sortie" in pivotDf.columns.tolist()):
      pivotDf["solde"]=pivotDf.apply(lambda row: row.entree+row.sortie, axis=1)
    else:
      pivotDf["solde"]=pivotDf.apply(lambda row: row.entree, axis=1)
    output = pivotDf.sort_values(["entree", "itemname"], ascending=[0,1])
    concatenatedDf.sort_values(by=["docdate", "doctime"], inplace=True)
    forced_entries=pd.pivot_table(concatenatedDf.query("u_name=='gilette' and type=='entree'"), index=index_fields, values=["quantity"], aggfunc='count', fill_value=0)
    cashierSiDf = pd.DataFrame(forced_entries.to_records()).sort_values(by=["quantity"], ascending=False)
    return output, concatenatedDf, cashierSiDf

  def getReceptionsMarchandise(self, fromDate):
      fields = ["_pdn1.itemcode"
                #,"_pdn1.linenum"
                ,"_opdn.docdate"
                ,"_opdn.docnum"
                ,"_opdn.numatcard"
                ,"_opdn.cardcode"
                ,"_opdn.cardname"
                ,"_opdn.comments"
                ,"_pdn1.quantity"
                ,"_pdn1.u_dluo"
                ,"_pdn1.dscription"
                ,"_itm1.price"
                ,"_pdn1.serialnum"]
      entree_march_params={
      'fields':fields,
      'tables':"pdn1 _pdn1",
      'join':{
        "opdn _opdn":(1,["_opdn.docentry=_pdn1.docentry", "_opdn.docdate>='{}'".format(fromDate)]),
        "oitm _oitm":(2,["_pdn1.itemcode=_oitm.itemcode"]),
        "itm1 _itm1":(3,["_pdn1.itemcode=_itm1.itemcode","_itm1.pricelist='4'"]),
        },
      'orderby':"_pdn1.itemcode"
      }
      q=querybuilder(entree_march_params)
      df=self.execute_query(q)
      df["comments"] = [self.normalize_comments(row.comments) for row in df.itertuples()]
      return df

  def getItemsBoughtByClient(self, cardcode, periodInWeeks):
    df = self.execute_query(items_by_client(cardcode, periodInWeeks))
    masterdata = self.getMasterDataDf()

    df["c"]=[assign_date(row) for row in df.itertuples()]
    index_fields=["itemcode", "dscription", "onhand"]
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
      outputDf[last_weeks_label]=[reduce(lambda a,b:a+b, map(lambda x: 1 if row[x]>0 else 0, range(ind_fields_count+labels_count-NB_WEEKS,ind_fields_count+labels_count))) for row in outputDf.itertuples(index=False)]
      added_displayed_cols = [last_weeks_label] + added_displayed_cols
    outputDf["occurences"]=[reduce(lambda a,b:a+b, map(lambda x: 1 if row[x]>0 else 0, range(ind_fields_count,ind_fields_count+labels_count))) for row in outputDf.itertuples(index=False)]
    outputDf["total"]=outputDf.loc[:,shortened_col_labels].sum(axis=1)
    outputDf["moy"]=outputDf["total"]/outputDf["occurences"]
    outputDf = outputDf.sort_values(["occurences"], ascending=[0])
    out_cols=[*index_fields, "pcb_achat",*shortened_col_labels,*added_displayed_cols,"categorie"]
    merged = pd.merge(outputDf.loc[:,index_fields+shortened_col_labels+added_displayed_cols], masterdata.loc[:,["itemcode", "categorie", "pcb_achat"]],
      on=["itemcode"])
    return merged.loc[:,out_cols]

  def getItemsOnSale(self, today):
    query="""select t0.itemcode, t0.cardcode, t0.price, t0.discount, t0.fromdate, t0.todate from dbo.spp1 t0 where t0.listnum='1'"""
    df=self.execute_query(query)
    todayString = today.strftime("%Y-%m-%d")
    qry_result = df.query("cardcode=='*1' and fromdate<='{today}' and todate>='{today}'".format_map({"today":todayString}))
    return qry_result

  def getDiscountedItemsFromDate(self, dateString):
    query="""select t0.itemcode, t0.cardcode, t0.price, t0.discount, t0.fromdate, t0.todate from dbo.spp1 t0 where t0.listnum='1'"""
    df=self.execute_query(query)
    qry_result = df.query("cardcode=='*1' and todate>='{today}'".format_map({"today":dateString}))
    return qry_result.query("discount>0")

  def compute_sales_for_itemcodes_betweenDates(self, itemcodes, fromDate, toDate):
    months = compute_months_dict_betweenDates(fromDate,toDate)
    queries = [build_query_cash(",".join(["'{}'".format(x) for x in itemcodes]), y, m) for (y,m) in months.items()]
    result_df = []
    for qry in queries:
        result_df.append(self.execute_query(qry))
    df=pd.concat(result_df)
    index_fields=["docdate", "itemcode", "targettype"]
    values_fields=["quantity", "linetotal"]
    columns_fields=[]
    pivotDf = pd.pivot_table(df, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
    outputDf=pd.DataFrame(pivotDf.to_records())
    columns_renamed={"('sum', 'quantity')":'quantity', "('sum', 'linetotal')":'linetotal'}
    outputDf.rename(columns=columns_renamed, inplace=True)
    return outputDf.query("targettype == -1")

  def getSalesStatsforItem(self, itemcode, fromDate, movingAvg=0):
    itemcodes=[itemcode]
    nowDate = dt.datetime.now().date()
    df = self.compute_sales_for_itemcodes_betweenDates(itemcodes, fromDate, nowDate)
    df = df.sort_values(by=["docdate"])
    result = {}
    output_cols = ["docdate","quantity","linetotal"]
    value_to_plot="quantity"
    qstring="itemcode=='{}'"
    masterdata = self.getMasterDataDf()
    for code in itemcodes:
        ydata=df.query(qstring.format(code)).loc[:,output_cols].set_index("docdate")
        start = ydata.index[0]
        end = nowDate
        X = pd.date_range(start, end, freq='D')
        Y = pd.Series(ydata[value_to_plot], index=X)
        item_data = masterdata.query(qstring.format(code))
        data = convertSerieToDataArray(Y)
        if movingAvg>1:
            data = convertSerieToDataArray(Y.fillna(0.0).rolling(window=movingAvg).mean())
        result[code] = {"itemname": item_data.itemname.tolist()[0],"data": data}
    return json.dumps(result[itemcode])

  def getSalesForItemAtDate(self, itemcode, isoDate):
    qry="""select t1.cardname, t0.quantity, t0.targettype from dbo.inv1 
    t0 join dbo.oinv t1 on t1.docentry=t0.docentry and t1.docdate='{}' 
    where t0.itemcode='{}'""".format(isoDate,itemcode)
    df = self.execute_query(qry)
    df = df.query("targettype == -1")
    index_fields=["cardname"]
    values_fields=["quantity"]
    columns_fields=[]
    pvdf = pd.pivot_table(df, index=index_fields,values=values_fields, columns=columns_fields,aggfunc=[np.sum], fill_value=0)
    outputDf=pd.DataFrame(pvdf.to_records())
    columns_renamed={"('sum', 'quantity')":'quantity'}
    outputDf.rename(columns=columns_renamed, inplace=True)
    return outputDf

  def compute_sales_for_itemcodes(self, itemcodes, periods):
    queries = [build_query_over(build_period(y,m))(itemcodes) for (y,m) in periods.items()]
    df=pd.concat(map(lambda q: self.execute_query(q),queries))
    index_fields = ["cardname"]
    values_fields = ['quantity']
    columns_fields = ['year','month']
    pvtable = pd.pivot_table(df, index=index_fields,
       values=values_fields,
       columns=columns_fields,
       aggfunc=[np.sum],
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
    result_df = df1.append(pd.Series([ALL_CLIENTS_LABEL]+[df1[x].sum() for x in cols], index=["cardname"]+cols), ignore_index=True)
    result_df["freq"]=result_df.loc[:,date_labels].gt(0).sum(axis=1)
    past_months=dates[1:]
    result_df["moy"]=result_df.loc[:,past_months].replace(0, np.nan).mean(axis=1, skipna=True)
    result_df["remplis."]=(result_df[dates[0]]-result_df["moy"])/result_df["moy"]
    result_df=result_df.sort_values(["freq","total"], ascending=[0,0])

    output_cols = ["cardname","total","freq"]+dates+["moy", "remplis."]
    return result_df.loc[:, output_cols]

dao = SapDao()


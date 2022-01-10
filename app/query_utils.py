import itertools

def build_query_cash(itemcodes, year, months):
    sql_params={
    'fields':["t0.docdate","t1.itemcode","sum(t1.quantity) as quantity","sum(t1.linetotal) as linetotal", "t1.targettype"],
    'tables':"dbo.inv1 t1",
    'where':["t1.itemcode in ({itemcodes})"],
    'join':{"dbo.oinv t0":('1',["t0.docentry=t1.docentry","year(t0.docdate)='{year}'","month(t0.docdate) in {months}"]),
             "ocrd _ocrd":('2', ["_ocrd.cardcode=t0.cardcode","(_ocrd.qrygroup1='Y' or _ocrd.qrygroup18='Y')"])},
    'groupby':"t0.docdate, t1.itemcode ,t1.targettype",
    }
    months_string="("+",".join(["'{}'".format(str(m)) for m in months])+")"
    stmt=querybuilder(sql_params).format_map({'itemcodes':itemcodes, 'year':year,'months':months_string})
    return stmt

def build_query_over(period):
    sql_params={
    'fields':["year(t1.docdate) as year","month(t1.docdate) as month","datepart(wk, t1.docdate) as week"
             ,"t1.docdate", "t1.doctime", "t0.itemcode", "t0.dscription as itemname", "t0.quantity", "t1.cardname"
             ,"t0.linetotal", "t1.docnum"],
    'tables':"dbo.inv1 t0",
    'where':["t0.itemcode in ({itemcodes})"],
    'join':{"dbo.oinv t1":('1',["t0.docentry=t1.docentry","year(t1.docdate)='{year}'","month(t1.docdate) in ({months})"]),
             "dbo.ocrd _ocrd":('2', ["_ocrd.cardcode=t1.cardcode","(_ocrd.qrygroup1='Y' or _ocrd.qrygroup18='Y')"])},
    }
    def format_with_itemcodes(itemcodes):
        stmt=querybuilder(sql_params).format_map({'itemcodes':itemcodes, 'year':period['year'],'months':period['months']})
        return stmt
    return format_with_itemcodes

def querybuilder(params):
    keys=['fields', 'tables', 'join','leftjoin', 'where', 'groupby','orderby']
    keywords={'fields':'{}','tables':'from {}', 'join':'join {} on {}', 'where':'where {}',
    'groupby':'group by {}', 'orderby':'order by {}', 'leftjoin':"left join {} on {}"}
    query_tpl="select"
    for k in keys:
        if k in params:
            if k=='join' or k=='leftjoin' :
                for t in sorted(params[k], key=params[k].__getitem__):
                    on=" and ".join(params[k][t][1])
                    table=t
                    query_tpl = " ".join([query_tpl, keywords[k].format(table,on)])
            elif k=='where':
                where=" and ".join(params[k])
                query_tpl=" ".join([query_tpl, keywords[k].format(where)])
            elif k=='fields':
                fields=",".join(params[k])
                query_tpl=" ".join([query_tpl, fields])
            else:
                query_tpl=" ".join([query_tpl, keywords[k].format(params[k])])
    return query_tpl

def reduit(x,y):
    if x-y>0:
        return x-y
    else:
        return x
def convertSerieToDataArray(serie):
    # convert timestamp to unix time and round value to 2 decimals
    # 
    return [[int(x.value/10**6),float(round(y,2)) if y==y else None] for x,y in serie.iteritems()]

def compute_months_dict_betweenDates(fromDate, toDate):
    months={}
    fromYear = fromDate.year
    toYear = toDate.year
    nbYears = toYear-fromYear+1
    if nbYears==1:
        months[fromYear]=list(range(fromDate.month, toDate.month+1))
    elif nbYears==2:
        months[fromYear]=list(range(fromDate.month, 13))
        months[toYear]=list(range(1, toDate.month+1))
    elif nbYears>2:
        fullYears=list(range(fromYear+1, toYear))
        months[fromYear]=list(range(fromDate.month, 13))
        for year in fullYears:
            months[year]=list(range(1,13))
        months[toYear]=list(range(1, toDate.month+1))
    return months

def build_dict(keys):
    def with_values(values):
        return {k:v for (k,v) in zip(keys, values)}
    return with_values
def build_months_list(months):
    return ",".join(["'{}'".format(x) for x in months])

def build_period(year, months):
    # year data
    keys = ["year", "months"]
    aPeriod=build_dict(keys)
    return aPeriod([year, build_months_list(months)])

def toJoinedString(separator):
    def build_string(items_list):
        return separator.join(["'{}'".format(x) for x in items_list])
    return build_string

def build_date(y,m,d):
    return dt.datetime(y,m,d).date()

def build_pivot_labels(fields, years, months):
    return ["('sum', '{}', {}, {})".format(x[0], x[1], x[2])for x in itertools.product(fields, years, months)]

def get_first_values(dataframe, column):
    return dataframe[column].values.tolist()[0]
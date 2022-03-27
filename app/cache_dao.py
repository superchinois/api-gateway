# coding: utf-8
import pymongo
import pandas as pd
import datetime as dt


class CacheDao:
    keys = ["MONGO_URI", "MONGO_DATABASE", "MONGO_COLLECTION"]
    URI, DB, COLLECTION = keys

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

    def last_docnum(from_date):
        DATE_FMT="%Y-%m-%d"
        start_date=dt.datetime.strptime(from_date,DATE_FMT)
        pipeline = [
            {"$match":{"docdate":{"$gt":start_date}}},
            {"$project":{"docnum":1, "week_num":{"$isoWeek":"$docdate"}}},
            {"$sort":{"docnum":pymongo.DESCENDING}},
            {"$limit":1}
        ]
        options={}
        return pipeline, options

cache_dao = CacheDao()

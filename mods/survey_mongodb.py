from datetime import datetime as dt
from time import time
from pymongo import MongoClient
import pandas as pd
import resource
import gc

class MongoSurvey:
    def __init__(self):
        self.DB_USER = 'db-user'
        self.DB_PASS = 'Passw0rd'
        self.DB_NAME = 'TEST_DB'
        self.TABLE_NAME = 'TEST_TABLE'
        self.MONGO_IP = 'XXX.XXX.XXX'
        try:
            client = MongoClient(f"mongodb://{self.DB_USER}:{self.DB_PASS}@{self.MONGO_IP}:27017")
            db = client[f"{self.TABLE_NAME}"]
            self.collection = db[f"{self.TABLE_NAME}"]

        except:
            print("Error: not connect mongo") 
        # #メモリ制約
        resource.setrlimit(resource.RLIMIT_DATA, (19 * 1024 ** 3, -1))
        # 書き換わっていることを確認
        print('memory limit',resource.getrlimit(resource.RLIMIT_DATA))

    def insert_mongo_db(self,df):
        dummy_df = df.copy()
        dummy_df['timestamp'] = dummy_df['timestamp'].apply(lambda s: dt.fromisoformat(s))
        dummy_data = dummy_df
        del df
        del dummy_df
        gc.collect()
        print('dummy_data\n',dummy_data)
        
        mongo_insert_start = time()
        try:
            self.collection.insert_many(dummy_data.to_dict('records'))
        except:
            print("Error: not insert mongo") 
        
        mongo_insert_end = time()
        mongo_insert_diff= mongo_insert_start - mongo_insert_end
        print('mongodbのinsert経過時間\n',mongo_insert_diff,'秒\n')


    def search_mongo_db(self):
        mongo_search_start = time()
        try:
            rows = self.collection.find()
        except:
            print("Error: not searh mongo") 
        
        mongo_search_end = time()
        mongo_search_diff= mongo_search_start - mongo_search_end
        print('mongodbのsearch経過時間\n',mongo_search_diff,'秒\n')
        print('mongodb取得最終行データ表示\n',rows[10000000])
        rows_count = self.collection.count_documents({})
        print('mongodb取得データ件数\n',rows_count)
        

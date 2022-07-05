import os
import pymongo
import pandas as pd
from config import Config

MONGODB_CONNECTION_STRING = Config.MONGODB['CONNECTION_STRING']


class DB:
    def __init__(self,database):
        self.MONGODB_CONNECTION_STRING = MONGODB_CONNECTION_STRING
        self.mongo_client = self.client()
        self.database = database

    def find_all(self, collection):
        col = self.mongo_client[self.database][collection]
        cursor = col.find({},{'_id':0})
        df = pd.DataFrame(list(cursor))
        return df

    def query(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        cursor = col.find(cond,{'_id':0}) #find by condition (dictionary type)
        df = pd.DataFrame(list(cursor))
        return df

    def text_score(self, collection, field , text):
        col = self.mongo_client[self.database][collection]
        col.create_index([(field, pymongo.TEXT)])
        cursor = col.find({'$text':{'$search':text}},{'score':{'$meta':'textScore'}})
        df = pd.DataFrame(list(cursor))
        df = df.drop(['_id'],axis=1)
        return df

    def query_where(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        cursor = col.find().where(cond) #find by where condition which cond is javascript functio
        df = pd.DataFrame(list(cursor))
        df = df.drop(['_id'],axis=1)
        return df

    def insert(self, collection, list_dict):
        col = self.mongo_client[self.database][collection]
        col.insert_many(list_dict) #insert list of dictionary per document 
       
    
    def insert_one(self, collection, list_dict):
        col = self.mongo_client[self.database][collection]
        df = col.insert_one(list_dict) #insert list of dictionary per document 
        dbid = (df.inserted_id)

        return dbid

    def delete(self, collection, cond):
        col = self.mongo_client[self.database][collection]
        col.delete_many(cond)

    def update(self, collection, cond, newvalue):
        col = self.mongo_client[self.database][collection]
        col.update_many(cond, {'$set':newvalue}) # Ex. {"name" : {"$regex":"^J"}},{"signature":"j","_id":3}

    def client(self):
        """
        create mongoDB client object
        """
        mc = pymongo.MongoClient(self.MONGODB_CONNECTION_STRING)
        return mc

if __name__ == "__main__":
    ROOT_USERNAME = ""
    ROOT_PASSWORD = ""
    # DB_IP = 
    # DB_PORT = 
    # db = DB('test_by_paint')
    # df = db.find_all('greeting')
    # print("\nfind_all\n",df)
    # df = db.query('greeting',{'signature':'Nuzuto'})
    # print("\nquery\n",df)
    # df = db.text_score('greeting','signature','Nuzuto')
    # print("\ntext_score\n",df)
    # df = db.query_where('greeting','String(this._id).length > String(this.signature).length')
    # print("\nquery_where\n",df)
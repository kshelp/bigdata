from pymongo import MongoClient  # pip install "pymongo[srv]"
import os

# 생성자
mongo=MongoClient(os.getenv("MONGO_CLUSTER_URI"))
print('/** Mongo Client: {0}'.format(mongo))

# server_info() 메서드: 연결된 서버의 정보를 출력한다.
server_info = mongo.server_info()
print('/** Server Info: {0}'.format(server_info))

# 데이터베이스 생성
database = 'mongodb'
collection = 'mongocoll'

#student = { 'name':'이철희', 'address':'Seoul', 'course':'Physics', 'score':77 }
#mongo[database][collection].insert_one(student)

student = [
    { 'name':'이철희', 'address':'Seoul', 'course':'Physics', 'score':77 },
    { 'name':'이영희', 'address':'Busan', 'course':'Biology', 'score':83 },
    { 'name':'홍길동', 'address':'Gwangju', 'course':'Chemistry', 'score':90 },
    { 'name':'유재석', 'address':'Daegu', 'course':'Statistics', 'score':95 }
]

mongo[database][collection].insert_many(student)

list_db = mongo.list_database_names()
print('database list: {0}'.format(list_db))

list_coll = mongo[database].list_collection_names()
print('collection list: {0}'.format(list_coll))

mongo.close()
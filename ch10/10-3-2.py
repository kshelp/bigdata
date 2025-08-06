from pymongo import MongoClient  # pip install "pymongo[srv]"
import os

# 생성자
mongo=MongoClient(os.getenv("MONGO_CLUSTER_URI"))

db = mongo['mongodb']
coll = db['mongocoll']

# update_one, update_many: 해당 도큐먼트의 요소값을 갱신한다.
coll.update_one({'name':'이철희'}, {'$set': {'course': 'Athletics'}})
coll.update_many({'address':'Seoul'}, {'$inc': {'score':-10}})

doc_list = coll.find()
for doc in doc_list:
    print(doc)

mongo.close()
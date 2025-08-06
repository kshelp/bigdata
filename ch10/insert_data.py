from pymongo import MongoClient
import os
import pandas as pd
from datetime import datetime

# MongoDB 연결
mongo = MongoClient(os.getenv("MONGO_CLUSTER_URI"))
db = mongo["etlmongodb"]

# 테스트용 샘플 데이터 생성
sample_data = [
    {"state": "CA", "date": datetime(2024, 1, 1), "value": 100},
    {"state": "CA", "date": datetime(2024, 2, 1), "value": 110},
    {"state": "TX", "date": datetime(2024, 1, 1), "value": 90},
    {"state": "TX", "date": datetime(2024, 2, 1), "value": 95},
]

# 컬렉션 이름들
collections = [
    "coll_earn_Construction_month",
    "coll_earn_Financial_Activities_month",
    "coll_earn_Goods_Producing_month",
    "coll_earn_Leisure_and_Hospitality_month",
    "coll_earn_Manufacturing_month",
    "coll_earn_Private_Service_Providing_month",
    "coll_earn_Professional_and_Business_Services_month",
    "coll_earn_Trade_Transportation_and_Utilities_month",
    "coll_unempl_month",
]

# 각 컬렉션에 동일한 데이터 삽입
for coll_name in collections:
    db[coll_name].delete_many({})  # 기존 데이터 초기화
    db[coll_name].insert_many(sample_data)
    print(f"✅ {coll_name}에 데이터 삽입 완료.")


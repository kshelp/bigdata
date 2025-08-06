1. 자바 실습 코드인 ‘ETL-Stream-Java’와 파이썬 실습 코드인 ‘ETL-Stream-Python’의 프로젝트 안에는 모두 ‘outputs’ 폴더가 있는데,
프로젝트를 실행하면 자동으로 생기는 csv 파일을 저장하는 폴더이며, FRED 내에서 계속 업데이트되어 시간이 흐를수록 파일 내용도 고정되어 있지 않고 바뀌기에 참고용으로만 사용하시기 바랍니다.


2. FRED에서 특정 키워드에 대해 서비스를 단종(DISCONTINUED)하였고, 단종된 키워드는 다음과 같습니다:
Average Hourly Earnings of All Employees: Education and Health Services


이에 단종으로 인하여 더 이상 시계열 데이터를 제공하지 않는 코드를 수정해야 하고 대상 파일은 다음과 같습니다:
ETL-Stream-Java                       ETL-Stream-Python                         Deep-Learn-Python
=====================================================================================================
EtlFileUploader2Hdfs.java           etl_file_uploader_hdfs.py                     mongo_feat.py
EtlDataUploader2Kafka.java          etl_data_uploader_kafka.py                 feat_extract_mongo.py
EtlDataUploader2MongoDB.java        etl_data_uploader_mongo.py


위 키워드에 대한 시계열 값이 반환되지 않으므로 해당 키워드가 들어간 코드 행을 주석 처리하여 수정하여야 함을 알려드립니다.


제이펍 드림
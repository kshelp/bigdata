import os
from hdfs_kafka import Hdfs2Kafka

if __name__ == '__main__':
    kafka = Hdfs2Kafka()

    # 현재 파일 기준으로 resources 디렉토리 경로 설정
    base_path = os.path.join(os.path.dirname(__file__), 'outputs')

    def full_path(filename):
        return os.path.join(base_path, filename)

    kafka.getHdFileInfo(full_path('unemployee_annual.csv'))
    list_data = kafka.readHdFile(full_path('unemployee_annual.csv'))
    kafka.sendData2Kafka('topic_unempl_ann', list_data)

    kafka.getHdFileInfo(full_path('household_income.csv'))
    list_data = kafka.readHdFile(full_path('household_income.csv'))
    kafka.sendData2Kafka('topic_house_income_ann', list_data)

    kafka.getHdFileInfo(full_path('tax_exemption.csv'))
    list_data = kafka.readHdFile(full_path('tax_exemption.csv'))
    kafka.sendData2Kafka('topic_tax_exemp_ann', list_data)

    kafka.getHdFileInfo(full_path('civilian_force.csv'))
    list_data = kafka.readHdFile(full_path('civilian_force.csv'))
    kafka.sendData2Kafka('topic_civil_force_ann', list_data)

    kafka.getHdFileInfo(full_path('poverty.csv'))
    list_data = kafka.readHdFile(full_path('poverty.csv'))
    kafka.sendData2Kafka('topic_pov_ann', list_data)

    kafka.getHdFileInfo(full_path('real_gdp.csv'))
    list_data = kafka.readHdFile(full_path('real_gdp.csv'))
    kafka.sendData2Kafka('topic_gdp_ann', list_data)

    kafka.getHdFileInfo(full_path('unemployee_monthly.csv'))
    list_data = kafka.readHdFile(full_path('unemployee_monthly.csv'))
    kafka.sendData2Kafka('topic_unempl_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Construction.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Construction.csv'))
    kafka.sendData2Kafka('topic_earn_Construction_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Financial_Activities.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Financial_Activities.csv'))
    kafka.sendData2Kafka('topic_earn_Financial_Activities_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Goods_Producing.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Goods_Producing.csv'))
    kafka.sendData2Kafka('topic_earn_Goods_Producing_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Leisure_and_Hospitality.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Leisure_and_Hospitality.csv'))
    kafka.sendData2Kafka('topic_earn_Leisure_and_Hospitality_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Manufacturing.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Manufacturing.csv'))
    kafka.sendData2Kafka('topic_earn_Manufacturing_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Private_Service_Providing.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Private_Service_Providing.csv'))
    kafka.sendData2Kafka('topic_earn_Private_Service_Providing_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Professional_and_Business_Services.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Professional_and_Business_Services.csv'))
    kafka.sendData2Kafka('topic_earn_Professional_and_Business_Services_mon', list_data)

    kafka.getHdFileInfo(full_path('earnings_Trade_Transportation_and_Utilities.csv'))
    list_data = kafka.readHdFile(full_path('earnings_Trade_Transportation_and_Utilities.csv'))
    kafka.sendData2Kafka('topic_earn_Trade_Transportation_and_Utilities_mon', list_data)

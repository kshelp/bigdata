from kafka import KafkaConsumer

if __name__ == '__main__':
    # 사용할 토픽 이름 (원하는 값으로 변경 가능)
    #topic = 'topic_unempl_mon'
    topic = 'topic_unempl_ann'

    '''
    사용 가능한 토픽 목록:
    topic_unempl_ann
    topic_house_income_ann
    topic_tax_exemp_ann
    topic_civil_force_ann
    topic_pov_ann
    topic_gdp_ann
    topic_unempl_mon
    topic_earn_Construction_mon
    topic_earn_Education_and_Health_Services_mon
    topic_earn_Financial_Activities_mon
    topic_earn_Goods_Producing_mon
    topic_earn_Leisure_and_Hospitality_mon
    topic_earn_Manufacturing_mon
    topic_earn_Private_Service_Providing_mon
    topic_earn_Professional_and_Business_Services_mon
    topic_earn_Trade_Transportation_and_Utilities_mon
    '''

    # KafkaConsumer 설정
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers='localhost:9092',
        group_id='test-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode('utf-8')  # 메시지 디코딩
    )

    print(f"✅ Subscribed to topic: {topic}")

    try:
        for msg in consumer:
            print(msg.value)
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    finally:
        consumer.close()
        print("👋 Kafka consumer closed.")

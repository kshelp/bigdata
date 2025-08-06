from kafka import KafkaConsumer
import json

# Kafka 브로커 주소 (Ubuntu VM의 IP 주소와 Kafka 포트)
# <Ubuntu_VM_IP_주소> 부분을 위에서 확인한 VM의 실제 IP 주소로 변경하세요.
KAFKA_BROKER = '192.168.127.131:9092'
KAFKA_TOPIC = 'my_topic' # 프로듀서와 동일한 토픽 이름
CONSUMER_GROUP_ID = 'my_python_consumer_group' # 컨슈머 그룹 ID

print(f"Connecting to Kafka broker at: {KAFKA_BROKER}")

try:
    # Kafka Consumer 인스턴스 생성
    # value_deserializer를 사용하여 JSON 메시지를 역직렬화합니다.
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        auto_offset_reset='earliest', # 'earliest'는 처음부터 읽기, 'latest'는 최신 메시지부터 읽기
        enable_auto_commit=True,
        group_id=CONSUMER_GROUP_ID,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Listening for messages on topic '{KAFKA_TOPIC}' in group '{CONSUMER_GROUP_ID}'...")

    # 메시지 수신 및 처리
    for message in consumer:
        print(f"Received message: Partition={message.partition}, Offset={message.offset}, Value={message.value}")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'consumer' in locals() and consumer is not None:
        consumer.close()
        print("Consumer closed.")
from kafka import KafkaProducer
import json
import time

# Kafka 브로커 주소 (Ubuntu VM의 IP 주소와 Kafka 포트)
# <Ubuntu_VM_IP_주소> 부분을 위에서 확인한 VM의 실제 IP 주소로 변경하세요.
KAFKA_BROKER = '192.168.127.131:9092'
KAFKA_TOPIC = 'my_topic' # 사용할 Kafka 토픽 이름

print(f"Connecting to Kafka broker at: {KAFKA_BROKER}")

try:
    # Kafka Producer 인스턴스 생성
    # value_serializer를 사용하여 메시지를 JSON 형태로 직렬화합니다.
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # 메시지 전송
    for i in range(5):
        message = {'number': i, 'message': f'Hello Kafka from Python! {i}'}
        print(f"Sending message: {message}")
        producer.send(KAFKA_TOPIC, value=message)
        time.sleep(1) # 1초 간격으로 메시지 전송

    # 모든 메시지가 전송될 때까지 기다립니다.
    producer.flush()
    print("Messages sent successfully!")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if 'producer' in locals() and producer is not None:
        producer.close()
        print("Producer closed.")
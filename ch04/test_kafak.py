# test_kafka.py
#from confluent_kafka import Producer
#conf = {'bootstrap.servers': 'localhost:9092'}
#producer = Producer(conf)
#print("Kafka Producer 생성 성공")

from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
print("Kafka Producer 생성 성공")

producer.send('test-topic', b'Hello, Kafka!')
producer.flush()



from pyarrow import fs
import configparser
import os
import subprocess
from kafka import KafkaProducer
from kafka.errors import KafkaError


class Hdfs2Kafka(object):
    '''
    HDFS에서 데이터를 읽어 Kafka로 전송하는 클래스
    '''

    def __init__(self):
        '''
        생성자: 환경설정 및 KafkaProducer 초기화
        '''
        classpath = subprocess.Popen(["/home/ubuntu/hadoop/bin/hdfs", "classpath", "--glob"], stdout=subprocess.PIPE).communicate()[0]

        os.environ["CLASSPATH"] = classpath.decode("utf-8")
        os.environ["ARROW_LIBHDFS_DIR"] = "/home/ubuntu/hadoop/lib/native"

        self._hdfs = fs.HadoopFileSystem('localhost', port=9000)

        # Kafka 설정
        config = configparser.ConfigParser()
        config.read('resources/SystemConfig.ini')
        kafka_brokers = config['KAFKA_CONFIG']['kafka.brokerlist']
        
        # kafka-python은 auto.offset.reset이 consumer용이므로 제거
        self._producer = KafkaProducer(
            bootstrap_servers=kafka_brokers.split(','),
            value_serializer=lambda v: v.encode('utf-8')
        )

    def getHdFileInfo(self, filename):
        f_Info = self._hdfs.get_file_info(filename)
        print('파일 종류 : ' + str(f_Info.type))
        print('파일 경로 : ' + str(f_Info.path))
        print('파일 크기 : ' + str(f_Info.size))
        print('파일 수정 일자 : ' + str(f_Info.mtime))

    def readHdFile(self, filename):
        with self._hdfs.open_input_file(filename) as inf:
            read_data = inf.read().decode('utf-8').splitlines()
            newline = [line.split(",") for line in read_data]
            return newline

    def sendData2Kafka(self, topic, list_line):
        for data in list_line:
            str_tmp = ",".join(data).split(",")
            modified_data = ",".join(str_tmp[:2]) + "," + ",".join(str_tmp[4:])
            print(modified_data)

            # Kafka 전송
            future = self._producer.send(topic, value=modified_data)
            future.add_callback(kafka_producer_callback)
            future.add_errback(kafka_producer_errback)

        self._producer.flush()


def kafka_producer_callback(record_metadata):
    print("전달된 메시지: {} [{}] @ {}\n".format(record_metadata.topic, record_metadata.partition, record_metadata.offset))
    # timestamp, key, value 등은 kafka-python에서는 보내는 쪽에서 명시해야 함

def kafka_producer_errback(excp):
    print('Kafka 전송 실패:', excp)

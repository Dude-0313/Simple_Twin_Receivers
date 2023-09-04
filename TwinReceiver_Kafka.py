# -*- coding: utf-8 -*-
"""
Created on Fri Jun 16 12:52:15 2023
Twin Reciver - Kafka version
@author: kulje
"""

from kafka import KafkaConsumer, consumer
from time import sleep
import json

#BROKER = '172.19.0.2:9092'
BROKER = '192.168.1.20:9092'
TOPIC_1 = 'telemetry-motor-1'
TOPIC_2 = 'telemetery-motor-2'
GROUP_ID = 'twin-consumer'

class TwinConsumer: 
    broker =""
    topic=""
    consumer_grpid=""
    logger= None
    
    def __init__(self, broker, topic, group_id):
        self.broker =broker
        self.topic = topic
        self.consumer_grpid=group_id
    
    def activate_listener(self):
        consumer = KafkaConsumer(bootstrap_servers=self.broker,
                                 group_id=self.consumer_grpid,
                                 consumer_timeout_ms=60000,
                                 auto_offset_reset='earliest',
                                 enable_auto_commit=False,
                                 value_deserializer= lambda m: json.loads(m.decode('ascii')))
        consumer.subscribe(self.topic)
        print("Consumer Ready...")
        try:
            for message in consumer:
               print("Message : ",message)
               consumer.commit()
        except KeyboardInterrupt:
            print("Aborted..")
        finally:
            consumer.close()
            
def main():
    consumer1 = TwinConsumer(BROKER, TOPIC_1, GROUP_ID)
    consumer1.activate_listener()
    consumer2 = TwinConsumer(BROKER, TOPIC_2, GROUP_ID)
    consumer2.activate_listener()


if __name__ =='__main__' :
        main()

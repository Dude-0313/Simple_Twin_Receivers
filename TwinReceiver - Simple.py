# -*- coding: utf-8 -*-
"""
Created on Tue May 30 13:41:10 2023

Twin Receiver : Receives telemetry from twin motor

@author: kulje
"""
import pika, sys, os

TWN_XCHNG_NAME = 'twinExchange'
TWN_QUEUE_NAME = 'twinMotor.telemetry'

def callback(ch, method, properties,body):
    print('[x] Received : %r' % body.decode())

def main():
    credentials = pika.PlainCredentials('twinuser', 'twinpass')
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='bella',credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=TWN_QUEUE_NAME)
    channel.basic_consume(queue=TWN_QUEUE_NAME,on_message_callback=callback,auto_ack=True)
    print('[x] Waiting from telemetry...')
    channel.start_consuming()

if __name__ =='__main__' :
    try:
        main()
    except KeyboardInterrupt:
        print('Exiting...')
        try :
            sys.exit(0)
        except SystemExit:
            os._exit(0)
        
    

# -*- coding: utf-8 -*-
"""
Created on Fri Jun  2 11:24:13 2023
Twin receiver based on PyAMQP library(based on pika)
@author: kulje
"""

from pyamqp.rabbit.receiver import Receiver

TWN_XCHNG_NAME = 'twinExchange'
TWN_QUEUE_NAME = 'twinMotor.telemetry'
TWN_ROUTING_PREFIX = 'motor_'

receiver_instance = Receiver(host='bella',username='twinuser',password='twinpass')

receiver_instance.connect_queue(queue_name=TWN_QUEUE_NAME,
                                exchange=TWN_XCHNG_NAME,
                                routing_keys=[TWN_ROUTING_PREFIX+'1',TWN_ROUTING_PREFIX+'2'],
                                is_durable=False,
                                auto_delete=False)

#callback
def get_message(message,details):
    print(message)
    print(details)

receiver_instance.consume(callback_function=get_message,
                          no_ack=True)        


#!/usr/bin/env python
import time
import boto3
import logging
from multiprocessing import Process
from python_sqs_consumer.processor import SQSMessagesProcessor

logger = logging.getLogger(__name__)


class SQSConsumerPool(object):
    workers_pool = None

    def __init__(self, max_threads, processor_class, queue_name):
        self.max_threads = max_threads

        self.workers_pool = [None] * self.max_threads
        self.processor_class = processor_class
        self.queue_name = queue_name

        class _SQSMessagesProcessor(SQSMessagesProcessor):
            def __init__(self):
                self.processor_class = processor_class

        self.processor_sqs_class = _SQSMessagesProcessor

    def run(self):
        while True:
            for index, worker in enumerate(self.workers_pool):
                if worker is None or not worker.is_alive():
                    consume = self.consume
                    connect = self.connect
                    worker = Process(target=consume, args=(connect()))
                    worker.start()
                    self.workers_pool[index] = worker

                time.sleep(1)

    def connect(self):
        conn_sqs = boto3.resource('sqs')
        queue = conn_sqs.get_queue_by_name(QueueName=self.queue_name)
        return queue

    def consume(self, queue):
        processor_sqs = self.processor_sqs_class()
        logger.debug('Consumer waiting for incomming message...')
        if queue is not None:

            messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
            processor_sqs.process(messages)

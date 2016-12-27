#!/usr/bin/env python
import time
import boto3
import logging
from multiprocessing import Process

logger = logging.getLogger(__name__)


class SQSConsumerPool(object):
    workers_pool = None

    def __init__(
            self, max_threads, processor_class, queue_name, max_number_of_messages=1, wait_time_seconds=10):
        self.max_threads = max_threads

        self.workers_pool = [None] * self.max_threads
        self.processor_class = processor_class
        self.queue_name = queue_name
        self.max_number_of_messages = max_number_of_messages
        self.wait_time_seconds = wait_time_seconds

    def run(self):
        while True:
            for index, worker in enumerate(self.workers_pool):
                if worker is None or not worker.is_alive():
                    consume = self.consume
                    connect = self.connect
                    worker = Process(target=consume, args=(connect(),))
                    worker.start()
                    self.workers_pool[index] = worker

                time.sleep(1)

    def connect(self):
        conn_sqs = boto3.resource('sqs')
        queue = conn_sqs.get_queue_by_name(QueueName=self.queue_name)
        return queue

    def consume(self, queue):
        processor = self.processor_class()
        logger.debug('Consumer waiting for incomming message...')
        if queue is not None:

            messages = queue.receive_messages(
                MaxNumberOfMessages=self.max_number_of_messages, WaitTimeSeconds=self.wait_time_seconds)
            processor.process(messages)

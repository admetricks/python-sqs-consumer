#!/usr/bin/env python
import time
import boto3
import logging
from multiprocessing import Process

logger = logging.getLogger(__name__)


class SQSConsumerPool(object):
    workers_pool = None

    def __init__(self, max_threads, processor_class, queue_name):
        self.max_threads = max_threads

        self.workers_pool = [None] * self.max_threads
        self.processor_class = processor_class
        self.queue_name = queue_name

    def run(self):
        while True:
            for index, worker in enumerate(self.workers_pool):
                if worker is None or not worker.is_alive():
                    worker = Process(target=self.consume, args=(self.connect(), self.processor_class()))
                    worker.start()
                    self.workers_pool[index] = worker

                time.sleep(1)

    def connect(self):
        conn_sqs = boto3.resource('sqs')
        queue = conn_sqs.get_queue_by_name(QueueName=self.queue_name)
        return queue

    def consume(self, queue, processor):
        logger.debug('Consumer waiting for incomming message...')
        if queue is not None:

            messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=10)
            if not messages:
                return

            logger.debug('Processing!...{}'.format(messages))

            if not len(messages) == 0:
                for message in messages:
                    processor.process(message)

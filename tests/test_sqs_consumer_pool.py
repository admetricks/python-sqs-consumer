import random
import time
import unittest
from unittest.mock import patch, call
from unittest.mock import MagicMock
from python_sqs_consumer.processor import S3Processor
from python_sqs_consumer.processor import SQSMessageProcessor
from python_sqs_consumer.processor import SQSMessagesProcessor
from python_sqs_consumer.sqs_consumer_pool import SQSConsumerPool


def unique_id(name):
    """
    Generate a unique ID that includes the given name,
    a timestamp and a random number. This helps when running
    integration tests in parallel that must create remote
    resources.
    """
    return '{0}-{1}-{2}'.format(name, int(time.time()),
                                random.randint(0, 10000))


class TestSQSConsumerPool(unittest.TestCase):
    def setUp(self):
        self.queue_name = unique_id('boto3-test')

    def test_init(self):
        max_threads = MagicMock()
        processor_class = MagicMock()
        queue_name = MagicMock()
        consumer = SQSConsumerPool(max_threads=max_threads, processor_class=processor_class, queue_name=queue_name)
        assert consumer.max_threads == max_threads
        assert consumer.queue_name == queue_name
        assert consumer.processor_class == processor_class

    def test_cunsume(self):
        message = MagicMock()
        queue = MagicMock()
        queue.receive_messages = MagicMock(return_value=[message])

        consumer = SQSConsumerPool(max_threads=1, processor_class=SQSMessagesProcessor, queue_name=self.queue_name)

        with patch.object(SQSMessagesProcessor, 'processor_class', create=True) as mock_processor_class:
            consumer.consume(queue)

        mock_processor_class.assert_has_calls([
            call(),
            call().process(message)])

    def test_cunsume_empty_queue(self):
        queue = MagicMock()
        queue.receive_messages = MagicMock(return_value=[])

        consumer = SQSConsumerPool(max_threads=1, processor_class=SQSMessagesProcessor, queue_name=self.queue_name)

        with patch.object(SQSMessagesProcessor, 'processor_class', create=True) as mock_processor_class:
            consumer.consume(queue)

        assert mock_processor_class.called

    def test_cunsume_message(self):
        message = MagicMock()
        message.body = '{"Records":[{"s3":{"bucket":{"name":"name"},"object":{"key":"key"}}}]}'
        queue = MagicMock()
        queue.receive_messages = MagicMock(return_value=[message])

        class _SQSMessagesProcessor(SQSMessagesProcessor):
            def __init__(self):
                self.processor_class = SQSMessageProcessor

        consumer = SQSConsumerPool(max_threads=1, processor_class=_SQSMessagesProcessor, queue_name=self.queue_name)

        with patch.object(SQSMessageProcessor, 'processor_class', create=True) as mock_processor_class:
            consumer.consume(queue)

        mock_processor_class.assert_has_calls([
            call(),
            call().process({'Records': [{'s3': {'bucket': {'name': 'name'}, 'object': {'key': 'key'}}}]})])

    def test_cunsume_message_s3(self):
        message = MagicMock()
        message.body = '{"Records":[{"s3":{"bucket":{"name":"name"},"object":{"key":"key"}}}]}'
        queue = MagicMock()
        queue.receive_messages = MagicMock(return_value=[message])

        class _SQSMessageProcessor(SQSMessageProcessor):
            def __init__(self):
                self.processor_class = S3Processor

        class _SQSMessagesProcessor(SQSMessagesProcessor):
            def __init__(self):
                self.processor_class = _SQSMessageProcessor

        consumer = SQSConsumerPool(max_threads=1, processor_class=_SQSMessagesProcessor, queue_name=self.queue_name)

        with patch.object(S3Processor, 'processor_class', create=True) as mock_processor_class:
            consumer.consume(queue)

        mock_processor_class.assert_has_calls([
            call(),
            call().process({'s3': {'bucket': {'name': 'name'}, 'object': {'key': 'key'}}})])

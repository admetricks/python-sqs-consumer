import unittest
from unittest.mock import patch, call
from unittest.mock import MagicMock

from python_sqs_consumer.processor import S3Processor, S3RecordProcessor, SQSMessageProcessor, SQSMessagesProcessor, SNSRecordProcessor


class TestProcessor(unittest.TestCase):
    def test_s3_processor(self):
        mock = MagicMock()
        mock.body = '{"Records":[{"s3":{"bucket":{"name":"name"},"object":{"key":"key"}}}]}'
        processor = S3Processor()
        with patch.object(S3Processor, 'processor_class', create=True) as mock_processor_class:
            processor.process(mock)

        mock_processor_class.assert_has_calls([
            call(),
            call().process({'s3': {'bucket': {'name': 'name'}, 'object': {'key': 'key'}}})])

        mock.assert_has_calls([call.delete()])

    def test_s3_record_processor(self):
        body = {
            's3': {
                'bucket':
                    {'name': 'name'},
                'object':
                    {'key': 'key'}}}

        processor = S3RecordProcessor()

        with patch.object(S3RecordProcessor, 'processor_class', create=True) as mock_processor_class:
            processor.process(body)

        mock_processor_class.assert_has_calls([
            call(),
            call().process('name', 'key')])

    def test_processors(self):
        mock = MagicMock()
        mock.body = '{"Records":[{"s3":{"bucket":{"name":"name"},"object":{"key":"key"}}}]}'

        class Processor(S3Processor):
            def __init__(self):
                self.processor_class = S3RecordProcessor

        processor = Processor()

        with patch.object(S3RecordProcessor, 'processor_class', create=True) as mock_processor_class:
            processor.process(mock)

        mock_processor_class.assert_has_calls([
            call(),
            call().process('name', 'key')])

    def test_sqs_messages_processor(self):
        mock = [MagicMock(), MagicMock()]
        processor = SQSMessagesProcessor()
        with patch.object(SQSMessagesProcessor, 'processor_class', create=True) as mock_processor_class:
            processor.process(mock)

        mock_processor_class.assert_has_calls([
            call(),
            call().process(mock[0]),
            call().process(mock[1])])

    def test_sqs_message_processor(self):
        mock = MagicMock()
        mock.body = '{"Records":[{"s3":{"bucket":{"name":"name"},"object":{"key":"key"}}}]}'
        processor = SQSMessageProcessor()
        with patch.object(SQSMessageProcessor, 'processor_class', create=True) as mock_processor_class:
            processor.process(mock)

        mock_processor_class.assert_has_calls([
            call(),
            call().process({'Records': [{'s3': {'bucket': {'name': 'name'}, 'object': {'key': 'key'}}}]})])

        mock.assert_has_calls([call.delete()])

    def test_sns_record_processor(self):
        body = {
            'Message': '{"dummy":"value"}'}

        processor = SNSRecordProcessor()

        with patch.object(SNSRecordProcessor, 'processor_class', create=True) as mock_processor_class:
            processor.process(body)

        mock_processor_class.assert_has_calls([
            call(),
            call().process({'dummy': 'value'})])

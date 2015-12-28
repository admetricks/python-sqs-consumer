import json


class Processor(object):
    def process(self, body):
        raise NotImplementedError


class S3Processor(Processor):
    def process(self, message):
        processor = self.processor_class()

        if 'Records' in message:
            for m in message['Records']:
                processor.process(m)


class S3RecordProcessor(object):
    def process(self, message):
        processor = self.processor_class()
        s3 = message['s3']
        bucket = s3['bucket']['name']
        key = s3['object']['key']
        processor.process(bucket, key)


class SQSMessagesProcessor(Processor):
    def process(self, messages):
        processor = self.processor_class()
        if not messages:
            return

        if not len(messages) == 0:
            for message in messages:
                processor.process(message)


class SQSMessageProcessor(Processor):
    def process(self, message):
        processor = self.processor_class()

        body = json.loads(message.body)
        processor.process(body)

        message.delete()


class SNSRecordProcessor(Processor):
    def process(self, message):
        processor = self.processor_class()

        if 'Message' in message:
            message_content = json.loads(message['Message'])
            processor.process(message_content)

import json


class Processor(object):
    def process(self, body):
        raise NotImplementedError


class S3Processor(Processor):

    def process(self, message):
        processor = self.processor_class()

        body = json.loads(message.body)
        if 'Records' in body:
            for m in body['Records']:
                processor.process(m)

        message.delete()


class S3RecordProcessor(object):
    def process(self, message):
        processor = self.processor_class()
        s3 = message['s3']
        bucket = s3['bucket']['name']
        key = s3['object']['key']
        processor.process(bucket, key)

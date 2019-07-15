import os
import json

import boto3
from satstac import Collection

root_url = 'https://cognition-disaster-data.s3.amazonaws.com'

sqs = boto3.client('sqs')
account_name = boto3.client('sts').get_caller_identity().get('Account')

def find_items(collection_name, sensor_name=None):
    col = Collection.open(os.path.join(root_url, 'DGOpenData', collection_name, 'catalog.json'))
    for item in col.items():
        if sensor_name:
            if 'eo:platform' in item.properties:
                if item.properties['eo:platform'] == sensor_name:
                    yield item
        else:
            yield item

def rebuild_thumbnails(collection_name, sensor_name):
    for item in find_items(collection_name, sensor_name):
        sqs.send_message(
            QueueUrl=f'https://sqs.us-east-1.amazonaws.com/{account_name}/newThumbnailQueue',
            MessageBody=json.dumps(item.data)
        )

def rebuild_all_thumbnails(collection_name):
    for item in find_items(collection_name):
        sqs.send_message(
            QueueUrl=f'https://sqs.us-east-1.amazonaws.com/{account_name}/newThumbnailQueue',
            MessageBody=json.dumps(item.data)
        )
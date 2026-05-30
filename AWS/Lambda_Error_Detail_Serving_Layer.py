import json
import pandas as pd #type:ignore
from io import BytesIO
import boto3 #type:ignore
import logging
print('error')

#Write to cloud watch
logger = logging.getLogger()
logger.setLevel(logging.INFO)


s3 = boto3.client('s3')

def get_object_key_value(s3_event):

    bucket_name = s3_event['s3']['bucket']['name']
    key_name = s3_event['s3']['object']['key']

    return { 
            "bucket_name" : bucket_name ,
             "key_name" : key_name
             }

def get_content_from_s3(s3_object):

    content_obj = s3.get_object(
                                Bucket = s3_object['bucket_name'],
                                Key = s3_object['key_name']
                                )

    content_byte = content_obj['Body'].read()

    return content_byte


def get_content(content_bytes):

    order_error = pd.read_parquet(
                                    BytesIO(content_bytes),
                                    engine= 'pyarrow'
                                  )
    return order_error


def lambda_handler(event, context):
    
    for sqs_record in event['Records']:

        s3_events = json.loads(sqs_record['body'])

        for s3_event in s3_events['Records']:

            s3_object = get_object_key_value(s3_event)

            content_bytes = get_content_from_s3(s3_object)

            order_error = get_content(content_bytes)

            for record in order_error.to_dict("records"):
                # USER UI
                logging.info(f"The order {record['order_id']} is failed due to {record['transaction_error_description']}")


    return {
        'statusCode': 200,
        'body': json.dumps('Order Error')
    }

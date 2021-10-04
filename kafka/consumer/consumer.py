from kafka import KafkaConsumer
from json import loads
import threading
import json
import boto3
from decimal import *
import datetime
import sys
sys.path.append('/palego/redit/kafka')
sys.path.append('/palego/redit')
from globalconfig.base import *
from config.base import *

def load_to_dynamodb(id,created,url,selftext,upvote_ratio,author,author_premium,over_18,treatment_tags, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('palego_assignment')
    response = table.put_item(Item={'id': id,'created': datetime.datetime.fromtimestamp(created).isoformat(),'url': url,'selftext': selftext,'upvote_ratio': Decimal(str(upvote_ratio)),'author_detals': {'author_name': author, 'author_premium': author_premium=='True','over_18':over_18=='True'},'treatment_tags': treatment_tags})
    return response



def consume_msg_dynamodb(consumer):
    for message in consumer:
        message = message.value
        final_dictionary = json.loads(message)
        id=final_dictionary['id']
        created=final_dictionary['created']
        url=final_dictionary['url']
        selftext=final_dictionary['selftext']
        upvote_ratio=final_dictionary['upvote_ratio']
        author=final_dictionary['author']
        author_premium=final_dictionary['author_premium']
        over_18=final_dictionary['over_18']
        treatment_tags=final_dictionary['treatment_tags']
        load_resp = load_to_dynamodb(id,created,url,selftext,upvote_ratio,author,author_premium,over_18,treatment_tags)
        print('this msg is :'+'{}'.format(id))
def consume_msg_rds(consumer):
    for message in consumer:
        print('This is just to show example that we can create multiple consumer groups and consume message in many destinations like Dynamo, RDS, S3, Redshift for simplicity only dynamo db consumer has been covered  :'+'{}'.format(id))
def consume_msg_s3(consumer):
    for message in consumer:
        print('This is just to show example that we can create multiple consumer groups and consume message in many destinations like Dynamo, RDS, S3, Redshift for simplicity only dynamo db consumer has been covered  :'+'{}'.format(id))
if __name__ == "__main__":

    brokers=['b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.com:9092', 'b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.c                                                                                  om:9092']
# Define Kafka topic to be produced to
    kafka_topic=kafka_local_topic

# A Kafka client that consumes records from a Kafka cluster
    consumer1 = KafkaConsumer(
          kafka_topic,
          bootstrap_servers=brokers,
          auto_offset_reset='earliest',
          enable_auto_commit=True,
          group_id='my-group1',
          value_deserializer=lambda x: loads(x.decode('utf-8')))
    consumer2 = KafkaConsumer(
          kafka_topic,
          bootstrap_servers=brokers,
          auto_offset_reset='earliest',
          enable_auto_commit=True,
          group_id='my-group2',
          value_deserializer=lambda x: loads(x.decode('utf-8')))

     # creating thread for parellelism and consuming Kafka message from reddit api on many aws services like dynamodb,s3, rds, redshift
    t1 = threading.Thread(target=consume_msg_dynamodb, args=(consumer1,))
    t2 = threading.Thread(target=consume_msg_rds, args=(consumer2,))
    t3 = threading.Thread(target=consume_msg_s3, args=(consumer3,))

    t1.start()
    t2.start()
    t3.start()

        # wait until thread 1 is completely executed
    t1.join()
        # wait until thread 2 is completely executed
    t2.join()
    # wait until thread 2 is completely executed
    t3.join()
    

        # All three threads have been completely executed
    print("Done!")

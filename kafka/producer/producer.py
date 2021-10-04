from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
import sys
sys.path.append('/palego/redit/kafka')
sys.path.append('/palego/redit')
from globalconfig.base import *
from config.base import *

# Define Amazon MSK Brokers
#brokers=['b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.com:9092', 'b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.com:9092']
##Below variable defined in /palego/redit/kafka/base.py ###############
brokers=kafka_broker_list 
# Define Kafka topic to be produced to
kafka_topic=kafka_local_topic,
# A Kafka client that publishes records to the Kafka cluster
producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda x: dumps(x).encode('utf-8'))

import praw

reddit = praw.Reddit(
    client_id="CCXVctIyT5n9JHNStRYdHg",
    client_secret="gW3wfOTtlLVjnjAhgq9vtN7dCRiE0w",
    password="&myuio@@@123",
    user_agent="palego assignment by u/Chandan1987",
    username="chandan1987",
)
subreddit = reddit.subreddit("learnpython")
for submission in subreddit.hot(limit=100):
    dict={}
    dict['id']=submission.id
    dict['created']=submission.created
    dict['url']=submission.url
    dict['selftext']=submission.selftext
    dict['upvote_ratio']=submission.upvote_ratio
    dict['author']=str(submission.author)
    dict['author_premium']=submission.author_premium
    dict['over_18']=submission.over_18
    dict['treatment_tags']=submission.treatment_tags
    final_dictionary = json.dumps(dict)
    print(str(final_dictionary))
    sleep(1)

    producer.send(kafka_topic,value=str(final_dictionary))


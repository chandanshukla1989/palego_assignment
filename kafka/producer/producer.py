from time import sleep
from json import dumps
from kafka import KafkaProducer
import json
import sys
sys.path.append('palego/redit/kafka')
sys.path.append('palego/redit/globalconfig')

# Define Amazon MSK Brokers
brokers=['b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.com:9092', 'b-1.demo-cluster-1.ww4d4r.c7.kafka.us-east-2.amazonaws.com:9092']
# Define Kafka topic to be produced to
kafka_topic='AWSKafkaTopic'
# A Kafka client that publishes records to the Kafka cluster
producer = KafkaProducer(bootstrap_servers=brokers, value_serializer=lambda x: dumps(x).encode('utf-8'))
# To produce 1000 numbers from 0 to 999

import praw

reddit = praw.Reddit(
    client_id="CCXVctIyT5n9JHNStRYdHg",
    client_secret="gW3wfOTtlLVjnjAhgq9vtN7dCRiE0w",
    password="Comnet@123",
    user_agent="testscript by u/fakebot3",
    username="chandan1987",
)
subreddit = reddit.subreddit("learnpython")
for submission in subreddit.hot(limit=10):
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


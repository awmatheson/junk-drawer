import json
from random import randint
from time import sleep

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError

input_topic_name = "web_events"
localhost_bootstrap_server = "localhost:9092"
producer = KafkaProducer(bootstrap_servers=[localhost_bootstrap_server])
admin = KafkaAdminClient(bootstrap_servers=[localhost_bootstrap_server])

# Create input topic
try:
    input_topic = NewTopic(input_topic_name, num_partitions=3, replication_factor=1)
    admin.create_topics([input_topic])
    print(f"input topic {input_topic_name} created successfully")
except:
    print(f"Topic {input_topic_name} already exists")

# Add data to input topic
users = [
    {"user_id":"a12", "email":"joe@mail.com"}, 
    {"user_id":"a34", "email":"jane@mail.com"}, 
    {"user_id":"a56", "email":"you@mail.com"}, 
    {"user_id":"a78", "email":"the@mail.com"}, 
    {"user_id":"a99", "email":"employee@bytewax.io"}
    ]
event_types = [
    "click",
    "post",
    "comment",
    "login"
]
try:
    for i in range(500):
        event = users[randint(0,4)]
        event['type'] = event_types[randint(0,3)]
        event_ = json.dumps(event).encode()
        producer.send(input_topic_name, value=event_)
        sleep(0.1)
    print(f"input topic {input_topic_name} populated successfully")
except KafkaError:
    print("A kafka error occurred")
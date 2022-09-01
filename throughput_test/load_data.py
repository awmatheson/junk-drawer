import sys
from time import sleep

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

input_topic_name = "server_request_test"
localhost_bootstrap_server = "localhost:9092"
admin = AdminClient({'bootstrap.servers': localhost_bootstrap_server})
producer = Producer(**{'bootstrap.servers': localhost_bootstrap_server,
                       'queue.buffering.max.messages': 10000000,
                       'batch.num.messages': 500,
                       })


# Create input topic
input_topic = NewTopic(input_topic_name, num_partitions=1, replication_factor=1)
fs = admin.create_topics([input_topic])
for topic, f in fs.items():
    try:
        f.result()  # The result itself is None
        print("Topic {} created".format(topic))
    except Exception as e:
        print("Failed to create topic {}: {}".format(topic, e))

# Add data to input topic
def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))

with open("input_data/access.log") as f:
    for log in f:
        try:
            producer.produce(input_topic_name, log.encode(), callback=delivery_report)
            producer.poll(0)
        except BufferError as e:
            print(e, file=sys.stderr)
            producer.poll(1)
            producer.flush()
    
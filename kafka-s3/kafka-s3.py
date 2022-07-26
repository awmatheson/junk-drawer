from io import BytesIO
import os

from confluent_kafka import Consumer
from bytewax import Dataflow, parse, spawn_cluster, AdvanceTo, Emit

import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import json, fs

BUCKET_NAME = os.getenv("BUCKET_NAME")

def kafka_input(topic, server, timeout=1.0):
    consumer = Consumer({
        'bootstrap.servers': server,
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
        })
    consumer.subscribe([topic])
    epoch = 0
    window = 0
    while True:
        msg = consumer.poll(timeout)
        if msg is None:
            continue
        if msg.error():
            print("Consumer error: {}".format(msg.error()))
            continue
        data = msg.value()
        yield Emit((1, data))
        window += 1
        if window%10 == 0:
            epoch += 1
            yield AdvanceTo(epoch)


def input_builder(worker_index, worker_count):
    return kafka_input('drivers', 'localhost:9092')


def output_builder(worker_index, worker_count):
    return write_to_s3


def write_to_s3(epoch__table):
    epoch, table = epoch__table
    s3fs = fs.S3FileSystem(region='us-east-1')
    ds.write_dataset(
        table,
        format='parquet',
        base_dir="bw-demo-dwh/test",
        partitioning=["year", "month", "day"],
        filesystem=s3fs,
        existing_data_behavior='overwrite_or_ignore'
    )


def accumulate_events(events, event):
    return events + '\n'.encode() + event


def convert_to_arrow(epoch__events):
    epoch, events = epoch__events
    json_bytes = BytesIO(events)
    table = json.read_json(json_bytes)
    event_time = table['event_timestamp'].to_pylist()
    table = table.append_column("year", [[x.year for x in event_time]])
    table = table.append_column("month", [[x.month for x in event_time]])
    table = table.append_column("day", [[x.day for x in event_time]])
    return table


flow = Dataflow()
flow.reduce_epoch(accumulate_events)
flow.map(convert_to_arrow)
# flow.inspect(print)
flow.capture()

if __name__ == "__main__":
    
    spawn_cluster(flow, input_builder, output_builder, **parse.cluster_args())

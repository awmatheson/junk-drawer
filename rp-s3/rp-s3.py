from io import BytesIO
import os
import datetime
import uuid

from bytewax import Dataflow, parse, cluster_main
from bytewax.inputs import KafkaInputConfig

import pyarrow.dataset as ds
from pyarrow import json, fs

def output_builder(worker_index, worker_count):
    return write_to_s3

def modify_key(data):
    key, data = data
    return ('', data)

def write_to_s3(epoch__table):
    epoch, table = epoch__table
    s3fs = fs.S3FileSystem(region='us-west-2')
    ds.write_dataset(
        table,
        format='parquet',
        base_dir=BUCKET_NAME + "/" + ROOT_FOLDER,
        partitioning=["year", "month", "day", "hr"],
        filesystem=s3fs,
        existing_data_behavior='overwrite_or_ignore',
        basename_template = "part-{i}-" + uuid.uuid4().hex + ".parquet"
    )

def accumulate_events(events, event):
    return events + '\n'.encode() + event

def convert_to_arrow(epoch__events):
    epoch, events = epoch__events
    json_bytes = BytesIO(events)
    table = json.read_json(json_bytes)
    event_time = table['timestamp'].to_pylist()
    table = table.append_column("year", [[datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').year for x in event_time]])
    table = table.append_column("month", [[datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').month for x in event_time]])
    table = table.append_column("day", [[datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').day for x in event_time]])
    table = table.append_column("hr", [[datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%fZ').hour for x in event_time]])
    return table

BUCKET_NAME = os.getenv("BUCKET_NAME", "bytewax-redpanda-demo")
ROOT_FOLDER = os.getenv("ROOT_FOLDER", "parquet")
KAFKA_SERVERS = os.getenv("BYTEWAX_KAFKA_SERVER", "localhost:9092")
KAFKA_TOPIC = os.getenv("BYTEWAX_KAFKA_TOPIC", "ec2_metrics")
KAFKA_TOPIC_GROUP_ID = os.getenv("BYTEWAX_KAFKA_TOPIC_GROUP_ID", "group_id")

flow = Dataflow()
flow.map(modify_key)
flow.reduce_epoch(accumulate_events)
flow.map(convert_to_arrow)
flow.inspect(print)
flow.capture()

if __name__ == "__main__":
    input_config = KafkaInputConfig(
        KAFKA_SERVERS, KAFKA_TOPIC_GROUP_ID, KAFKA_TOPIC, messages_per_epoch=10
    )
    cluster_main(flow, input_config, output_builder, **parse.proc_env())
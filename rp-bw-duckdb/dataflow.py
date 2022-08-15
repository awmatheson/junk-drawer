from io import BytesIO
import os
import datetime
import uuid
import requests

from bytewax import Dataflow, cluster_main
from bytewax.inputs import KafkaInputConfig
from bytewax.recovery import KafkaRecoveryConfig

import pyarrow.dataset as ds
from pyarrow import csv, fs


def modify_key(data):
    key, log = data
    return ('', log)


def enrichment(data):
    '''
    Function that:
      - modifies the log string for datetime parsing
      - makes an ip lookup from ipapi
      - reformats log string to include country, region and city
    '''
    key, log = data
    log = log.decode().replace('[','"').replace(']','"') # to parse datetime
    ip_address = log.split()[0]
    response = requests.get(f'https://ipapi.co/{ip_address}/json/')
    response_json = response.json()
    if response_json.get('error'):
        city, region, country = "-", "-", "-"
    else:
        city, region, country = response_json.get("city"), response_json.get("region"), response_json.get("country_name")
    location_data = '"' + '" "'.join([city, region, country]) + '"'
    return (key, b" ".join([location_data.encode(), log.encode()]))
    

def accumulate_logs(logs, log):
    return logs + '\n'.encode() + log


def parse_logs(epoch__logs):
    '''
    Function that uses pyarrow to parse logs 
    and create a datetime partitioned pyarrow table
    '''
    epoch, logs = epoch__logs
    logs = BytesIO(logs)
    parse_options = csv.ParseOptions(delimiter=" ")
    read_options = csv.ReadOptions(
        column_names=["city", "region", "country", "ip", "1-", "2-", "event_time", "request_url",
        "status_code", "3-", "request_referrer", "user_agent", "4-"])
    table = csv.read_csv(logs, parse_options=parse_options, read_options=read_options)
    event_time = table["event_time"].to_pylist()
    table = table.append_column("year", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').year for x in event_time]])
    table = table.append_column("month", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').month for x in event_time]])
    table = table.append_column("day", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').day for x in event_time]])
    table = table.append_column("hr", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').hour for x in event_time]])
    return table


def write_to_fs(epoch__table):
    '''
    create a filesystem object with pyarro fs
    write pyarrow table as parquet to filesystem
    '''
    epoch, table = epoch__table
    local = fs.LocalFileSystem()
    ds.write_dataset(
        table,
        format='parquet',
        base_dir=ROOT_FOLDER,
        partitioning=["year", "month", "day", "hr"],
        filesystem=local,
        existing_data_behavior='overwrite_or_ignore',
        basename_template = "part-{i}-" + uuid.uuid4().hex + ".parquet"
    )


def output_builder(worker_index, worker_count):
    return write_to_fs

flow = Dataflow()
flow.map(modify_key)
flow.map(enrichment)
flow.reduce_epoch(accumulate_logs)
flow.map(parse_logs)
flow.inspect(print)
flow.capture()

ROOT_FOLDER = os.getenv("ROOT_FOLDER", "data")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server_request_logs")
KAFKA_TOPIC_GROUP_ID = os.getenv("KAFKA_TOPIC_GROUP_ID", "group_id")
KAFKA_TOPIC_GROUP_ID = os.getenv("RECOVERY_TOPIC", "group_id")

if __name__ == "__main__":
    input_config = KafkaInputConfig(
        KAFKA_BROKERS, KAFKA_TOPIC_GROUP_ID, KAFKA_TOPIC, messages_per_epoch=100
    )
    # recovery_config = KafkaRecoveryConfig(
    # KAFKA_BROKERS,
    # "requests",
    # )
    cluster_main(flow, input_config, output_builder, [], 0)
from io import BytesIO
import os
import time
import datetime
import uuid
import sys
import logging

from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig
from bytewax.outputs import ManualOutputConfig, StdOutputConfig
from bytewax.execution import spawn_cluster, cluster_main
from bytewax.window import TumblingWindowConfig, SystemClockConfig

import pyarrow.dataset as ds
from pyarrow import csv, fs

logging.basicConfig(level=logging.DEBUG)


ROOT_FOLDER = os.getenv("ROOT_FOLDER", "data")
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "server_request_test")
KAFKA_TOPIC_GROUP_ID = os.getenv("KAFKA_TOPIC_GROUP_ID", "group_1")
KAFKA_TOPIC_RECOVERY = os.getenv("RECOVERY_TOPIC", "recover")


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
    log = log.decode()
    log = log.replace("|", "%7C")
    log = log.replace(" ["," |").replace("] ","| ").replace(' "', ' |').replace('" ', '| ')[:-2] + '|' # to parse datetime
    return (key, log.encode())
    

def accumulate_logs(logs, log):
    return logs + '\n'.encode() + log


def skip_log(log):
    print(str(log))
    return("skip")


def parse_logs(epoch__logs):
    '''
    Function that uses pyarrow to parse logs 
    and create a datetime partitioned pyarrow table
    '''
    epoch, logs = epoch__logs
    logs = BytesIO(logs)
    parse_options = csv.ParseOptions(delimiter=" ", invalid_row_handler=skip_log, quote_char="|")
    read_options = csv.ReadOptions(
        column_names=["ip", "1-", "2-", "event_time", "request_url",
        "status_code", "3-", "request_referrer", "user_agent", "4-"])
    table = csv.read_csv(logs, parse_options=parse_options, read_options=read_options)
    event_time = table["event_time"].to_pylist()
    table = table.append_column("year", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').year for x in event_time]])
    table = table.append_column("month", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').month for x in event_time]])
    table = table.append_column("day", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').day for x in event_time]])
    table = table.append_column("hr", [[datetime.datetime.strptime(x, '%d/%b/%Y:%H:%M:%S %z').hour for x in event_time]])
    return table


def write_to_fs(table):
    '''
    create a filesystem object with pyarrow fs
    write pyarrow table as parquet to filesystem
    '''
    local = fs.LocalFileSystem()
    ds.write_dataset(
        table,
        format='parquet',
        base_dir=ROOT_FOLDER,
        partitioning=["year", "month", "day", "hr"],
        filesystem=local,
        existing_data_behavior='overwrite_or_ignore',
        basename_template = "part-{i}-" + uuid.uuid4().hex + "-" + str(int(time.time())) + ".parquet"
    )
    print(f"write file at {str(int(time.time()))}")


def output_builder(worker_index, worker_count):
    return write_to_fs

additional_configs = {"bootstrap.servers":"localhost:9092"}

flow = Dataflow()
flow.input(
    "inp",
    KafkaInputConfig(
        brokers=["localhost:9092"],
        topic="server_request_test",
        starting_offset="beginning",
        tail=False,
        additional_configs = additional_configs,
    ),
)

cc = SystemClockConfig()
wc = TumblingWindowConfig(length=datetime.timedelta(seconds=5))

flow.map(modify_key)
flow.map(enrichment)
flow.reduce_window("reducer", cc, wc, accumulate_logs)
flow.map(parse_logs)
flow.capture(ManualOutputConfig(output_builder))
# flow.capture(StdOutputConfig())

if __name__ == "__main__":
    
    cluster_main(flow, [], 0)
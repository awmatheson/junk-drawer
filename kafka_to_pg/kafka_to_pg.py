import psycopg2
import json
import datetime
from collections import defaultdict

from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig 
from bytewax.outputs import ManualOutputConfig, StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig
from bytewax.execution import cluster_main

def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    event_data = json.loads(payload_bytes) if payload_bytes else None
    return event_data["user_id"], event_data

def anonymize_email(user_id__event_data):
    user_id, event_data = user_id__event_data
    event_data["email"] = "@".join(["******", event_data["email"].split("@")[-1]])
    return user_id, event_data

def remove_bytewax(user_id__event_data):
    user_id, event_data = user_id__event_data
    return "bytewax" not in event_data["email"]

flow = Dataflow()
flow.input("inp", KafkaInputConfig(brokers=["localhost:9092"], topic="web_events"))
flow.map(deserialize)
flow.map(anonymize_email)
flow.filter(remove_bytewax)

cc = SystemClockConfig()
wc = TumblingWindowConfig(length=datetime.timedelta(seconds=5))

def build():
    return defaultdict(lambda: 0)

def count_events(results, event):
    results[event["type"]] += 1
    return results

flow.fold_window("session_state_recovery", cc, wc, build, count_events)


def output_builder(worker_index, worker_count):
    # create the connection at the worker level
    conn = psycopg2.connect("dbname=website user=bytewax")
    conn.set_session(autocommit=True) 
    cur = conn.cursor()

    def write_to_postgres(user_id__user_data):
        user_id, user_data = user_id__user_data
        query_string = f'''
                    INSERT INTO events (user_id, data) 
                    VALUES ('{user_id}', '{json.dumps(user_data)}')
                    ON CONFLICT (user_id) 
                    DO 
                        UPDATE SET data = '{json.dumps(user_data)}';'''
        cur.execute(query_string)
    return write_to_postgres

flow.capture(ManualOutputConfig(output_builder))

if __name__ == "__main__":
    addresses = [
    "localhost:2101"
    ]

    cluster_main(
        flow, 
        addresses=addresses,
        proc_id=0,
        worker_count_per_proc=2)
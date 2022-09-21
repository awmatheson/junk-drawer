import json
import datetime

#created_at,entry_id,PM1.0_CF1_ug/m3,PM2.5_CF1_ug/m3,PM10.0_CF1_ug/m3,UptimeMinutes,RSSI_dbm,Temperature_F,Humidity_%,PM2.5_ATM_ug/m3,

from numpy import mean
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig 
from bytewax.outputs import ManualOutputConfig, StdOutputConfig
from bytewax.window import TumblingWindowConfig, SystemClockConfig
from bytewax.execution import cluster_main

from river import anomaly, preprocessing

def deserialize(key_bytes__payload_bytes):
    key_bytes, payload_bytes = key_bytes__payload_bytes
    key = json.loads(key_bytes) if key_bytes else None
    sensor_data = json.loads(payload_bytes) if payload_bytes else None
    return key, sensor_data

flow = Dataflow()
flow.input(
    "aqi_state", 
    KafkaInputConfig(
        brokers=["localhost:9092"], 
        topic="sensor_data",
        starting_offset = "end",
        tail = True
        )
    )

flow.map(deserialize)

cc = SystemClockConfig()
wc = TumblingWindowConfig(length=datetime.timedelta(seconds=.2))

def build():
    return {"PM2.5_CF1_ug/m3":[],"Temperature_F":[],"Humidity":[]}

def count_events(results, event):
    results["PM2.5_CF1_ug/m3"].append(float(event["PM2.5_CF1_ug/m3"])),
    results["Temperature_F"].append(float(event["Temperature_F"])),
    results["Humidity"].append(float(event["Humidity"]))
    return results

flow.fold_window("session_state_recovery", cc, wc, build, count_events)

def average(sensor__data):
    sensor, data = sensor__data
    avg_data = {}
    for key, value in data.items():
        avg_data[key] = mean(value)
    return sensor, avg_data

flow.map(average)

class AnomalyDetector:

    def __init__(self, n_trees=10, height=8, window_size=72, seed=11):
        self.scaler = preprocessing.MinMaxScaler()
        self.detector = anomaly.HalfSpaceTrees(
                                                n_trees=n_trees,
                                                height=height,
                                                window_size=window_size,
                                                seed=seed
                                                )
    
    def update(self, data):
        normalized = self.scaler.learn_one({"x": data['PM2.5_CF1_ug/m3']}).transform_one({"x": data['PM2.5_CF1_ug/m3']})
        self.detector.learn_one(normalized)
        data['score'] = self.detector.score_one(normalized)
        return self, data

flow.stateful_map(
    step_id = "anomaly_detector",
    builder = lambda: AnomalyDetector(n_trees=10, height=5, window_size=100, seed=42),
    mapper = AnomalyDetector.update,
)
flow.filter(lambda x: x[1]['score']>0.90)
flow.capture(StdOutputConfig())
# flow.capture(ManualOutputConfig(output_builder))

if __name__ == "__main__":
    addresses = [
    "localhost:2101"
    ]

    cluster_main(
        flow, 
        addresses=addresses,
        proc_id=0,
        worker_count_per_proc=1)
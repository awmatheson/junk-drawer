import json
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import time

from geopy.geocoders import Nominatim
from bytewax.dataflow import Dataflow
from bytewax.inputs import KafkaInputConfig 
from bytewax.outputs import ManualOutputConfig, StdOutputConfig
from bytewax.window import TumblingWindowConfig, EventClockConfig
from bytewax.execution import cluster_main

from scipy.stats import variation

from river import anomaly

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
        starting_offset = "beginning",
        tail = False
        )
    )

flow.map(deserialize)
class AnomalyDetector:

    def __init__(self, n_trees=10, height=8, window_size=72, seed=11):
        self.detector = anomaly.HalfSpaceTrees(
                                                n_trees=n_trees,
                                                height=height,
                                                window_size=window_size,
                                                limits={'x': (0.0, 2000)},
                                                seed=seed
                                                )
    
    def update(self, data):
        data['score'] = self.detector.score_one({'x': float(data['PM2.5_CF1_ug/m3'])})
        self.detector.learn_one({'x': float(data['PM2.5_CF1_ug/m3'])})
        return self, data

flow.stateful_map(
    step_id = "anomaly_detector",
    builder = lambda: AnomalyDetector(n_trees=4, height=3, window_size=50, seed=11),
    mapper = AnomalyDetector.update,
)
flow.filter(lambda x: x[1]['score']>0.7)
flow.filter(lambda x: float(x[1]['PM2.5_CF1_ug/m3'])>50)

def groupby_region(loc__data):
    coordinates, data = loc__data
    # add the sensor location to the data payload
    data["coordinates"] = coordinates

    # Uncomment this code if you want to convert the lat, long to county automatically
    # locator = Nominatim(user_agent="myGeocoder")
    # location = locator.reverse(coordinates)
    # key = location.raw['address']['county']

    # since we know the location of these we will mark the key manually
    # as the generalized area we are interested in 
    key = "Lake Tahoe"
    return key, data

flow.map(groupby_region)

def get_event_time(event):
    return datetime.strptime(event["created_at"], "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=timezone.utc)

cc = EventClockConfig(get_event_time, wait_for_system_duration=timedelta(minutes=10))
start_at = datetime.strptime("2022-07-01 00:00:00 UTC", "%Y-%m-%d %H:%M:%S %Z").replace(tzinfo=timezone.utc)
wc = TumblingWindowConfig(start_at=start_at, length=timedelta(hours=6))

class Anomalies:

    def __init__(self):
        self.sensors = []
        self.times = []
        self.values = []

    def update(self, event):
        self.sensors.append(event["coordinates"])
        self.times.append(event["created_at"])
        self.values.append(float(event["PM2.5_CF1_ug/m3"]))

        return self

    def __str__(self):
        return f"{self.times}, {self.sensors}, {self.values}"

flow.fold_window("count_sensors", cc, wc, Anomalies, Anomalies.update)

def convert(key__anomalies):
    key, anomalies = key__anomalies
    
    # check is more than one sensor anomalous
    count_sensors = len(set(anomalies.sensors))
    count_anomalies = len(anomalies.values)
    min_event = min(anomalies.times)
    max_pm25 = max(anomalies.values)
    sensors = set(anomalies.sensors)
    malfunction = False
    anom_variance = None

    if count_sensors < 2:
        if count_anomalies < 2:
            malfunction = True
        else:
            anom_variance = variation(anomalies.values)
            if anom_variance > 0.3:
                malfunction = True

    return {
            "sensors": set(anomalies.sensors),
            "count_sensors": count_sensors,
            "count_anomalies": count_anomalies,
            "anomalies": anomalies.values,
            "min_event": min_event,
            "max_pm25": max_pm25,
            "variance": anom_variance,
            "malfunction": malfunction
            }

flow.map(convert)
flow.capture(
    StdOutputConfig()
    # KafkaOutputConfig(
    #     brokers=["localhost:9092"],
    #     topic="sensor_anomalies",
    # )
)

if __name__ == "__main__":
    addresses = [
    "localhost:2101"
    ]

    cluster_main(
        flow, 
        addresses=addresses,
        proc_id=0,
        worker_count_per_proc=1)
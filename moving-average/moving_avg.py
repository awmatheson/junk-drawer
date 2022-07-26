import base64
import json
# from statistics import mean
import traceback
from typing import Tuple
from bytewax.inputs import ManualInputConfig, AdvanceTo, Emit
from bytewax.execution import run_main, run
from bytewax import Dataflow
# import pandas as pd

# from kafka import KafkaConsumer

from datetime import datetime, timedelta


def _preprocess(payload_input):
    try:
        content = payload_input['content']
        base64_bytes = base64.b64decode(content)
        measurement = base64_bytes.decode('utf-8')
        measurement_object = json.loads(measurement)
        dt = datetime.strptime(measurement_object['dtm'], '%Y-%m-%dT%H:%M:%S')
        device_id = measurement_object['device_id']
        pi = measurement_object['pi']
        return (device_id, (dt, pi))
    except Exception as e:
        print("An error occurred while decoding and parsing JSON measurement:", e)
        traceback.print_exc()
        return None



data = [{"topic": "lte/msg/6970631401582127/l2", "content": "eyJkZXZpY2VfaWQiOiJHRzZvajRQdCIsImR0bSI6IjIwMjItMDctMjFUMTQ6MzY6MzAiLCJyc3NpIjotMTA0LCJtZXRlcl9pZCI6IjY5NzA2MzE0MDE1ODIxMjciLCJ1bml4IjoxNjU4NDA2OTkwLCJwaSI6ODgzLjAsInBlIjowLjAsInFpIjozOTQuMCwicWUiOjAuMCwiaTEiOjIuMDE5LCJpMiI6NC42MDQsImkzIjoyLjYyMCwidTEiOjIzMC4yLCJ1MiI6MC4wLCJ1MyI6MjI4LjF9"},
        {"topic": "lte/msg/6970631401582127/l1", "content": "eyJkZXZpY2VfaWQiOiJHRzZvajRQdCIsImR0bSI6IjIwMjItMDctMjFUMTQ6MzY6MzQiLCJyc3NpIjotMTA0LCJ1bml4IjoxNjU4NDA2OTk0LCJwaSI6ODg0LjB9"},
        {"topic": "lte/msg/6970631401582127/l1", "content": "eyJkZXZpY2VfaWQiOiJHRzZvajRQdCIsImR0bSI6IjIwMjItMDctMjFUMTQ6MzY6MzIiLCJyc3NpIjotMTA0LCJ1bml4IjoxNjU4NDA3MDAyLCJwaSI6ODg1LjB9"},
        {"topic": "lte/msg/6970631401582127/l1", "content": "eyJkZXZpY2VfaWQiOiJHRzZvajRQdCIsImR0bSI6IjIwMjItMDctMjFUMTQ6MzY6MzIiLCJyc3NpIjotMTA0LCJ1bml4IjoxNjU4NDEwMDAyLCJwaSI6ODg1LjB9"},
        {"topic": "lte/msg/6970631408343196/l1","content":"eyJkZXZpY2VfaWQiOiJKSjlpdDY4UCIsImR0bSI6IjIwMjItMDctMjFUMTQ6MzY6MzQiLCJyc3NpIjotOTksInVuaXgiOjE2NTg0MDY5OTQsInBpIjoyNTMuMH0="},
        {"topic": "nte/msg/6970631407585061/l1","content":"eyJkdG0iOiIyMDIyLTA3LTIxVDE0OjM2OjQ0IiwibWV0ZXJfZHRtIjoiMjAyMi0wNy0yMVQxNDozNjo0NCIsInVuaXgiOjE2NTg0MDcwMDQsInBpIjo2MTkuMCwicnNzaSI6LTYyLCJkZXZpY2VfaWQiOiJKSjlpdDY4UCIsImJvb3RzIjoyMDksImZ3Ijo5fQ=="}]


def data_input():
    epoch = 0
    for line in data:
        yield (epoch, line)
        epoch += 1

# we are going to use the pattern of builder/mapper with
# class objects that we can control the information contained
# for each hour. stateful map operators are aggregated by key
# like reduce_epoch, but they can operate over many epochs
# this allows us to "greedily" evaluate the average and send data
# downstream for capture before the hour is over.
class Sensor:
    def __init__(self):
        self.sum_vals = 0
        self.num_vals = 0
        self.current_hr = None

    def update_avg(self, data):
        # update the data for the average
        dt, pi = data # data passed in
        hour = int(dt.hour)
        
        # check that a value for hr is set
        if self.current_hr == None:
            self.current_hr = hour
        
        # if the current_hr == hour for this data we update
        # the average otherwise we will reset it
        if hour == self.current_hr:
            self.sum_vals += pi
            self.num_vals += 1
        else:
            print('reset avg for new hr')
            self.sum_vals = pi
            self.num_vals = 1

        # calc the average for each new value
        self.avg = self.sum_vals/self.num_vals

        return self, (dt, pi, self.avg)
    

flow = Dataflow()
flow.map(_preprocess)
flow.inspect(print)
flow.stateful_map("average", lambda key: Sensor(), Sensor.update_avg)
flow.capture()


if __name__ == "__main__":
    print("Dataflow starting!")
    for e, i in run(flow, data_input()):
        print(e, i)
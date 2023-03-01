import random
import operator
import os
import sys
from time import sleep
from datetime import datetime, timedelta
import json

import requests

from bytewax.dataflow import Dataflow
from bytewax.execution import spawn_cluster
from bytewax.inputs import ManualInputConfig
from bytewax.window import SystemClockConfig, TumblingWindowConfig
from bytewax.outputs import ManualOutputConfig, StdOutputConfig

WEBHOOK_URL = os.getenv("WEBHOOK_URL")


def input_builder(worker_index, worker_count, resume_state):
    def random_datapoints():
        for _ in range(20000):
            if _ > 8200 and _ < 8500:
                factor = 50
            elif _ > 15000 and _ < 15300:
                factor = 50
            else:
                factor = 1
            yield None, ("topic_name", {"some_random_data":"the_value"})
            sleep(random.randrange(1, 5)/1000*factor)

    return random_datapoints()


flow = Dataflow()
flow.input("inp", ManualInputConfig(input_builder))
flow.map(lambda x: (x[0], 1)) # we just care about the rate and not the data at this point
flow.reduce_window(
    "count",
    SystemClockConfig(),
    TumblingWindowConfig(length=timedelta(seconds=2)),
    operator.add
)

class ZTestDetector:
    """Anomaly detector.
    Use with a call to flow.stateful_map().
    Looks at how many standard deviations the current item is away
    from the mean (Z-score) of the last 10 items. Mark as anomalous if
    over the threshold specified.
    """

    def __init__(self, threshold_z):
        self.threshold_z = threshold_z

        self.last_10 = []
        self.mu = None
        self.sigma = None

    def _push(self, value):
        self.last_10.insert(0, value)
        del self.last_10[10:]

    def _recalc_stats(self):
        last_len = len(self.last_10)
        self.mu = sum(self.last_10) / last_len
        sigma_sq = sum((value - self.mu) ** 2 for value in self.last_10) / last_len
        self.sigma = sigma_sq**0.5

    def push(self, value):
        is_anomalous = False
        if len(self.last_10) < 10:
            is_anomalous = False
        elif self.mu and self.sigma:
            is_anomalous = abs(value - self.mu) / self.sigma > self.threshold_z

        self._push(value)
        self._recalc_stats()

        return self, (value, self.mu, self.sigma, is_anomalous)

flow.stateful_map("AnomalyDetector", lambda: ZTestDetector(2.0), ZTestDetector.push)


# output smoke event data to slack channel
def output_builder(worker_index, worker_count):
    
    def send_to_slack(key__data):
        topic_key, (value, mu, sigma, is_anomalous) = key__data
        
        if WEBHOOK_URL:
            if is_anomalous:
                message = f'''Topic {topic_key} experiencing anomalous volume, see https://dashboard.lab.bytewax.io/demo/github-demo/output'''
                title = (f"Data Quality Event")
                slack_data = {
                    "username": "Data Quality Bot",
                    "icon_emoji": ":chart_with_upwards_trend:",
                    "channel" : "#hacking-on-bytewax",
                    "attachments": [
                        {
                            "color": "#9733EE",
                            "fields": [
                                {
                                    "title": title,
                                    "value": message,
                                    "short": "false",
                                }
                            ]
                        }
                    ]
                }
                byte_length = str(sys.getsizeof(slack_data))
                headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
                response = requests.post(WEBHOOK_URL, data=json.dumps(slack_data), headers=headers)
                if response.status_code != 200:
                    raise Exception(response.status_code, response.text)
            else:
                print(f"""{topic_key}: value = {value}, mu = {mu:.2f}, sigma = {sigma:.2f}, {is_anomalous}""")
        else:
            print(f"""{topic_key}: value = {value}, mu = {mu:.2f}, sigma = {sigma:.2f}, {is_anomalous}""")

    return send_to_slack

flow.capture(ManualOutputConfig(output_builder))

if __name__ == '__main__':
    spawn_cluster(flow, proc_count=1)

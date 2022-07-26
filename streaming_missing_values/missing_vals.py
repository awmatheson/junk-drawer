import random
import numpy as np

from bytewax import Dataflow, inputs, parse, run_cluster


def random_datapoints():
    for epoch in range(100):
        if epoch % 5 == 0:
            yield f'data', np.nan
        else:
            yield f'data', random.randrange(0, 10)


class Windowed_Array:
    """Windowed Numpy Array.

    Create a numpy array to run windowed statistics on.
    """

    def __init__(self, window_size):
        self.last_n = np.empty(0, dtype=object)
        self.n = window_size


    def _push(self, value):
        self.last_n = np.insert(self.last_n, 0, value)
        try:
            self.last_n = np.delete(self.last_n, self.n)
        except IndexError:
            pass


    def impute_value(self, value):
        self._push(value)
        if np.isnan(value):
            new_value = np.nanmean(self.last_n)
        else:
            new_value = value
        return self, (value, new_value)


def inspector(epoch, data):
    metric, (value, imputed) = data
    print(f"data: {value}, imputed value if required {imputed}")


flow = Dataflow()
# ("metric", value)
flow.stateful_map(lambda key: Windowed_Array(10), Windowed_Array.impute_value)
# ("metric", (old value, new value))
flow.capture()


if __name__ == "__main__":
    for epoch, item in run_cluster(
        flow, inputs.fully_ordered(random_datapoints()), **parse.cluster_args()
    ):
        inspector(epoch, item)
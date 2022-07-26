import math
import collections

import pyshark
from bytewax import Dataflow, parse, spawn_cluster, AdvanceTo, Emit

def input_builder(worker_index, worker_count):
    # Since our input is not parallelizable, we open a live capture only
    # in the first worker.
    if worker_index == 0:
        capture = pyshark.LiveCapture(interface='en0', bpf_filter="tcp")
        epoch = 0
        for packet in capture.sniff_continuously(packet_count=2000):
            yield Emit(packet)
            epoch += 1
            yield AdvanceTo(epoch)


def log2(p):
    return math.log(p, 2) if p > 0 else 0 


def group_by_flow(packet):
    '''packet flow in pyshark is shown as
    the TCP stream'''
    # protocol type
    protocol = packet.transport_layer   

    return (packet[protocol].stream, (packet))


class Packets:

    def __init__(self):
        self.count_seen_packets = 0
        self.packet_counts_ = collections.defaultdict(int)
        self.entropy = 0


    def update(self, packet):
        self.protocol = packet.transport_layer   # protocol type
        self.src_addr = packet.ip.src            # source address
        self.src_port = packet[self.protocol].srcport   # source port
        self.dst_addr = packet.ip.dst            # destination address
        self.dst_port = packet[self.protocol].dstport   # destination port
        self.packet_bytes = int(packet.length)   # bytes in the current packet

        # calculate entropy of packet size
        self.entropy = self._calc_entropy(self.packet_bytes, 1)

        # update packet details
        self.count_seen_packets += 1
        self.packet_counts_[self.packet_bytes] += 1

        return self, ((self.src_addr, self.src_port, self.dst_addr, self.dst_port), self.count_seen_packets, self.entropy)


    def _calc_entropy(self, size_bytes, r):
        '''Modified entropy calculation for estimating the incremental
        entropy in a stream of data. This was modified from Blaz Sovdat's
        paper - https://arxiv.org/abs/1403.6348 and the corresponding 
        post -  https://stackoverflow.com/questions/48601396/calculating-incremental-entropy-for-data-that-is-not-real-numbers
        '''
        p_new = (self.packet_counts_[size_bytes] + 1) / (self.count_seen_packets + r)
        p_old = self.packet_counts_[size_bytes] / (self.count_seen_packets + r)

        residual = p_new * log2(p_new) - p_old * log2(p_old)
        self.entropy = self.count_seen_packets * (self.entropy - log2(self.count_seen_packets / (self.count_seen_packets + r))) / (self.count_seen_packets + r) - residual

        return self.entropy


def alert_low_entropy(packet_data):
    flow_number, data = packet_data
    flow_details, num_packets, entropy = data
    return entropy < 0.5 and num_packets > 4


def output_builder(worker_index, worker_count):
    return print


flow = Dataflow()
flow.map(group_by_flow)
#(flow_number, packet_data)
flow.stateful_map(lambda key: Packets(), Packets.update)
flow.inspect(print)
flow.filter(alert_low_entropy)
flow.capture()

if __name__ == "__main__":

    spawn_cluster(flow, input_builder, output_builder, **parse.cluster_args())
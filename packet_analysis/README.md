# Protecting Against Cyberattacks with Bytewax and Pyshark


Every year, cyberattacks become more and more commonplace. Almost daily there are reports of ongoing cyberattacks causing information destruction, service disruption or data theft. With headlines like, "[Russian group claims hack of Lithuanian sites in retaliation for transit ban](https://www.reuters.com/technology/lithuania-hit-by-cyber-attack-government-agency-2022-06-27/)", "[Cyberattack forces Iran steel company to halt production](https://abcnews.go.com/International/wireStory/cyberattack-forces-iran-steel-company-halt-production-85773314)" or "[General Motors Hacked In Cyberattack, Personal Data Exposed](https://www.motor1.com/news/588646/general-motors-hacked-cyberattack/)". One of the most damaging types of cyberattacks is the command-and-control attack, which can be devastating to the targeted organization. In a command-and-control attack, an attacker will first attempt to infect a machine in the organization's network via phishing, browser plugins or other infected software. Once infected the machine will open communication with the attackers machine and the infected machine can be controlled remotely. This can be devastating, with the attackers stealing data, shutting down machines or overwhelming the network with DDoS attacks.

In this post we are going to look at how we can detect suspicious activity on a network that could be a command-and-control style attack. We will use the common network analysis tool wireshark to sniff network packets and then Bytewax to process the flow of packets and analyze the flows for their entropy.
__This is a very rudimentary analysis and could be fooled by an attacker, please consult a professional for your security needs__

We are going to write a Bytewax dataflow that will:

1. Connect to the network interface and sniff packets
2. Group the packets by flow (source and destination ip and port)
3. Calculate the incremental entropy for each additional packet
4. Filter down to only low entropy flows
5. Alert on these flows

### Setup

install wireshark

install pyshark and bytewax

## Building the dataflow

### Sniffing Packets

To start, let's construct an input builder, the mechanism for adding input to a dataflow. Create a new python file called `packet_analysis` with the code below. This takes care of some of the later imports as well.

```python
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
```

Now we have our input builder ready to capture packets from interface `en0` (note: if you are using a different OS, this interface could be called something different.) and we are filtering for TCP only.
For each packet we receive, we will `Emit` this to the dataflow and then advance the epoch. The epoch is used to control the flow and completion of processing in a dataflow. We are advancing every time because we are processing one packet at a time downstream.

### Processing the Packet

#### Grouping by TCP Flow

Pyshark provides a nicely formatted object for us, so we can access many of the properties of the packet. Since we are analyzing flows of TCP packets we want to group by the stream number (flow id), which is one of the nice properties that we can use out of the box from wireshark/pyshark. If we did not have this, we could create a key that was of the format `source_ip:port:destination_ip:port` and use that. We are going to use this flow id to group the TCP packet flows in the next step. Let's create our dataflow and transform the data in a map operator to be a tuple of the shape `(flow_number, packet_data)`.

```python
def group_by_flow(packet):
    '''packet flow in pyshark is shown as
    the TCP stream'''
    # protocol type
    protocol = packet.transport_layer   

    return (packet[protocol].stream, (packet))

flow = Dataflow()
#(epoch, packet_data)
flow.map(group_by_flow)
#(epoch, (flow_number, packet_data))
```

#### Aggregating and Calculating Entropy

The next step in our dataflow is going to be a stateful operator that will aggregate packets for each flow and then we will be able to calculate the entropy of the size of the bytes in the packets.

*An Aside on Entropy:*

Diving into the mathematical derivation of entropy is outside of this tutorial. You can think of entropy in this application as the amount of randomness in a stream of information. When the entropy is low, it is uniform and high it is varied. In most applications the flow of bytes has some randomness in it, but if a machine is compromised, it could be sending the same information to the attackers machine repeatedly or the attacker may be sending the same instructions to the machine repeatedly and the entropy will therefore be low. We are going to use an adaptation in the entropy calculation that will allow us to incrementally update the entropy score instead of computing it from the entire set of packets every time to increase the efficiency of our dataflow. The calculation was adapted from [this stack overflow answer](https://stackoverflow.com/questions/48601396/calculating-incremental-entropy-for-data-that-is-not-real-numbers) which is based on [this paper](https://arxiv.org/abs/1403.6348).

There is a lot going on in the code below, so we will unpack it below.

```python
flow.stateful_map(lambda key: Packets(), Packets.update)

def log2(p):
    return math.log(p, 2) if p > 0 else 0


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
```

First, lets go over the dataflow line:

```python
flow.stateful_map(lambda key: Packets(), Packets.update)
```

Here we are adding the `stateful_map` operator to our dataflow as the step after we modified our data in the previous step. Stateful map is a one-to-one transformation of values in (key, value) pairs, but allows you to reference a persistent state for each key when doing the transformation. The operator calls two functions, a builder and a mapper. The builder function returns a new state for each key. In our case this is a new `Packets` object for each TCP flow id. The mapper transforms the values passed in. In this case it is the Packets.update call.

For every time we first see a new flow id (combination of source and destination ip addresses and ports) we will instantiate a new Packets object.

```python
class Packets:

    def __init__(self):
        self.count_seen_packets = 0
        self.packet_counts_ = collections.defaultdict(int)
        self.entropy = 0
```

Then the `stateful_map` operator will call `Packet.update`. Which pulls out some information from the packet and then calculates the entropy.

```python
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
```

The entropy calculation calculates the probability of seeing the current sized packet given the number of occurrences that a packet of that size has been seen before. A residual is computed and then those values are used to calculate the entropy, which is returned.

```python
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
```

Now once we have the entropy score, we can filter out the random flows with the filter operator and alert when there is a flow that has a low entropy score and has had at least 4 packets in the flow.

```python
flow.filter(alert_low_entropy)
flow.capture()

def alert_low_entropy(packet_data):
    flow_number, data = packet_data
    flow_details, num_packets, entropy = data
    return entropy < 0.5 and num_packets > 4


def output_builder(worker_index, worker_count):
    return print
```

The capture operator is how you specify output of a dataflow. Whenever an item flows into a capture operator, the output handler of the worker is called with that item and epoch. In this case we are just printing out the suspect flow details.

### Running the Dataflow

Our dataflow is ready to rock and sniff some packets. To kick off the dataflow we can run it from the command line as a python file. The number of workers should be specified in the call and will be parsed and used in the code below. 

```python
if __name__ == "__main__":

    spawn_cluster(flow, input_builder, output_builder, **parse.cluster_args())
```

*Kick it off!*

```bash
python packet_analysis.py -w 1
```

Thanks for following along! We hope you liked it, if you have any comments or feedback, log an issue in the [GitHub repo](https://github.com/bytewax/bytewax). Give us a star while you are there if you want to support the project!

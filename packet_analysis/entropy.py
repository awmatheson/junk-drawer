import collections
import math
import random


def log2(p):
    return math.log(p, 2) if p > 0 else 0


CountChange = collections.namedtuple('CountChange', ('label', 'change'))


class EntropyHolder:
    def __init__(self):
        self.counts_ = collections.defaultdict(int)

        self.entropy_ = 0
        self.sum_ = 0

    def update(self, count_changes):
        print("count_change", count_changes)
        r = sum([change for _, change in count_changes])
        print("r", r)

        residual = self._compute_residual(count_changes)

        self.entropy_ = self.sum_ * (self.entropy_ - log2(self.sum_ / (self.sum_ + r))) / (self.sum_ + r) - residual

        self._update_counts(count_changes)

        return self.entropy_

    def _compute_residual(self, count_changes):
        r = sum([change for _, change in count_changes])
        residual = 0

        for label, change in count_changes:
            print(self.counts_[label])
            p_new = (self.counts_[label] + change) / (self.sum_ + r)
            p_old = self.counts_[label] / (self.sum_ + r)

            print(p_new, p_old)

            residual += p_new * log2(p_new) - p_old * log2(p_old)

        return residual

    def _update_counts(self, count_changes):
        for label, change in count_changes:
            self.sum_ += change
            self.counts_[label] += change

    def entropy(self):
        return self.entropy_



def naive_entropy(counts):
    s = sum(counts)
    return sum([-(r/s) * log2(r/s) for r in counts])


if __name__ == '__main__':
    print(naive_entropy([1, 1]))
    print(naive_entropy([1, 1, 1, 1]))

    entropy = EntropyHolder()
    freq = collections.defaultdict(int)
    for _ in range(100):
        index = random.randint(0, 5)
        entropy.update([CountChange(index, 1)])
        freq[index] += 1

    print(naive_entropy(freq.values()))
    print(entropy.entropy())
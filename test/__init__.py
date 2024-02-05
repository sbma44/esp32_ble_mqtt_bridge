class Accumulator(object):
    def __init__(self):
        self.value = 0

    def get(self):
        self.value = (self.value + 1) % 100
        return self.value
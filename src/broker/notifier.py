from typing import Callable, List


class Notifier:
    def __init__(self):
        self.callbacks: List[Callable] = []

    def subscribe(self, callback: Callable):
        self.callbacks.append(callback)

    def publish(self, data):
        for callback in self.callbacks:
            callback(data)

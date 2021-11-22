from enum import Enum


class Market(Enum):
    FUTURES = "FUTURES"
    SPOT = "SPOT"

    def __str__(self):
        return self.value

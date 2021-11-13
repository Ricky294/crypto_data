import os
import json
import yaml

SEC_MAP = {
    "s": 1,
    "m": 60,
    "h": 3600,
    "d": 86400,
    "w": 604800,
    "M": 2629800,
    "Y": 31557600,
}


def read_json(*path):
    with open(os.path.join(*path)) as f:
        return json.loads(f.read())


def read_yaml(*path):
    with open(os.path.join(*path)) as f:
        return yaml.load(f, Loader=yaml.FullLoader)


def split_interval(interval: str):
    measure = interval[len(interval) - 1]
    value = interval[0 : len(interval) - 1]

    return int(value), measure


def interval_ratio(numerator_interval: str, denominator_interval: str) -> int:
    niv, nim = split_interval(numerator_interval)
    div, dim = split_interval(denominator_interval)

    n_multiplier = SEC_MAP[nim]
    d_multiplier = SEC_MAP[dim]

    return int((n_multiplier * niv) / (d_multiplier * div))


def add_interval(timestamp: int, interval: str):
    timestamp = int(timestamp)

    value, time = split_interval(interval)
    time_in_sec = SEC_MAP[time]
    return timestamp + (time_in_sec * value)


def round_down(number: float, precision: int):
    s = str(number)
    return float(s[: s.find(".") + precision + 1])

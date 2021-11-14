import os
import json
import time

import yaml
from tqdm import tqdm

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
    timeframe = interval[len(interval) - 1]
    value = interval[0 : len(interval) - 1]

    return int(value), timeframe


def interval_ratio(numerator_interval: str, denominator_interval: str) -> int:
    niv, nim = split_interval(numerator_interval)
    div, dim = split_interval(denominator_interval)

    n_multiplier = SEC_MAP[nim]
    d_multiplier = SEC_MAP[dim]

    return int((n_multiplier * niv) / (d_multiplier * div))


def interval_in_seconds(interval: str) -> int:
    value, timeframe = split_interval(interval)
    return value * SEC_MAP[timeframe]


def round_down(number: float, precision: int):
    s = str(number)
    return float(s[: s.find(".") + precision + 1])


def progress_bar(
    start_time: int,
    end_time: int,
    interval: str,
    update_size: int,
    sleep_in_seconds: int,
):
    total_time = (end_time - start_time) / interval_in_seconds(interval)

    with tqdm(total=int(total_time)) as bar:
        for _ in range(round(total_time / update_size)):
            time.sleep(sleep_in_seconds)
            bar.update(update_size)

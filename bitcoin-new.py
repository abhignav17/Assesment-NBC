import os
import json
import random
import datetime
from typing import Any, Dict, Iterable, List, Tuple
from dataclasses import dataclass, field
import statistics

import urllib.request

FETCH_URL = os.environ.get(
    "FETCH_URL", "https://api.coinranking.com/v1/public/coin/1/history/30d")


def gen_data():
    num_points = 30*24
    today = datetime.datetime.now()

    data = []
    gen_nums = random.choices(range(num_points), k=num_points)

    for i, gen_num in enumerate(gen_nums):
        gen_date = today - datetime.timedelta(hours=num_points - i)
        data.append((gen_date, float(gen_num)))

    return data


def fetch_and_parse():
    url = FETCH_URL

    if url is None:
        return

    raw_file = urllib.request.urlopen(url)
    raw_json = raw_file.read()

    data = json.loads(raw_json)
    for row in data["data"]["history"]:
        # python uses second format and this is in millisecond format
        date_val = datetime.datetime.fromtimestamp(row["timestamp"] / 1000)
        price_val = float(row["price"])
        yield date_val, price_val


PriceDateEntry = Tuple[datetime.datetime, float]


@dataclass
class DailyPriceStats:
    # current data points
    curr_val: float = None
    curr_date: datetime.datetime = None

    # last data point (last hour)
    prev_val: float = None
    prev_date: datetime.datetime = None

    prev_day_val: float = None

    def output_format(self) -> Dict[str, Any]:
        raise NotImplementedError

    def hourly_update(self):
        pass

    def post_daily_update(self):
        pass

    @property
    def new_day(self):
        return self.prev_date is None or \
            (self.prev_date is not None and self.prev_date.day != self.curr_date.day)

    def act(self, data: Iterable[PriceDateEntry]) -> Iterable[Dict[str, Any]]:
        for line in data:
            self.curr_date, self.curr_val = line

            # new day, emit changes and update SOD data
            if self.new_day:
                yield self.output_format()
                self.post_daily_update()
                self.prev_day_val = self.curr_val

            self.hourly_update()

            self.prev_date = self.curr_date
            self.prev_val = self.curr_val

        self.hourly_update()
        yield self.output_format()


@dataclass
class Schema1(DailyPriceStats):
    _min_val: float = None
    _max_val: float = None

    @property
    def day_of_week(self) -> str:
        return self.curr_date.strftime("%A")

    @property
    def change(self) -> float:
        if self.prev_day_val is None:
            return None
        return self.curr_val - self.prev_day_val

    @property
    def direction(self) -> str:
        if self.change is None:
            return None
        return "up" if self.change > 0 else "down" if self.change < 0 else "same"

    @property
    def high_since_start(self) -> float:
        if self._max_val is None:
            self._max_val = self.curr_val
            return None

        if self.curr_val > self._max_val:
            self._max_val = self.curr_val
            return True

        return False

    @property
    def low_since_start(self) -> float:
        if self._min_val is None:
            self._min_val = self.curr_val
            return None

        if self.curr_val < self._min_val:
            self._min_val = self.curr_val
            return True

        return False

    def output_format(self) -> Dict[str, Any]:
        return {
            "date": self.curr_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "price": f"{self.curr_val:0.2f}",
            "direction": self.direction,
            "change": f"{self.change:0.2f}" if self.change else None,
            "dayOfWeek": self.day_of_week,
            "highSinceStart": self.high_since_start,
            "lowSinceStart": self.low_since_start,
        }


@dataclass
class Schema2(DailyPriceStats):
    vals: List[float] = field(default_factory=list)

    def daily_average(self):
        if len(self.vals) < 1:
            return self.curr_val

        return statistics.mean(self.vals)

    def daily_variance(self):
        if len(self.vals) < 2:
            return 0

        return statistics.variance(self.vals)

    def volatility_alert(self):
        for i in range(len(self.vals)):
            selected_vals = self.vals[:i]
            if len(selected_vals) < 2:
                continue
            std_dev = statistics.stdev(selected_vals)
            new_mean = statistics.mean(selected_vals)
            if -1*std_dev*2 < selected_vals[-1] - new_mean < std_dev*2:
                return True

        return False

    def hourly_update(self):
        self.vals.append(self.curr_val)
        return super().hourly_update()

    def post_daily_update(self):
        self.vals = []
        return super().post_daily_update()

    def output_format(self) -> Dict[str, Any]:
        return {
            "date": self.curr_date.strftime("%Y-%m-%dT%H:%M:%S"),
            "price": f"{self.curr_val:0.2f}",
            "dailyAverage": self.daily_average(),
            "dailyVariance": self.daily_variance(),
            "volatilityalert": self.volatility_alert()
        }


def schema1_handler(event, context):
    output_vals = list(Schema1().act(fetch_and_parse()))

    response = {
        "statusCode": 200,
        "body": json.dumps(output_vals)
    }

    return response


def schema2_handler(event, context):
    output_vals = list(Schema2().act(fetch_and_parse()))

    response = {
        "statusCode": 200,
        "body": json.dumps(output_vals)
    }

    return response


if __name__ == "__main__":
    print(list(Schema1().act(gen_data())))
    print(list(Schema2().act(gen_data())))

    print(list(Schema1().act(fetch_and_parse())))
    print(list(Schema2().act(fetch_and_parse())))

import dt.test
from dt_algos.curve import get_dates, get_rates
import datetime

def func(i):
    return i + 1

def test_dates():
    assert get_dates() == [datetime.datetime(2022, 1, 1),
                           datetime.datetime(2022, 7, 1),
                           datetime.datetime(2023, 1, 1)]

def test_rates():
    assert get_rates() == [1.0, 0.98, 0.95]

def __test_main__():
    dt.test.run()

if __name__ == '__main__':
    __test_main__()

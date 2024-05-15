import datetime
import numpy as np
from rateslib.curves import Curve
from rateslib.instruments import IRS

def get_curve(time, rate):
    usd_curve = Curve(nodes=dict(zip(time, rate)), calendar="nyc", id="sofr")
    return usd_curve

def get_IRS(effective=datetime.datetime(2022, 2, 15), termination='6m', notional=1000000000, fixed_rate=2.0):
    return IRS(
    effective=effective,
    termination=termination,
    notional=notional,
    fixed_rate=fixed_rate,
    spec="usd_irs")

def get_dates():
    return [datetime.datetime(2022, 1, 1),
            datetime.datetime(2022, 7, 1),
            datetime.datetime(2023, 1, 1)]

def get_rates():
    return [1.0, 0.98, 0.95]
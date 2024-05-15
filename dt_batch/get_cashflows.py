import os
import pandas as pd
from rateslib.curves import Curve

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    if isinstance(args[0], Curve):
        curve, instrument = args
    else:
        instrument, curve = args
    print(f'{instrument.spec=}')
    print(pd.DataFrame.from_records(curve.nodes, index=['rate']).T)
    return instrument.cashflows(curve)

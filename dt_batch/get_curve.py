from dt_algos.curve import get_curve, get_dates, get_rates
import datetime
import pandas as pd
import numpy as np

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    if 'start_date' in job_config and 'end_date' in job_config:
        start_date = datetime.datetime.strptime(job_config['start_date'], '%Y-%m-%d')
        end_date = datetime.datetime.strptime(job_config['end_date'], '%Y-%m-%d')
        dates = [ts.to_pydatetime() for ts in pd.date_range(start_date, end_date, freq='QS').to_list()]
        rates = list(np.linspace(1.0, 0.95, len(dates)))
        print(f'Getting curve for dates {start_date} to {end_date}')
        return get_curve(dates, rates)
    print('Getting default curve')
    return get_curve(get_dates(), get_rates())

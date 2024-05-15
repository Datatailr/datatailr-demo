from dt_algos.curve import get_IRS
import datetime

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    if 'effective' in job_config and isinstance(job_config['effective'], str):
        job_config['effective'] = datetime.datetime.strptime(job_config['effective'], '%Y-%m-%d')
    print(f'Getting instrument with: {job_config}')

    return get_IRS(**job_config)

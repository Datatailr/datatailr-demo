from dt_quantlib.utils import FredHandler

# batch that fetches info
def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    if job_config.get('api_key'):
        Fred = FredHandler(api_key=job_config['api_key'])
    else:
        # use default api key if none provided
        Fred = FredHandler()
    Fred.fetch_data_to_S3(job_config['countries'], job_config['start_date'], job_config['end_date'])
    return {}

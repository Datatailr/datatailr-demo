from dt_quantlib.utils import store_results_in_S3
import logging
import sys

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=f'%(asctime)s - sum_up_ir_risk.py - %(levelname)s - %(message)s')
logger = logging.getLogger()

# batch that sums up country-wise IR risk estimates
def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    risks_per_country = {}
    for arg in args:
        risks_per_country.update(arg)
    countries_count = len(risks_per_country.keys())
    logger.info(f'VaR were calculated for {countries_count} countries.')
    store_results_in_S3(risks_per_country)
    logger.info('Results were uploaded to S3.')
    return risks_per_country

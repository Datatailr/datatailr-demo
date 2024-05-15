# This is a demo batch job that can be run on the Datatailr platform.
# It demonstrates how to use the batch job API and how to log to the platform's logging system.
# To test the job, you can run it locally:
# Simply edit the configuration object in the __main__ block at the bottom of the file and run the script.

import datetime
import logging
import sys
import dt.log
import dt.user
import random
import traceback
import os
import time
import pkg_resources


MODULE_NAME = '.'.join(__file__.split('/'))
USER = os.environ.get('USER', 'root')
logger = logging.getLogger()

__version__ = '0.1.34'

image_build_date = None
if os.path.exists('/etc/image_build_time'):
    with open('/etc/image_build_time', 'r') as f:
        image_build_date = f.readlines()[0].rstrip()

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    print(f'{sys.flags=}')
    if image_build_date:
        print(f'{image_build_date=}')
    print(f'{job_config=}, type={type(job_config)}')
    print(f'{scheduled_time=}, type={type(scheduled_time)}')
    print(f'{sub_job_name=}')
    print(f'{rundate=}')
    print('-'  * 120)
    print('Arguments:')
    for i, arg in enumerate(args):
        print(i, arg)
    print('-'  * 120)
    print(f'{__version__=}')
    fail_probability = job_config.get('fail_probability', 0)
    print('-'  * 120)
    print('Some logging:')
    logger.debug('this is debug')
    logger.info('This is info')
    logger.warning('this is warning')
    logger.error('this is error')
    logger.critical('this is critical')
    if job_config.get('log_to_cloud', False):
        from dt.log import CloudLogger
        cloud_logger = CloudLogger(log_group='/datatailr/batches', stream_name='test_job')
        cloud_logger.debug(USER, 'this is debug')
        cloud_logger.info(USER, 'This is info')
        cloud_logger.warning(USER, 'this is warning')
        cloud_logger.error(USER, 'this is error')
        cloud_logger.critical(USER, 'this is critical')
    print('-'  * 120)
    print('Installed packages:')
    dists = [str(d).replace(" ","==") for d in pkg_resources.working_set]
    for i in dists:
        print(i)
    print('-'  * 120)
    if random.random() < fail_probability:
        raise ValueError(f'Unfortunately this run failed ({fail_probability=})')
    amount_of_naps = job_config.get('amount_of_naps', 0)
    nap_time = job_config.get('nap_time', 1)
    for i in range(amount_of_naps):
        print(f'This is nap number {i} out of {amount_of_naps}. Going to nap for {nap_time} seconds')
        time.sleep(nap_time)
    if 'blob_name' in job_config:
        from dt.cloud.blob_storage import BlobStorage
        dt.user.set_current_user(dt.user.signed_user())
        BlobStorage().put_object(b'This is a blob of data', job_config['blob_name'])
        print(f'Saved a blob to {job_config["blob_name"]}')

    if 'return_value' in job_config:
        return job_config['return_value']

    return [1, 2, 3, {'a': 999}, datetime.datetime.now()]


if __name__ == '__main__':
    config = {'A': 123,
              "test": True,
            #   This will store an object into a file on the blob storage
              'blob_name': 'blob_from_batch',
            #   This is useful for creating failing tasks and experimenting with reruns
              'fail_probability': 0.1,
            #   This is the value returned by the function and passed to downstream dependencies
              'return_value': 1,
            #   Add some duration to the job run
              'amount_of_naps': 1,
              'nap-time': 10
              }
    result = __batch_main__(None, datetime.datetime.now(), None, None, None, config, datetime.datetime.now().date(), 1, 2, 'a', 'b', 'c')
    print(f'{result=}')

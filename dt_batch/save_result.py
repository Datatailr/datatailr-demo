from dt.cloud.blob_storage import BlobStorage
import dt.user
import io
import datetime
import boto3
import dt.host

dt.user.set_current_user(dt.user.signed_user())

def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    data = args[0]
    blob = io.BytesIO()
    data.to_parquet(blob)
    print('Cashflows:')
    print(f'{data}')
    bucket_name = f'{dt.host.blob_store_prefix()}datatailr-user-data'
    file_name = f'curves/{rundate}'
    print(f'Saving data to {bucket_name}/{file_name}')
    try:
        session = boto3.Session(profile_name='blob-store-admin')
    except Exception:
        session = boto3.Session()
    s3_client = session.resource('s3')
    s3_client.Object(bucket_name, file_name).put(Body=blob.getvalue())

if __name__ == '__main__':
    from dt_algos.curve import *
    from get_curve import __batch_main__ as get_curve
    from get_instrument import __batch_main__ as get_irs
    from get_cashflows import __batch_main__ as get_cashflow

    curve = get_curve(None, None, None, None, None, {'start_date': '2022-01-01', 'end_date': '2024-01-01'}, None)
    irs = get_irs(None, None, None, None, None, {'effective': '2022-02-15', 'termination': '6m', 'notional': 1000000000, 'fixed_rate': 2.0}, None)
    cash_flow = get_cashflow(None, None, None, None, None, {}, None, irs, curve)
    __batch_main__(None, None, None, None, None, {}, datetime.datetime.now().date(), cash_flow)
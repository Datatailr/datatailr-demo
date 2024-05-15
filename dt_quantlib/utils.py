import dt.user
from fredapi import Fred
import dt.host
import io
import pandas as pd
import QuantLib as ql
import logging
import sys
from scipy.stats import norm
import os
import boto3
import pickle
import numpy as np

logging.basicConfig(level=logging.INFO, stream=sys.stdout, format=f'%(asctime)s - utils.py - %(levelname)s - %(message)s')
logger = logging.getLogger()
USER = dt.user.signed_user()
os.environ['HOME'] = f'/home/{USER.name}'

CALENDARS = {
        "US": ql.UnitedStates(ql.UnitedStates.GovernmentBond),
        "DE": ql.Germany(),
        "GB": ql.UnitedKingdom(),
        "CA": ql.Canada(),
        "AU": ql.Australia(),
        "AT": ql.TARGET(),  # TARGET is used for Eurozone countries
        "FR": ql.France(),
        "JP": ql.Japan(),
        "IT": ql.Italy(),
        "ES": ql.TARGET(),
        "IE": ql.TARGET(),
        "NL": ql.TARGET(),
        "CH": ql.Switzerland(),
        "CZ": ql.CzechRepublic(),
        "SI": ql.TARGET(),
        "SK": ql.Slovakia(),
        "HU": ql.Hungary(),
        "NO": ql.Norway(),
        "FI": ql.Finland(),
        "DK": ql.Denmark(),
        "IL": ql.Israel(),
        "PL": ql.Poland(),
        "KR": ql.SouthKorea(),
        "GR": ql.TARGET(),
        "LU": ql.TARGET(),
        "SE": ql.Sweden(),
        "BE": ql.TARGET(),
        "PT": ql.TARGET(),
        "NZ": ql.NewZealand(),
        "RU": ql.Russia(),
        "ZA": ql.SouthAfrica(),
    }

def fetch_data_from_S3(country_code):
    # This ensures you are signed in and will give access to the blob storage
    dt.user.set_current_user(USER)
    logger.info(f'Getting data for {country_code} from the S3...')
    try:
        session = boto3.Session(profile_name='blob-store-admin')
    except Exception:
        session = boto3.Session()
    s3 = session.resource('s3')
    bucket = dt.host.blob_store_prefix() + 'datatailr-user-data'
    file_name = f'quantlib_demo/IRLTLT01{country_code}M156N_data'
    obj = s3.Object(bucket, file_name).get()['Body']
    # Load the bytes as a parquet object into a dataframe
    loaded_data = pd.read_pickle(obj)
    logger.info('Requested data is fetched from the S3.')
    return loaded_data

def store_results_in_S3(results):
    logger.info('Uploading calculated VaR values to the S3...')
    # This ensures you are signed in and will give access to the blob storage
    dt.user.set_current_user(USER)
    try:
        session = boto3.Session(profile_name='blob-store-admin')
    except Exception:
        session = boto3.Session()
    s3 = session.resource('s3')
    bucket = dt.host.blob_store_prefix() + 'datatailr-user-data'
    # Create a blob of bytes
    blob = io.BytesIO()
    # Write the contents of your dataframe to the blob
    pickle.dump(results, blob)
    file_name = 'quantlib_demo/var_est_per_country_dict'
    s3.Object(bucket, file_name).put(Body=blob.getvalue())
    logger.info('Uploaded results to the S3.')
    return

def calculate_var(country_code):
    yield_data = fetch_data_from_S3(country_code)
    df = pd.DataFrame({'Date': yield_data.index, 'Value': yield_data.values})
    # Assuming normal distribution for simplicity
    confidence_level = 0.95
    volatility = df['Value'].std()
    VaR_95 = norm.ppf(1 - confidence_level, df['Value'].mean(), volatility)
    VaR_95 = np.abs(VaR_95 - df['Value'].mean())
    return VaR_95

def model_sample_bond(country_code, issue_date=None, maturity_date=None):
    logger.info(f'Pricing a hypothetical bond given {country_code} government bond yields.')
    # Step 1: Fetch Data
    yield_data = fetch_data_from_S3(country_code)
    start_date = yield_data.index.min()
    end_date = yield_data.index.max()
    df = pd.DataFrame({'Date': yield_data.index, 'Value': yield_data.values})
    dates = [ql.Date(d.day, d.month, d.year) for d in df['Date']]
    # Convert percentage yields to decimal
    yields = [y/100.0 for y in df['Value']]
    # Create the yield curve
    calendar = CALENDARS.get(
        country_code, ql.UnitedStates(ql.UnitedStates.GovernmentBond))
    day_count = ql.ActualActual(ql.ActualActual.ISMA)
    curve = ql.ZeroCurve(dates, yields, day_count, calendar)
    yield_curve_handle = ql.YieldTermStructureHandle(curve)
    # Define a hypothetical bond
    if (type(issue_date) is str) and (type(maturity_date) is str):
        issue_date = ql.Date(
            int(issue_date.split('-')[2]),
            int(issue_date.split('-')[1]),
            int(issue_date.split('-')[0])
            )
        maturity_date = ql.Date(
            int(maturity_date.split('-')[2]),
            int(maturity_date.split('-')[1]),
            int(maturity_date.split('-')[0])
            )
    elif issue_date is None and maturity_date is None:
        issue_date = ql.Date(
            start_date.day, start_date.month, start_date.year)
        maturity_date = ql.Date(
            end_date.day, end_date.month, end_date.year)
    else:
        issue_date = ql.Date(
            issue_date.day, issue_date.month, issue_date.year)
        maturity_date = ql.Date(
            maturity_date.day, maturity_date.month, maturity_date.year)
    coupon_rate = 0.025
    face_value = 100
    coupons = [coupon_rate]
    # Set the evaluation date to the bond's issue date
    ql.Settings.instance().evaluationDate = issue_date
    # Bond creation
    schedule = ql.Schedule(
        issue_date, maturity_date, ql.Period(ql.Annual), calendar, 
        ql.Unadjusted, ql.Unadjusted, ql.DateGeneration.Backward, False
        )
    fixed_rate_bond = ql.FixedRateBond(3, face_value, schedule, coupons, day_count)
    # Bond pricing
    bond_engine = ql.DiscountingBondEngine(yield_curve_handle)
    fixed_rate_bond.setPricingEngine(bond_engine)
    # Calculate the clean price
    bond_price = fixed_rate_bond.cleanPrice()
    logger.info(f'The price of the hypothetical bond is: {bond_price}, face value: {face_value}, coupon_rate: {coupon_rate}')
    return bond_price


class FredHandler:
    def __init__(self, api_key='66f3604634d69c01d5c7842e9b189287'):
        max_retries = 3
        for num_retry in range(1, max_retries + 1):
            try:
                self.fred = Fred(api_key=api_key)
                return
            except Exception:
                logger.error(f'Attempt #{num_retry} try to init Fred handler failed')
                logger.error('Retrying...' if num_retry <= max_retries else 'Reached max retries, terminating')

    def fetch_data_to_S3(self, countries, start_date='2013-08-01', end_date='2023-08-01'):
        logger.info(f'Fetching data for {len(countries)} countries from {start_date} to {end_date}...')
        try:
            session = boto3.Session(profile_name='blob-store-admin')
        except Exception:
            session = boto3.Session()
        s3 = session.resource('s3')
        bucket = dt.host.blob_store_prefix() + 'datatailr-user-data'
        for country, country_code in countries.items():
            logger.info(f'Fetching data for {country}...')
            # Create a blob of bytes
            blob = io.BytesIO()
            data = self.fred.get_series(f'IRLTLT01{country_code}M156N', start_date, end_date)
            # Write the contents of your dataframe to the blob
            data.to_pickle(blob)
            file_name = f'quantlib_demo/IRLTLT01{country_code}M156N_data'
            s3.Object(bucket, file_name).put(Body=blob.getvalue())
            logger.info(f'Uploaded data for {country} to S3.')
        logger.info('Requested data is fetched and stored in S3.')

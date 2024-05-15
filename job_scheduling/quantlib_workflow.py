from dt.scheduler.api import DAG, Schedule, Task

IMAGE = 'Dt Quantlib'
BASE_ENTRYPOINT = 'dt_quantlib.batch.'
SCHEDULE = Schedule(at_hours=[13], at_minutes=[45], timezone='CET')

START_DATE = '2013-08-01'
END_DATE = '2023-08-01'
COUNTRIES = {
    "USA": "US",
    "Germany": "DE",
    "United Kingdom": "GB",
    "Canada": "CA",
    "Australia": "AU",
    "Austria": "AT",
    "France": "FR",
    "Japan": "JP",
    "Italy": "IT",
    "Spain": "ES",
    "Ireland": "IE",
    "Netherlands": "NL",
    "Switzerland": "CH",
    "Czech Republic": "CZ",
    "Slovenia": "SI",
    "Slovak Republic": "SK",
    "Hungary": "HU",
    "Norway": "NO",
    "Finland": "FI",
    "Denmark": "DK",
    "Israel": "IL",
    "Poland": "PL",
    "Korea": "KR",
    "Greece": "GR",
    "Luxembourg": "LU",
    "Sweden": "SE",
    "Belgium": "BE",
    "Portugal": "PT",
    "New Zealand": "NZ",
    "Chile": "CL",
    "Russia": "RU",
    "South Africa": "ZA"
}

with DAG(Name='QuantLib batch', Tag='dev', Schedule=SCHEDULE) as dag:

    get_data = Task(
        Name='Data fetch',
        Image=IMAGE,
        Description=f'This job fetches the data for specified countries from FRED',
        dag=dag,
        Entrypoint=BASE_ENTRYPOINT + 'fetch_data',
        ConfigurationJson={'api_key': '66f3604634d69c01d5c7842e9b189287', 'countries': COUNTRIES, 'start_date': START_DATE, 'end_date': END_DATE})

    process_data = [Task(
        Name=f'{country_code} IR risk estimation',
        Image=IMAGE,
        Description=f'This job estimates IR risk for country {country_code}',
        dag=dag,
        Entrypoint=BASE_ENTRYPOINT + 'estimate_ir_risk',
        ConfigurationJson={'country_code': country_code}) for country_code in COUNTRIES.values()]

    upload_results = Task(
        Name='Process and upload results',
        Image=IMAGE,
        Description='This job sums up results of data processing tasks and uploads results to S3',
        dag=dag,
        Entrypoint=BASE_ENTRYPOINT + 'sum_up_ir_risk',
        ConfigurationJson={})

    get_data >> process_data >> upload_results

    dag.save()

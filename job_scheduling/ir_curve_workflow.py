from dt.scheduler.api import DAG, Task, ParallelTask, Schedule, Priority
import datetime


IMAGE = 'Datatailr Demo Batch'
ENTRYPOINT_BASE = 'dt_batch.'
SCHEDULE_IN_MINUTES_FROM_NOW = 2
scheduled_time = datetime.datetime.now() + datetime.timedelta(minutes=SCHEDULE_IN_MINUTES_FROM_NOW)

schedule = Schedule(at_minutes=[scheduled_time.minute],
                    # in_months=['Feb'], day_of_month=31,
                    at_hours=[scheduled_time.hour], timezone='UTC')

with DAG(Name=f'IR Curve Fit Cash Flow', Tag='dev', Schedule=schedule) as dag:
    start_date, end_date = '2022-01-01', '2024-01-01'
    get_curve = Task(Name='Get Curves',
                     Image=IMAGE,
                     Entrypoint= ENTRYPOINT_BASE + 'get_curve',
                     dag=dag,
                     Priority=Priority.NORMAL,
                     CountForPrewarming=False,
                     ConfigurationJson={'start_date': start_date, 'end_date': end_date},
                     Description=f'Get a curve object for the period {start_date} to {end_date}')

    get_instrument = Task(Name='Get Instrument',
                          Image=IMAGE,
                          Entrypoint=ENTRYPOINT_BASE + 'get_instrument',
                          dag=dag,
                          Priority=Priority.NORMAL,
                          CountForPrewarming=False,
                          ConfigurationJson={'effective': datetime.datetime(2022, 2, 15), 'termination': '6m', 'notional': 1000000000, 'fixed_rate': 2.0},
                          Description=f'Get an IRS instrument object')

    get_cashflows = Task(Name='Get Cashflows',
                         Image=IMAGE,
                         Entrypoint=ENTRYPOINT_BASE + 'get_cashflows',
                         dag=dag,
                         Priority=Priority.NORMAL,
                         CountForPrewarming=False,
                         Description=f'Get cashflows based on the instrument and IR curve')

    save_results = Task(Name='Save data',
                        Image=IMAGE,
                        Entrypoint=ENTRYPOINT_BASE + 'save_result',
                        dag=dag,
                        Priority=Priority.NORMAL,
                        CountForPrewarming=False,
                        Description=f'Save the result to a blob storage bucket')
    [get_curve, get_instrument] >> get_cashflows >> save_results
    dag.save()

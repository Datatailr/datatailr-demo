from dt_quantlib.utils import calculate_var, model_sample_bond

# batch that runs QuantLib stuff
def __batch_main__(sub_job_name, scheduled_time, runtime, part_num, num_parts, job_config, rundate, *args):
    var = calculate_var(country_code=job_config['country_code'])
    projected_bond_price = model_sample_bond(country_code=job_config['country_code'])
    result = {job_config['country_code']: {'VaR': var, 'Price': projected_bond_price}}
    return result

import sys
import dt.user
import dt.excel
from dt_quantlib.utils import calculate_var, model_sample_bond, fetch_data_from_S3

addin = dt.excel.Addin('QL helper', 'This Excel addin can call calculate_ir_risk function to get VaR for a country with specified country code.')

@addin.expose(description='Calculate VaR for a country', help='This function expects a country code to calculate VaR based on data in S3')
def get_var(country_code: str) -> float:
    return calculate_var(country_code)

@addin.expose(description='Get data for specified country from S3', help='This function expects a country code')
def get_country_data(country_code: str) -> list:
    data = fetch_data_from_S3(country_code)
    result = []
    for elem in data:
        result.append([elem])
    return result

@addin.expose(description='Get dates for specified country data from S3', help='This function expects a country code')
def get_country_dates(country_code: str) -> list:
    data = fetch_data_from_S3(country_code)
    result = []
    for i in range(len(data)):
        result.append([str(data.index[i]).split(' ')[0]])
    return result

@addin.expose(description='Price fixed rate bond', help='This function expects a country code to model a fixed rate bond given country government bond yields data')
def price_fixed_rate_bond(country_code: str, issue_date: str, maturity_date: str) -> float:
    return model_sample_bond(country_code, issue_date, maturity_date)

def __excel_main__(port, debug=False):
    addin.run(port=port)

if __name__ == '__main__':
    __excel_main__(int(sys.argv[1]))

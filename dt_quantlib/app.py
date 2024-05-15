import streamlit as st
import dt.streamlit
import plotly.express as px
from dt_quantlib.utils import fetch_data_from_S3, model_sample_bond
import dt.user
import pickle
import io
import sys
from dt.cloud.blob_storage import BlobStorage

USER = dt.user.signed_user()
dt.user.set_current_user(USER)

code_to_country = {
    'US': 'USA',
    'DE': 'Germany',
    'GB': 'United Kingdom',
    'CA': 'Canada',
    'AU': 'Australia',
    'AT': 'Austria',
    'FR': 'France',
    'JP': 'Japan',
    'IT': 'Italy',
    'ES': 'Spain',
    'IE': 'Ireland',
    'NL': 'Netherlands',
    'CH': 'Switzerland',
    'CZ': 'Czech Republic',
    'SI': 'Slovenia',
    'SK': 'Slovak Republic',
    'HU': 'Hungary',
    'NO': 'Norway',
    'FI': 'Finland',
    'DK': 'Denmark',
    'IL': 'Israel',
    'PL': 'Poland',
    'KR': 'Korea',
    'GR': 'Greece',
    'LU': 'Luxembourg',
    'SE': 'Sweden',
    'BE': 'Belgium',
    'PT': 'Portugal',
    'NZ': 'New Zealand',
    'CL': 'Chile',
    'RU': 'Russia',
    'ZA': 'South Africa'
}

@st.cache_data
def get_available_countries():
    available_countries = []
    s3 = BlobStorage()
    s3_items = s3.list_objects('quantlib_demo')
    for s3_item in s3_items:
        if s3_item.name.startswith('quantlib_demo/IRLTLT01') and s3_item.name.endswith('M156N_data'):
            country_code = s3_item.name.replace('quantlib_demo/IRLTLT01', '').replace('M156N_data', '')
            available_countries.append(country_code)
    return available_countries

@st.cache_data
def fetch_results():
    s3 = BlobStorage()
    try:
        loaded_blob = io.BytesIO(s3.get_object('quantlib_demo/var_est_per_country_dict'))
        results = pickle.load(loaded_blob)
        return results
    except:
        return {}

@st.cache_data
def fetch_data(country):
    data = fetch_data_from_S3(country)
    return data

def plot_country_data(country, data, var):
    plot = px.line(
        data, x=data.index, y=data, labels={'index': 'Date', 'y': 'Yield'},
        title=f"{code_to_country.get(country) if country in code_to_country else country} Government Bond Yields: 10-Year"
        )
    st.plotly_chart(plot, use_container_width=True)
    return

def main():
    st.set_page_config(layout="wide")
    col1, col2 = st.columns([0.6, 0.3], gap="large")
    # Plotting
    with col1:
        st.header('Datatailr demo')
        st.markdown('This Streamlit app can fetch data and results from S3 and plot them.')
        available_countries = get_available_countries()
        if st.button('Refresh available countries'):
            get_available_countries.clear()
            fetch_data.clear()
            available_countries = get_available_countries()
        selected_country = st.selectbox('Plot data for country', available_countries, key='selected_country')
        data = fetch_data(selected_country)
        results = fetch_results().get(selected_country)
        var = results.get('VaR')
        price = results.get('Price')
        plot_country_data(selected_country, data, var)
        st.text(f'Calculated VaR = {round(var, 3)}') if var else st.text('VaR calculation was not yet performed for the requested country')
        st.text(f'Price of a sample bond over the whole period = {round(price, 3)}') if price else st.text('Sample bond pricing was not yet performed for the requested country')
    # Pricing and data
    with col2:
        st.subheader('Pricing sample fixed rate bond')
        pricing_issue_date = st.selectbox('Sample bond issue date', data.index, key='issue_date_pricing')
        pricing_maturity_date = st.selectbox('Sample bond maturity date', data.index, index=len(data.index) - 1, key='maturity_date_pricing')
        modeled_price = model_sample_bond(selected_country, pricing_issue_date, pricing_maturity_date)
        st.text(f'The price of the hypothetical bond given {code_to_country.get(selected_country)} market = {round(modeled_price, 3)}')
        st.subheader(f'Data for {code_to_country.get(selected_country)}')
        st.dataframe(data, column_config={'_index': 'Date', '0': 'Yield'}, use_container_width=True)

application = dt.streamlit.Streamlit()

def __app_main__():
    return application

if __name__ == '__main__':
    application.run(port=int(sys.argv[1]))

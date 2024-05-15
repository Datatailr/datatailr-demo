'''
Copyright (c) 2022 - Datatailr Inc.
All Rights Reserved.
 
This file is part of Datatailr and subject to the terms and conditions
defined in 'LICENSE.txt'. Unauthorized copying and/or distribution
of this file, in parts or full, via any medium is strictly prohibited.
'''

import streamlit as st
import time
import numpy as np
import dt.streamlit
import sys

def main():
    progress_bar = st.sidebar.progress(0)
    status_text = st.sidebar.empty()
    last_rows = np.random.randn(1, 1)
    chart = st.line_chart(last_rows)

    for count in range(1, 101):
        new_rows = last_rows[-1, :] + np.random.randn(5, 1).cumsum(axis=0)
        status_text.text(f'{count}% Complete')
        chart.add_rows(new_rows)
        progress_bar.progress(count)
        last_rows = new_rows
        time.sleep(0.05)

    progress_bar.empty()
    st.button("Re-run")

application = dt.streamlit.Streamlit()

def __app_main__():
     return application

if __name__ == '__main__':
    application.run(port=int(sys.argv[1]))

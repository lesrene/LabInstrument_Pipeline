import streamlit as st
import pandas as pd
from utils.db_connection import get_connection, get_measurements_by_experiment, get_quarantine_logs, get_filtered_experiments

st.set_page_config(page_title="🔬 Lab Instrument Dashboard", layout="wide")

st.sidebar.header("Search Filters")

name_search = st.sidebar.text_input("Sample Name")

conn = get_connection()
operators = pd.read_sql("SELECT DISTINCT operator_name FROM Experiments", conn)['operator_name'].tolist()
conn.close()
selected_operator = st.sidebar.selectbox("Operator", ["All"] + operators)


date_range = st.sidebar.date_input("Date Range", [])

# setting up the filters
op_filter = None if selected_operator == "All" else selected_operator
start_d = date_range[0] if len(date_range) == 2 else None
end_d = date_range[1] if len(date_range) == 2 else None

tab1, tab2 = st.tabs(["📊 Experiment Browser", "⚠️ System Logs"])

with tab1:
    st.header("Experiment Analysis")
    
    results_df = get_filtered_experiments(name_search, op_filter, start_d, end_d)
    
    if not results_df.empty:
        st.write(f"Showing {len(results_df)} matching experiments")
        
        options = results_df.apply(lambda x: f"{x['experiment_id']}: {x['sample_name']}", axis=1)
        selected_run = st.selectbox("Select a run to visualize", options)
        
        selected_id = int(selected_run.split(":")[0])
        data_df = get_measurements_by_experiment(selected_id)
        
        st.line_chart(data_df, x="time_min", y="heat_flow_mw")
        
        with st.expander("View Raw Measurement Data"):
            st.dataframe(data_df)
    else:
        st.info("No experiments match your search criteria.")

with tab2:
    st.header("Quarantine & Error Logs")
    st.write("The following runs failed validation or encountered system errors.")
    
    error_df = get_quarantine_logs()
    
    if not error_df.empty:
        # highlights the row if the word 'Error' or 'FAILED' appears in the message
        styled_error_df = error_df.style.map(
            lambda x: 'background-color: #702020' if any(word in str(x) for word in ['FAILED', 'Error', 'Invalid']) else '',
            subset=['message']
        )
        
        st.dataframe(
            styled_error_df, 
            width=True,
            column_config={
                "timestamp": st.column_config.DatetimeColumn("Detected At", format="D MMM YYYY, h:mm a"),
                "file_name": "File Source",
                "message": "Reason for Quarantine"
            }
        )
    else:
        st.success("✨ System Healthy: No validation errors found.")



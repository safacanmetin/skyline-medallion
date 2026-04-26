import streamlit as st
from google.cloud import bigquery
import pandas as pd
import altair as alt

# Set page config for a professional look
st.set_page_config(page_title="Skyline Medallion Dashboard", layout="wide")

st.title("📊 Skyline Medallion: Bank Churn Analytics")

with st.sidebar:
    st.header("📌 Data Dictionary")
    st.markdown("""
    * **Young:** < 30 years old
    * **Middle-Aged:** 30 - 49 years old
    * **Senior:** 50+ years old""")
    st.divider()
    st.header("🛠 Tech Stack")
    st.write("- **Storage:** GCS")
    st.write("- **Engine:** Databricks")
    st.write("- **Warehouse:** BigQuery")

st.markdown("Real-time insights from GCP Storage via Databricks & BigQuery")

# Authenticate using the same method as before (gcloud auth)
client = bigquery.Client(project='project-x')

# Query to fetch data from Gold Layer in BigQuery
QUERY = """
    SELECT age_group, total_customers, churn_rate 
    FROM `project-x.skyline_bank_analytics.gold_churn_age_group`
    ORDER BY churn_rate DESC
"""

@st.cache_data
def load_data():
    query_job = client.query(QUERY)
    return query_job.to_dataframe()

try:
    df = load_data()

    # Layout with metrics
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Churn Rate by Age Group")

        # Create an Altair bar chart with specific sorting
        chart = alt.Chart(df).mark_bar().encode(
            x=alt.X('age_group:N', sort='-y', title='Age Groups', axis=alt.Axis(labelAngle=0)), # Sort by Y-axis (churn_rate) descending
            y=alt.Y('churn_rate:Q', title='Churn Rate'),
            tooltip=['age_group', 'churn_rate']
        ).properties(height=400)

        st.altair_chart(chart, use_container_width=True)
    
    with col2:
        st.subheader("Raw Gold Data")
        st.dataframe(df, use_container_width=True)

    st.success("Analysis based on the latest Gold Layer transformation.")

except Exception as e:
    st.error(f"Error connecting to BigQuery: {e}")
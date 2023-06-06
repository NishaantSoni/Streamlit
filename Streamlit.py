import streamlit as st
import pandas as pd 
import snowflake.connector
from functools import wraps
import matplotlib.pyplot as plt
import plotly.express as px
import time



s_account = 'hs58126.central-india.azure'
s_user = 'NishaantSoni'
s_password = 'Htek@#1010'
s_warehouse = 'COMPUTE_WH'
s_database = 'SNOWFLAKE'
s_schema = 'ACCOUNT_USAGE'

conn = snowflake.connector.connect(user = s_user, password = s_password, account = s_account,warehouse = s_warehouse, database = s_database, schema = s_schema)

@st.cache_data(ttl = 1800)
def query2(): 
    return pd.read_sql_query('''
SELECT SUM(CREDITS_USED)
FROM SNOWFLAKE.ACCOUNT_USAGE.METERING_HISTORY;
''', conn)

@st.cache_data(ttl = 1800)
def query3():
    return pd.read_sql_query('''
select
    count(*) as number_of_jobs
from
    snowflake.account_usage.query_history
where
    start_time >= date_trunc(month, current_date);
''', conn)

@st.cache_data(ttl = 1800)
def query4():
    return pd.read_sql_query('''
select
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(10, 9) as billable_gb
from
    account_usage.storage_usage
where
    USAGE_DATE = current_date() -1;
''', conn)

@st.cache_data(ttl = 1800)
def query5(): 
    return pd.read_sql_query('''
select
    warehouse_name,
    sum(credits_used) as total_credits_used
from
    account_usage.warehouse_metering_history
group by
    1
order by
    2 desc;
''', conn)

@st.cache_data(ttl = 1800)
def query6(): 
    return pd.read_sql_query('''
select
    start_time::date as usage_date,
    warehouse_name,
    sum(credits_used) as total_credits_used
from
    account_usage.warehouse_metering_history
group by
    1,
    2
order by
    2,
    1;
''', conn)

@st.cache_data(ttl = 1800)
def query7(): 
    return pd.read_sql_query('''
select
    query_type,
    warehouse_size,
    avg(execution_time) / 1000 as average_execution_time
from
    account_usage.query_history
group by
    1,
    2
order by
    3 desc;
''', conn)

@st.cache_data(ttl = 1800)
def query8():
    return pd.read_sql_query('''
select
    query_id,
    left(query_text,60) as query_text,
    (total_elapsed_time / 1000) as total_elapsed_time
from
    account_usage.query_history
where
    execution_status = 'SUCCESS'
order by
    total_elapsed_time desc
limit
    15;
''', conn)

@st.cache_data(ttl = 1800)
def query9():
    return pd.read_sql_query('''
select
    date_trunc('MONTH', usage_date) as Usage_Month,
    sum(CREDITS_BILLED)
from
    account_usage.metering_daily_history
group by
    Usage_Month;
''', conn)

@st.cache_data(ttl = 1800)
def query10():
    return pd.read_sql_query('''
select
    warehouse_name,
    sum(credits_used_cloud_services) credits_used_cloud_services,
    sum(credits_used_compute) credits_used_compute,
    sum(credits_used) credits_used
from
    account_usage.warehouse_metering_history
where
    true
group by
    1
order by
    2 desc
limit
    10;
''', conn)

@st.cache_data(ttl = 1800)
def query11():
    return pd.read_sql_query('''
select
    *
from
    account_usage.metering_daily_history;
''', conn)


@st.cache_data(ttl = 1800)
def query12():
    return pd.read_sql_query('''
select
    date_trunc(month, usage_date) as usage_month,
    avg(storage_bytes + stage_bytes + failsafe_bytes) / power(1024, 4) as billable_tb,
    avg(storage_bytes) / power(1024, 4) as Storage_TB,
    avg(stage_bytes) / power(1024, 4) as Stage_TB,
    avg(failsafe_bytes) / power(1024, 4) as Failsafe_TB
from
    account_usage.storage_usage
group by
    1
order by
    1;
''', conn)


def query13():
    return pd.read_sql_query('''
select
    user_name,
    query_id,
    query_text,
    query_type,
    execution_status,
    warehouse_name,
    start_time,
    end_time,
    (total_elapsed_time / 1000) as total_elapsed_time
from
    account_usage.query_history
order by 
    total_elapsed_time desc
limit
    100;
''', conn)




valid_username = "NishaantSoni"
valid_password = "Htek@#1010"

st.set_page_config("ADMIN DASHBOARD", layout='wide')

def login():
    st.title("Snowflake Login Details")
    #st.write("Click on Log in twice to access the dashboard")


    username1 = st.text_input("Username")
    password1 = st.text_input("Password", type="password")


    if st.button("Log in"):
        if username1 == valid_username and password1 == valid_password:
            st.session_state.authenticated = True
            st.experimental_set_query_params()
            st.empty()
            st.info("Log in credentials are correct")
            st.write("\n")
            st.info("Please click on Log in button again to access the Admin Dashboard")
        else:
            st.error("Invalid username or password")


def authenticate(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if st.session_state.get("authenticated"):
            func(*args, **kwargs)
        else:
            login()
    return wrapper

@authenticate
def costnomics1():
    st.title("")

@authenticate
def costnomics2():
    st.title("Welcome to CostNomics!")
    st.write("Please select one of the options you wish to see")

@authenticate
def show_credits_consumed():
    st.info("CREDITS USED")
    st.metric("Credits Used = ", value=query2()['SUM(CREDITS_USED)'].values)
    st.write("\n")
    #st.bar_chart(data=query2(), width=20, height=300, use_container_width=True)

    st.info("CREDITS USED DAILY BY WAREHOUSES AND CLOUD SERVICES \n")
    st.write("\n")
    with st.container():
        cols = st.columns([1,1])
        with cols[0]:
            st.info("CREDITS USED DAILY BY WAREHOUSES")
            fig1 = px.bar(query6(), x="USAGE_DATE", y="TOTAL_CREDITS_USED", color='WAREHOUSE_NAME', color_discrete_sequence=[
                        "orange", "red", "green", "purple"])
            st.plotly_chart(fig1, use_container_width=True)
            st.write("\n")

        with cols[1]:
            st.info("CREDIT USED DAILY BY CLOUD AND COMPUTE SERVICES")
            fig3 = px.bar(query11(), x="USAGE_DATE", y=["CREDITS_ADJUSTMENT_CLOUD_SERVICES","CREDITS_USED_CLOUD_SERVICES","CREDITS_USED_COMPUTE"], color_discrete_sequence=[
                        "orange", "red", "green", "purple"])
            st.plotly_chart(fig3, use_container_width=True)
            st.write("\n")
    #st.bar_chart(data=df6, width=20, height=300, use_container_width=True)

    st.info("CREDITS BILLED BY MONTH")
    #st.text_input("Credits billed by month", str(pd_df9["SUM(CREDITS_BILLED)"]))
    #fig2 = px.bar(query9(), x="USAGE_MONTH", y="SUM(CREDITS_BILLED)")
    fig2 = px.pie(query9(), names="USAGE_MONTH", values="SUM(CREDITS_BILLED)", color_discrete_sequence=[
                 "orange", "red", "green", "purple"])
    st.plotly_chart(fig2, use_container_width=True)
    st.write("\n")
    #st.bar_chart(data=df9, width=20, height=300, use_container_width=True)

    #st.info("CREDIT BREAKDOWN BY DAY")
    #fig3 = px.bar(query11(), x="USAGE_DATE", y=["CREDITS_ADJUSTMENT_CLOUD_SERVICES","CREDITS_USED_CLOUD_SERVICES","CREDITS_USED_COMPUTE"], color_discrete_sequence=[
    #             "orange", "red", "green", "purple"])
    #st.plotly_chart(fig3, use_container_width=True)
    #st.write("\n")
    #st.bar_chart(data=df11, width=20, height=300, use_container_width=True)
    

@authenticate
def show_storage_and_job_details():
    with st.container():
        cols1 = st.columns([1.5,1])
        with cols1[0]:
            st.info("CURRENT BILLABLE STORAGE (GB)")
            st.metric("Current Billable Storage in GB = ", value = query4()['BILLABLE_GB'].values)
            st.write("\n")
            #st.bar_chart(data=query4(), width=20, height=300, use_container_width=True)

        with cols1[1]:
            st.info("TOTAL JOBS EXECUTED")
            st.metric("Number of Jobs = ", value=query3()["NUMBER_OF_JOBS"].values)
            st.write("\n")
            #st.bar_chart(data=query3(), width=20, height=300, use_container_width=True)


    st.info("DATA STORAGE USED OVERTIME")
    fig5 = px.bar(query12(), x="USAGE_MONTH", y = ["BILLABLE_TB", "STORAGE_TB", "STAGE_TB", "FAILSAFE_TB"], color_discrete_sequence=[
                 "orange", "red", "green", "purple"])
    st.plotly_chart(fig5, use_container_width=True)
    st.write("\n")
    #st.bar_chart(data=df12, width=20, height=300, use_container_width=True)

    #st.info("TOTAL JOBS EXECUTED")
    #st.metric("Number of Jobs = ", value=query3()["NUMBER_OF_JOBS"].values)
    #st.write("\n")
    #st.bar_chart(data=query3(), width=20, height=300, use_container_width=True)

@authenticate
def show_warehouse_details():
    st.info("CREDITS USAGE BY WAREHOUSE")
    #fig7 = px.bar(query5(), x="TOTAL_CREDITS_USED", y="WAREHOUSE_NAME")
    fig7 = px.pie(query5(), values="TOTAL_CREDITS_USED", names="WAREHOUSE_NAME", color_discrete_sequence=[
                 "orange", "red", "green", "purple"])
    st.plotly_chart(fig7, use_container_width=True)
    st.write("\n")
    #st.bar_chart(data = df5, width=20, height=300, use_container_width=True)

    st.info("COMPUTE AND CLOUD SERVICES BY WAREHOUSE")
    fig8 = px.bar(query10(), x="WAREHOUSE_NAME", y=["CREDITS_USED_CLOUD_SERVICES","CREDITS_USED_COMPUTE"], color_discrete_sequence=[
                 "orange", "red", "green", "purple"])
    st.plotly_chart(fig8, use_container_width=True)
    st.write("\n")
    #st.bar_chart(data=df10, width=20, height=300, use_container_width=True)

@authenticate
def show_query_details():
    st.info("EXECUTION TIME BY QUERY TYPE (AVG)")
    #fig9 = px.bar(query7(), y="QUERY_TYPE", x="AVERAGE_EXECUTION_TIME")
    fig9 = px.pie(query7(), names="QUERY_TYPE", values="AVERAGE_EXECUTION_TIME", width = 250, height=500)
    st.plotly_chart(fig9, use_container_width=True)
    st.write("\n")
    #st.bar_chart(data=df7, width=20, height=600, use_container_width=True)

    st.info("TOP 15 LONGEST RUNNING QUERIES")
    fig10 = px.bar(query8(), x="TOTAL_ELAPSED_TIME", y="QUERY_TEXT", color_discrete_sequence=[
                 "orange", "red", "green", "purple"])
    st.plotly_chart(fig10, use_container_width=True)
    st.write("\n")
    st.write("For more information, please see the details provided in the table below. \n")
    #st.bar_chart(data=df8, width=20, height=600, use_container_width=True)

    st.write("\n")
    st.info("QUERY PROFILE")
    st.dataframe(data=query13())


#@authenticate
#def show_logins():
    #st.title("LOGINS BY USER")
    #st.bar_chart(data=df13, width=20, height=300, use_container_width=True) 


@authenticate
def main_page():
    st.header("Welcome to the Snowflake Admin Dashboard")
    st.title("\n")
    st.info("Please select the CostNomics tab to see the CostNomics details.")



#pages = {"Home Page":main_page, "CREDITS CONSUMED":show_credits_consumed,
#         "STORAGE AND JOBS DETAILS":show_storage_and_job_details,"WAREHOUSE USAGE DETAILS":show_warehouse_details,
#         "QUERY DETAILS":show_query_details}





@authenticate
@st.cache_data()
def main():
    #st.title("Welcome to the Admin Dashboard")
    #st.sidebar.title("ADMIN DASHBOARD CONTENTS")
    tabs1 = st.tabs(["HOME PAGE", "COSTNOMICS"])
    with tabs1[0]:
        main_page()
    with tabs1[1]:
        tabs2 = st.tabs(["CREDITS DETAILS", "STORAGE AND JOBS DETAILS", "WAREHOUSE USAGE DETAILS", "QUERY DETAILS"])
        with tabs2[0]:
            st.write("\n")
            show_credits_consumed()
        with tabs2[1]:
            st.write("\n")
            show_storage_and_job_details()
        with tabs2[2]:
            st.write("\n")
            show_warehouse_details()
        with tabs2[3]:
            st.write("\n")
            show_query_details()



main()
conn.close()

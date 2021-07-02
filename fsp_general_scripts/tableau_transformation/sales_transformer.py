""" Still WIP, adding operations and uploading to fsp_sql. Will eventually be deployed to Heroku and part of fsp_bi_hub.
Later version will use more advanced queries to transform data in database rather than in Python. Need to test performance before implementing

Example Query Below: fix quotes/commenting when implemented:


sold_query = "SELECT closedate::DATE "date", COALESCE(CASE WHEN market_region = '' THEN 'OST' END,market_region) market_region,
        COALESCE(CASE WHEN cac_category_lead_source = '' THEN 'Other' END,cac_category_lead_source) cac_category,
        SUM(COALESCE(CASE WHEN powerwall_split_portion != 'Yes' THEN 1 END,0)) amount, 'Contracts' object_type,'Actual' data_type, SUM(amount_in_home_currency) sale_price,
        SUM(system_size) system_size
        FROM deals_cleaned
        WHERE closedate >= '2020-1-1' AND closedate < NOW()::DATE
        GROUP BY
        closedate::DATE, market_region,cac_category_lead_source
        ORDER BY closedate::DATE,market_region,cac_category_lead_source;"

sold_df = query_selector(sold_query,['date','market_region','cac_category', 'amount','object_type','data_type','sale_price','system_size'],'sold_cleaned',return_df=True,use_query=True)
sold_df['date'] = pd.to_datetime(sold_df['date'],errors='coerce').dt.date

"""


import psutil
import json
import requests
import pandas as pd
import numpy as np
import psycopg2
from time import sleep
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import os
import pickle
import sqlalchemy
from sqlalchemy import create_engine, insert, update, delete, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, select, BigInteger
import datetime
from datetime import timedelta
import calendar


base_route = "C:\\Users\\myate\\Desktop\\Python\\fsp_general"

variables_path_base = base_route+'\\env_variables\\'
data_path_base = base_route+'\\data_dumps\\'
sheets_token = variables_path_base + 'token.pickle'

    
with open(variables_path_base + 'heroku_environment_variables.pickle', 'rb') as token:
            sunrise_creds = pickle.load(token)


from IPython.core.display import display, HTML
display(HTML("<style>.container { width:80% !important; }</style>"))

def google_pull(sheet_id,target_range):
    if os.path.exists(sheets_token):
        with open(sheets_token, 'rb') as token:
            creds = pickle.load(token)
    service = build('sheets', 'v4', credentials=creds)

    # Call the Sheets API
    sheet = service.spreadsheets()
    a = sheet.values().get(spreadsheetId=sheet_id,
                                        range=target_range).execute()
    results_google = a.get('values',[])
    return results_google


def google_update(sheet_id,target_range,data,clear_range):
    if os.path.exists(sheets_token):
        with open(sheets_token, 'rb') as token:
            creds = pickle.load(token)
    service = build('sheets', 'v4', credentials=creds)
    body_update = {'values': data}

    # Call the Sheets API
    sheet = service.spreadsheets()
    request = service.spreadsheets().values().clear(spreadsheetId=sheet_id, range=clear_range).execute()
    result = sheet.values().update(spreadsheetId=sheet_id, range = target_range,
                                    body=body_update,valueInputOption='USER_ENTERED').execute()
    
hs_api_key = sunrise_creds['hapikey']
db_database = sunrise_creds['database']
db_user = sunrise_creds['user']
db_password = sunrise_creds['password']
db_host = sunrise_creds['host']

def query_selector(query,cols,table,return_df=False):
    conn = psycopg2.connect(database=sunrise_creds['database'], user=sunrise_creds['user'], 
                        password = sunrise_creds['password'],
                        host = sunrise_creds['host'])
    cur = conn.cursor()
    cur.execute("rollback;")

    if cols == '*':
        final_query = "SELECT * FROM [table]"
        final_query = final_query.replace('[table]', table)
        col_query = "Select * FROM [table] LIMIT 0"
        cur.execute(col_query.replace('[table]',table))
        colnames = [desc[0] for desc in cur.description]
    else:
        query_build = ', '.join(cols)
        final_query = query.replace('[cols]', query_build).replace('[table]', table)
        col_query = "Select [cols] FROM [table] LIMIT 0"
        col_query = col_query.replace('[cols]', query_build).replace('[table]', table)
        cur.execute(col_query.replace('[table]',table))
        colnames = [desc[0] for desc in cur.description]
    cur.execute(final_query)
    data = cur.fetchall()
    cur.close()
    conn.close()
    if return_df:
        returned = pd.DataFrame(data=data,columns=colnames)
        del data
        return returned
    else:
        data.insert(0,colnames)
        return data

print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")


def general_build():
    
    base_query = "SELECT [cols] FROM [table] "
    contacts_query = base_query + """WHERE five9_create_date >= '2020-1-1'"""
    contacts_df = query_selector(contacts_query,['five9_create_date','market_region','n3pl_lead_source'],'contacts',return_df=True)
    contacts_df.rename(columns={'n3pl_lead_source': 'lead_source','five9_create_date': 'date'},inplace=True)
    contacts_df['date'] = pd.to_datetime(contacts_df['date'],errors='coerce').dt.date
    contacts_df['amount'] = 1
    contacts_df = contacts_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    contacts_df['object_type'] = 'Contact'
    contacts_df['data_type'] = 'Actual'
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")

    appointments_query = base_query + """WHERE appointment_date >= '2020-1-1' AND powerwall_split_portion != 'Yes'"""
    appointments_df = query_selector(appointments_query,['appointment_date','market_region','cac_category_lead_source'],'appointments_cleaned',return_df=True)
    appointments_df.rename(columns={'appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
    appointments_df['date'] = pd.to_datetime(appointments_df['date'],errors='coerce').dt.date
    appointments_df['amount'] = 1
    appointments_df = appointments_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    appointments_df['object_type'] = 'Appointment'
    appointments_df['data_type'] = 'Actual'
    
    merged_df = pd.concat([contacts_df,appointments_df],axis=0)
    del contacts_df
    del appointments_df
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")
    
    ran_query = base_query + """ WHERE run_status = 'Run' AND appointment_date >= '2020-1-1' AND powerwall_split_portion != 'Yes'"""
    ran_df = query_selector(ran_query,['appointment_date','market_region','cac_category_lead_source'],'appointments_cleaned',return_df=True)
    ran_df.rename(columns={'appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
    ran_df['date'] = pd.to_datetime(ran_df['date'],errors='coerce').dt.date
    ran_df['amount'] = 1
    ran_df = ran_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    ran_df['object_type'] = 'Ran Appt'
    ran_df['data_type'] = 'Actual'    
    
    merged_df = pd.concat([merged_df,ran_df],axis=0)
    del ran_df
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")

    sold_query = base_query + """WHERE dealstage = 'Closed Won' AND closedate >= '2020-1-1' AND powerwall_split_portion != 'Yes'"""
    sold_df = query_selector(base_query,['closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size'],'deals_cleaned',return_df=True)
    sold_df.rename(columns={'closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
    sold_df['date'] = pd.to_datetime(sold_df['date'],errors='coerce').dt.date
    sold_df['amount'] = 1
    sold_df = sold_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    sold_df['object_type'] = 'Contracts'
    sold_df['data_type'] = 'Actual'    
    
    merged_df = pd.concat([merged_df,sold_df],axis=0)
    del sold_df
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")
    
    aged_query = base_query + """WHERE dealstage = 'Closed Lost' AND closedate >= '2020-1-1' AND powerwall_split_portion != 'Yes'"""
    aged_cancels_df = query_selector(aged_query,['closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size'],'deals_cleaned',return_df=True)
    aged_cancels_df.rename(columns={'closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
    aged_cancels_df['date'] = pd.to_datetime(aged_cancels_df['date'],errors='coerce').dt.date
    aged_cancels_df['amount'] = 1
    aged_cancels_df = aged_cancels_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    aged_cancels_df['object_type'] = 'Aged Cancels'
    aged_cancels_df['data_type'] = 'Actual'  
    
    merged_df = pd.concat([merged_df,aged_cancels_df],axis=0)
    del aged_cancels_df 
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")

    current_cancel_query = base_query + """WHERE dealstage= 'Closed Lost' AND AND closed_lost_date >= '2020-1-1' AND powerwall_split_portion != 'Yes'"""
    current_cancels_df = query_selector(base_query,['closed_lost_date','market_region','cac_category_lead_source','amount_in_home_currency','system_size'],'cancels_cleaned',return_df=True)
    current_cancels_df.rename(columns={'closed_lost_date': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
    current_cancels_df['date'] = pd.to_datetime(current_cancels_df['date'],errors='coerce').dt.date
    current_cancels_df['amount'] = 1
    current_cancels_df = current_cancels_df.groupby(['date','lead_source','market_region'],as_index=False).sum()
    current_cancels_df['object_type'] = 'Current Cancels'
    current_cancels_df['data_type'] = 'Actual'    
    
    merged_df = pd.concat([merged_df,current_cancels_df],axis=0)
    del current_cancels_df
    
    print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")


    merged_df['sale_price'].fillna(0,inplace=True)
    merged_df['system_size'].fillna(0,inplace=True)
     
    merged_df = merged_df[['date','market_region','lead_source','object_type','data_type','sale_price','system_size','amount']]
    
    merged_df = merged_df.reset_index(drop=True)
    merged_df.fillna('',inplace=True)

    return merged_df

sales_df = general_build()

values_list = sales_df.values.tolist()

sales_df['date'] = pd.to_datetime(sales_df['date'].replace({pd.NaT:'', None: '',np.nan: ''}),errors='coerce')
sales_df['sale_price'] = sales_df['sale_price'].astype('float64')
sales_df['system_size'] = sales_df['system_size'].astype('float64')

# cancels = cancels_build()
# values_list = cancels.values.tolist()
# cancels['date'] = pd.to_datetime(cancels['date'],errors='coerce')
# cancels['sale_price'] = cancels['sale_price'].astype('float64')
# cancels['system_size'] = cancels['system_size'].astype('float64')
# cancels.to_csv(data_path_base + 'cancels_tableau.csv', index=False)

today = datetime.datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
today_day = today.day
today_month = today.month
today_year = today.year

current_month_days = calendar.monthrange(today_year,today_month)[1]
current_month_days_remaining = current_month_days - today_day + 1 #add back a day since today is not included
two_weeks_ago = today - timedelta(days=14)

# Setting up regions and lead sources
regions = list(sales_df['market_region'].unique())
lead_sources = list(sales_df['lead_source'].unique())

average_list = []
for x in regions:
    for i in lead_sources:
        count = sales_df[(sales_df['market_region']==x) & (sales_df['lead_source']==i) & (sales_df['date']>=two_weeks_ago) & (sales_df['date']<today) & (sales_df['object_type']=='Contact')]['date'].count()
        average = round(count/14,2)
        entry = [x,i,average]
        average_list.append(entry)

def generate_indicative(object_type, df):

    from datetime import timedelta
    import calendar

    today = datetime.datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
    today_day = today.day
    today_month = today.month
    today_year = today.year

    current_month_days = calendar.monthrange(today_year,today_month)[1]
    current_month_days_remaining = current_month_days - today_day + 1 #add back a day since today is not included
    two_weeks_ago = today - timedelta(days=14)

    current_month_indicative_dates = []
    for i in range(today_day,current_month_days+1):
        date = str(today_year) + '-' + str(today_month) + '-' + str(i)
        current_month_indicative_dates.append(date)


    regions = list(df['market_region'].unique())
    lead_sources = list(df['lead_source'].unique())

    average_list = []
    for x in regions:
        for i in lead_sources:
            count = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['date'].count()
            sum_price = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['sale_price'].sum()
            sum_size = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['system_size'].sum()
            average_count = round(count/14,2)
            average_price = round(sum_price/14,2)
            average_size = round(sum_size/14,2)
            entry = [x,i,average_price,average_size,average_count]
            average_list.append(entry)

    id_increment = 1
    current_month_indicative_forecast = []
    for x in current_month_indicative_dates:
        for i in average_list:
            market = i[0]
            lead_source = i[1]
            sum_price = i[2]
            sum_size = i[3]
            average_count = i[4]
            entry = [x,market,lead_source,object_type,'Indicative',sum_price,sum_size,average_count]
            current_month_indicative_forecast.append(entry)
            id_increment += 1
            
            
    current_month_indicative_forecast.insert(0,list(df.columns.values))
    indicative_df = pd.DataFrame(data=current_month_indicative_forecast[1::],columns=current_month_indicative_forecast[0])
    del current_month_indicative_forecast
    return indicative_df

indicative_contact_df = generate_indicative("Contact", sales_df)
indicative_appointment_df = generate_indicative("Appointment", sales_df)
indicative_ran_df = generate_indicative("Ran Appt", sales_df)
indicative_sold_df = generate_indicative("Contracts", sales_df)
indicative_current_cancel_df = generate_indicative("Current Cancels", sales_df)
indicative_aged_cancel_df = generate_indicative("Aged Cancels", sales_df)

print(f"Using {psutil.virtual_memory()[3]/ (1024.0**2)}")



sales_df = pd.concat([sales_df,indicative_contact_df,indicative_appointment_df,indicative_ran_df,indicative_sold_df,indicative_current_cancel_df,
    indicative_aged_cancel_df],axis=0).fillna('')
sales_df['date'] = pd.to_datetime(sales_df['date'],errors='coerce')
sales_df.to_excel(data_path_base + 'sales_df_test.xlsx', index=False)

print('Transformation Complete')
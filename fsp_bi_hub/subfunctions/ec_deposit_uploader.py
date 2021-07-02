import numpy as np
import pandas as pd
import os, json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import psycopg2


token = os.environ['sheets_token']
scopes = ['https://spreadsheets.google.com/feeds']


creds_dict = json.loads(token)
creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
client = gspread.authorize(creds)

def query_selector(query,cols,table):
    conn = psycopg2.connect(database=os.environ['database'], user=os.environ['user'], 
                        password = os.environ['password'],
                        host = os.environ['host'])
    cur = conn.cursor()
    cur.execute("rollback;")

    if cols == '*':
        final_query = "SELECT * FROM [table]"
        final_query = final_query.replace('[table]', table)
        col_query = "Select * FROM [table] LIMIT 0"
        cur.execute(col_query.replace('[table]',table))
    else:
        query_build = ', '.join(cols)
        final_query = query.replace('[cols]', query_build).replace('[table]', table)
    
    cur.execute(final_query)
    colnames = [desc[0] for desc in cur.description]
    data = cur.fetchall()
    data.insert(0,tuple(colnames))
    cur.close()
    conn.close()
    return data

query = """SELECT a.project_sunrise_id project_sunrise_id, dealname, hubspot_owner_id,payment_option,amount_in_home_currency,market_region,
    confirm_deposit_sent_to_accounting_completed,confirm_deposit_received_completed FROM tasks_wide a
    LEFT JOIN (SELECT project_sunrise_id, hubspot_owner_id FROM deals_cleaned) b
    ON a.project_sunrise_id = b.project_sunrise_id
    WHERE a.closedate >= '2020-1-1'"""
upload_raw = query_selector(query,['empty_list'],'tasks_wide')
df = pd.DataFrame(data=upload_raw[1::],columns=upload_raw[0])
df.fillna('',inplace=True)

def date_stripper(x):
    if x == '' or x is None:
        return ''
    else:
        return x.strftime('%#m/%#d/%Y')
    
for x in ['confirm_deposit_sent_to_accounting_completed','confirm_deposit_received_completed']:
    df[x] = df[x].apply(date_stripper)
    
def null_handler(x):
    if x == '':
        return 0
    elif x is None:
        return 0
    else:
        return x

def float_converter(column_array,df):
    for x in column_array:
        df[x].fillna(0.0,inplace=True)
        df[x].replace({'':0.0},inplace=True)
        df[x] = df[x].apply(null_handler)
        df[x] = df[x].apply(lambda x: str(x).replace(',','').replace('$',''))
        df[x] = df[x].astype('float64')
        df[x] = df[x].apply(lambda x: round(x,2))
        
float_converter(['amount_in_home_currency'],df)



sheet_id = os.environ['deposit_checker_id']
ws = client.open_by_key(sheet_id).worksheet('All Projects')
client.open_by_key(sheet_id).values_clear("All Projects!A:I")

ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")

print('Deposits Checker Updated!')
def duplicate_check():
    import json
    import requests
    import pandas as pd
    import numpy as np
    import psycopg2
    from time import sleep
    from oauth2client.service_account import ServiceAccountCredentials
    import os
    import pickle
    import datetime
    import gspread

    # authorizing gspread
    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']


    creds_dict = json.loads(token)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)

    # checks to see if there are any closed deals from HS that have the same lead ID
    deals_sheet_id = os.environ['appointments_sheet_id']
    deals_ws = client.open_by_key(deals_sheet_id).worksheet('Closed Deals Export')
    deals_raw = deals_ws.get("A:AD")  
    duplicate_df = pd.DataFrame(data=deals_raw[1::],columns = deals_raw[0])
    duplicate_df = duplicate_df[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','amount_in_home_currency','closedate']]
    duplicate_df = duplicate_df[duplicate_df.project_sunrise_id.isnull()==False]
    duplicate_df['project_sunrise_id'] = duplicate_df['project_sunrise_id'].apply(lambda x: str(x).replace(',',''))
    duplicate_df['project_sunrise_id'] = duplicate_df['project_sunrise_id'].astype('int64')
    duplicate_df['project_sunrise_id'] = duplicate_df['project_sunrise_id'].apply(lambda x: str(x))
    duplicate_df = duplicate_df[(duplicate_df.project_sunrise_id != '') & (duplicate_df.project_sunrise_id.isnull()==False)].copy()
    duplicate_df = duplicate_df[(duplicate_df.duplicated(keep=False, subset = 'project_sunrise_id'))].sort_values(by='project_sunrise_id',ascending=True)
    duplicate_df = duplicate_df[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','closedate']]
    duplicate_df['closedate'] = pd.to_datetime(duplicate_df['closedate'],errors='coerce')
    duplicate_df['closedate'] = duplicate_df['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    duplicate_df.fillna('',inplace=True)

    sheet_id = os.environ['appointments_sheet_id']
    ws = client.open_by_key(sheet_id).worksheet('Duplicate Deals')
    client.open_by_key(sheet_id).values_clear("Duplicate Deals!A:G")

    ws.update([duplicate_df.columns.values.tolist()] + duplicate_df.values.tolist(), value_input_option="USER_ENTERED")

    print('Duplicate Check Uploaded')

if __name__ == '__main__':
    duplicate_check()
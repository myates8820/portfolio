def close_date_check():

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

    #checks to see if close date in HS is the same month as it is in our projects DB

    deals_sheet_id = os.environ['appointments_sheet_id']
    deals_ws = client.open_by_key(deals_sheet_id).worksheet('Closed Deals Export')
    deals_raw = deals_ws.get("A:AD")  
    deals_df = pd.DataFrame(data=deals_raw[1::],columns = deals_raw[0])
    deals_df = deals_df[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','amount_in_home_currency','closedate']]
    deals_df = deals_df[deals_df.project_sunrise_id.isnull()==False]
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].apply(lambda x: str(x).replace(',',''))
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].astype('int64')
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].apply(lambda x: str(x))
    deals_df['closedate'] = pd.to_datetime(deals_df['closedate'],errors='coerce')


    sold_sheet_id = os.environ['sold_projects_id']
    sold_ws = client.open_by_key(sold_sheet_id).worksheet('Intake Data')
    sold_raw = sold_ws.get("A3:AG")   

    sold_array = [[x[0],x[32],x[29]] for x in sold_raw]
    sold_date_df = pd.DataFrame(data=sold_array[1::],columns=['project_sunrise_id','sale_date','duplicate'])
    sold_date_df['sale_date'] = pd.to_datetime(sold_date_df['sale_date'],errors='coerce')
    sold_date_df = sold_date_df[(sold_date_df.duplicate.isnull()) | (sold_date_df.duplicate == '') ]
    sold_date_df.drop('duplicate',axis=1,inplace=True)

    sale_date_check = pd.merge(deals_df,sold_date_df,on='project_sunrise_id')
    # sale_date_check['closedate'] = sale_date_check['closedate'].apply(lambda x: x - datetime.timedelta(hours=5))
    sale_date_check['hs_datemonth'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%Y'))
    sale_date_check['db_datemonth'] = sale_date_check['sale_date'].apply(lambda x: x.strftime('%m/%Y'))
    sale_date_check = sale_date_check[(sale_date_check.hs_datemonth != sale_date_check.db_datemonth)]
    sale_date_check = sale_date_check[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','closedate','sale_date']]
    sale_date_check['closedate'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
    sale_date_check['sale_date'] = sale_date_check['sale_date'].apply(lambda x: x.strftime('%m/%d/%Y'))
    sale_date_check.fillna('',inplace=True)


    sheet_id = os.environ['appointments_sheet_id']
    ws = client.open_by_key(sheet_id).worksheet('Closed Date Fixes')
    client.open_by_key(sheet_id).values_clear("Closed Date Fixes!A:G")

    ws.update([sale_date_check.columns.values.tolist()] + sale_date_check.values.tolist(), value_input_option="USER_ENTERED")

    print('Close Date Check Uploaded')

if __name__=='__main__':
    close_date_check()
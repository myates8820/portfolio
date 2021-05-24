
def run_tests(hs_data):

    import pickle
    import os.path
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import pathlib
    import csv
    import numpy as np
    import pandas as pd
    import datetime
    from time import sleep
    import time
    import sys, os
    import math

    import requests
    import json
    from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check

    print('Data Checks Started')

    #Grabbing API key from environment
    


    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']


    creds_dict = json.loads(token)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)
    sales_team_id = os.environ['sales_teams_sheet']
    appointments_sheet_id = os.environ['appointments_sheet_id']

    sales_ws = client.open_by_key(sales_team_id).worksheet('Sales Teams')
    sales_teams_raw = sales_ws.get("A2:B")
    sales_map = {}
    for x in sales_teams_raw:
        sales_map[x[0]] = x[1]

    print('EC regions have been retrieved.')


    #section for once db has updater fixed by David
    import psycopg2

    url_base = os.environ['sunrise_alltasks_base']
    sunrise_api_key = os.environ['sunrise_api_key']
    sunrise_org_key = os.environ['sunrise_org_key']
    headers = {'api-key': sunrise_api_key}


    deposit_integration_check(hs_data)


#     def close_date_check():

#         #checks to see if close date in HS is the same month as it is in our projects DB
        
#         sold_projects_id = '1aw8WTA3FLXofPNAuA0P2FvmnKvW2Obw6hunMRdt8ff0'
#         sold_raw = google_pull(sold_projects_id, 'Intake Data!A2:AG')

#         sold_array = [[x[0],x[32]] for x in sold_raw]
#         sold_date_df = pd.DataFrame(data=sold_array[1::],columns=['swf_lead_id','sale_date'])
#         sold_date_df['sale_date'] = pd.to_datetime(sold_date_df['sale_date'],errors='coerce')

#         sale_date_check = pd.merge(deals_df,sold_date_df,on='swf_lead_id')
#         sale_date_check = sale_date_check[(sale_date_check.dealstage == 'Closed Won') & (sale_date_check.closed_by_proposal_tool == 'Yes')]
#         sale_date_check['closedate'] = sale_date_check['closedate'].apply(lambda x: x - datetime.timedelta(hours=5))
#         sale_date_check['hs_datemonth'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%Y'))
#         sale_date_check['db_datemonth'] = sale_date_check['sale_date'].apply(lambda x: x.strftime('%m/%Y'))
#         sale_date_check = sale_date_check[(sale_date_check.hs_datemonth != sale_date_check.db_datemonth)]
#         sale_date_check = sale_date_check[['hs_object_id','swf_lead_id','dealname','hubspot_owner_id','dealstage','closedate','sale_date']]
#         sale_date_check['closedate'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
#         sale_date_check['sale_date'] = sale_date_check['sale_date'].apply(lambda x: x.strftime('%m/%d/%Y'))

#         date_upload = [list(sale_date_check.columns)]
#         for x in sale_date_check.values.tolist():
#             date_upload.append(x)

#         update_range = 'Closed Date Fixes!A1:G'+str(len(date_upload))

#         google_update('1eeO7Wf-OaIt0caV7UMbyT4TDhETizTbPvB2fatz921I', update_range, date_upload, 'Closed Date Fixes!A:G')

#         print('Close Date Check Uploaded')

#     def duplicate_check():

#         # checks to see if there are any closed deals from HS that have the same lead ID

#         duplicate_df = deals_df[deals_df.closed_by_proposal_tool == 'Yes'].copy()
#         duplicate_df = duplicate_df[(duplicate_df.duplicated(keep=False, subset = 'swf_lead_id'))].sort_values(by='swf_lead_id',ascending=True)
#         duplicate_df = duplicate_df[['hs_object_id','swf_lead_id','dealname','hubspot_owner_id','dealstage','closedate']]
#         duplicate_df['closedate'] = pd.to_datetime(duplicate_df['closedate'],errors='coerce')
#         duplicate_df['closedate'] = duplicate_df['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))

#         duplicate_upload = [list(duplicate_df.columns)]
#         for x in duplicate_df.values.tolist():
#             duplicate_upload.append(x)

#         update_range = 'Duplicate Deals!A1:G'+str(len(duplicate_upload))

#         google_update('1eeO7Wf-OaIt0caV7UMbyT4TDhETizTbPvB2fatz921I', update_range, duplicate_upload, 'Duplicate Deals!A:G')

#         print('Duplicate Check Uploaded')

#     def sold_projects_check():

#         # checking to see if there are any projects in the projects DB that are not in HS, and vise versa

#         sold_projects_id = '1aw8WTA3FLXofPNAuA0P2FvmnKvW2Obw6hunMRdt8ff0'
#         sold_raw = google_pull(sold_projects_id, 'Intake Data!A2:AG')
#         recent_deals = deals_df[deals_df.closedate>=datetime.datetime(2020,9,1)]

#         sold_array = [[x[0],x[1],x[18],x[17],x[32]] for x in sold_raw]
#         project_df = pd.DataFrame(data=sold_array[1::],columns=['swf_lead_id','dealname','ec','sale_price','sale_date'])
#         project_df['sale_date'] = pd.to_datetime(project_df['sale_date'],errors='coerce')

#         project_df['sale_price'] = project_df['sale_price'].apply(lambda x: x.replace('$','').replace(',',''))
#         project_df['sale_price'] = project_df['sale_price'].astype('float64')


#         project_df = project_df[(project_df.sale_date >= datetime.datetime(2020,9,1)) & (project_df.sale_date < datetime.date.today())]
#         project_df = project_df[project_df.sale_price > 5]
#         projects_db_check = pd.merge(project_df,recent_deals,on='swf_lead_id',how='left')
#         projects_db_check = projects_db_check[projects_db_check.hs_object_id.isnull()][['swf_lead_id','dealname','ec','sale_price','sale_date']]

    



#     # running above functions. separated out so I don't have to deal with scopes and variable names

#     deposit_check()
#     close_date_check()
#     duplicate_check()
    
    

if __name__ == "__main__":
    import pickle
    import os.path
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import pathlib
    import csv
    import numpy as np
    import pandas as pd
    import datetime
    from time import sleep
    import time
    import sys, os
    import math

    import requests
    import json
    from subfunctions.Hubspot_API import HubspotApi
    from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check
    from subfunctions.deal_parser import map_deals_list
    from updater import all_properties

    hs_api_key = os.environ['hapikey']
    hubspot = HubspotApi(hs_api_key)
    hubspot.get_all_deals(all_properties)
    deal_list_raw = hubspot.parse_deals()
    deal_list = map_deals_list(deal_list_raw)

    print('Deals have been parsed. Starting Tests.')
    run_tests(deal_list)
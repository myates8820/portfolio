import json
import pandas as pd
import numpy as np
import psycopg2
from time import sleep
from oauth2client.service_account import ServiceAccountCredentials
import os
import datetime
import gspread
import psycopg2
token = os.environ['sheets_token']
scopes = ['https://spreadsheets.google.com/feeds']


creds_dict = json.loads(token)
creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
client = gspread.authorize(creds)

def sunrise_data_checks(deals_list):

    import json
    import pandas as pd
    import numpy as np
    import psycopg2
    from time import sleep
    from oauth2client.service_account import ServiceAccountCredentials
    import os
    import datetime
    import gspread
    import sys

    # grabbing deal_df and making sure it's cleaned

    start_cutoff = datetime.datetime(2020,1,1)

    deals_df = pd.DataFrame(data=deals_list[1::],columns=deals_list[0])
    deals_df = deals_df[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage',
        'amount_in_home_currency','closedate','closed_lost_date']]
    deals_df = deals_df[deals_df.project_sunrise_id.isnull()==False]
    deals_df = deals_df[deals_df.project_sunrise_id != '']
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].apply(lambda x: str(x).replace(',','').replace('.0',''))
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].astype('int64')
    deals_df['project_sunrise_id'] = deals_df['project_sunrise_id'].apply(lambda x: str(x))
    deals_df['closedate'] = pd.to_datetime(deals_df['closedate'],errors='coerce')
    deals_df['closed_lost_date'] = pd.to_datetime(deals_df['closed_lost_date'],errors='coerce')
    
    # grabbing projects list from sunrise

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
            colnames = [desc[0] for desc in cur.description]
            print(final_query)
        else:
            query_build = ', '.join(cols)
            final_query = query.replace('[cols]', query_build).replace('[table]', table)
            colnames = cols
        cur.execute(final_query)
        data = cur.fetchall()
        data.insert(0,tuple(colnames))
        cur.close()
        conn.close()
        return data

    deal_query = """SELECT [cols] FROM [table]"""
    deal_list = query_selector(deal_query,'*','sunrise_projects')
    sunrise_df = pd.DataFrame(data=deal_list[1::],columns=deal_list[0]).fillna('')
    sunrise_df.rename(columns={'closedate':'created_at'},inplace=True)

    
    sunrise_df['created_at'] = pd.to_datetime(sunrise_df['created_at'],unit='ms',errors='coerce')
    sunrise_df = sunrise_df[sunrise_df.project_sunrise_id.isnull()==False]
    sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].apply(lambda x: str(x).replace(',','').replace('.0',''))
    sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].astype('int64')
    sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].apply(lambda x: str(x))

    # merge dfs and now can perform following tests: missing projects, incorrect close dates, and cancels
    df = pd.merge(deals_df,sunrise_df,on='project_sunrise_id',how='left')
    
    # missing projects. Identifies projects listed as closed won/lost in Hubspot which are not found in Sunrise. May have to adapt for old cancels
    def missing_projects_test():
        missing_df = df[df.hold_status.isnull()][['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','amount_in_home_currency','closedate']].copy()
        sheet_id = os.environ['appointments_sheet_id']
        missing_df['closedate'] = missing_df['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
        ws = client.open_by_key(sheet_id).worksheet('Missing Sunrise Projects')
        client.open_by_key(sheet_id).values_clear("Missing Sunrise Projects!A:G")
        ws.update([missing_df.columns.values.tolist()] + missing_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Missing Projects Check Completed')
        del missing_df

    def sale_date_checks():
        # close date check. Makes sure hubspot close date matches the creation date in Project Sunrise
        sale_date_check = df[df.created_at >= start_cutoff].copy()
        sale_date_check['hs_datemonth'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%Y'))
        sale_date_check['db_datemonth'] = sale_date_check['created_at'].apply(lambda x: x.strftime('%m/%Y'))
        sale_date_check = sale_date_check[(sale_date_check.hs_datemonth != sale_date_check.db_datemonth)]
        sale_date_check = sale_date_check[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','closedate','created_at']]
        sale_date_check['closedate'] = sale_date_check['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
        sale_date_check['created_at'] = sale_date_check['created_at'].apply(lambda x: x.strftime('%m/%d/%Y'))
        sale_date_check.fillna('',inplace=True)

        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Closed Date Fixer')
        client.open_by_key(sheet_id).values_clear("Closed Date Fixer!A:G")

        ws.update([sale_date_check.columns.values.tolist()] + sale_date_check.values.tolist(), value_input_option="USER_ENTERED")

        print('Close Date Check Uploaded')
        del sale_date_check

    # cancel check. Verifies that any job with hold status cancelled is actually closed lost in Hubspot, and vise versa
    def cancel_check():
        cancel_df = df[df.created_at >= start_cutoff].copy()
        cancel_df = cancel_df[((cancel_df.hold_type == 'Cancelled') & (cancel_df.dealstage != 'Closed Lost')) | ((cancel_df.hold_type != 'Cancelled') & (cancel_df.dealstage == 'Closed Lost'))]
        cancel_df = cancel_df[cancel_df.hold_status == True]
        cancel_df = cancel_df[['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','dealstage','hold_type','closedate','closed_lost_date']]
        cancel_df.sort_values(by=['dealstage','closed_lost_date'],inplace=True)
        cancel_df.fillna('',inplace=True)
        cancel_df['closedate'] = cancel_df['closedate'].apply(lambda x: x.strftime('%m/%d/%Y'))
        def closed_lost_fixer(x):
            if (x is None) or (x == ''):
                return ''
            else:
                return x.strftime('%m/%d/%Y')

        cancel_df['closed_lost_date'] = cancel_df['closed_lost_date'].apply(closed_lost_fixer)

        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Cancel Discrepancies')
        client.open_by_key(sheet_id).values_clear("Cancel Discrepancies!A:H")

        ws.update([cancel_df.columns.values.tolist()] + cancel_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Cancel Check Uploaded')
        del cancel_df

    missing_projects_test()
    sale_date_checks()
    cancel_check()

    # try:
    #     missing_projects_test()
    # except:
    #     print('Missing Projects Test Failed')

    # try:
    #     sale_date_checks()
    # except:
    #     print('Sale Date Test Failed')

    # try:
    #     cancel_check()
    # except:
    #     print('Cancellation Test Failed')







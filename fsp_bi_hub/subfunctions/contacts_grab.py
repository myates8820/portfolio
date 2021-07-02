def run_grab():

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
    import time
    import sys, os
    import math

    import requests
    import json

    #Grabbing API key from environment

    api_key = os.environ['hapikey']
    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']


    creds_dict = json.loads(token)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)

    contact_list = []

    import psycopg2


    db_database = os.environ['database']
    db_user = os.environ['user']
    db_password = os.environ['password']
    db_host = os.environ['host']


    def query_selector(cols,table):
        conn = psycopg2.connect(database=db_database, user=db_user, 
                            password = db_password,
                            host = db_host)
        cur = conn.cursor()
        cur.execute("rollback;")
        query = """ SELECT [cols] FROM [table]"""
        query_build = ', '.join(cols)
        query = query.replace('[cols]', query_build).replace('[table]', table)
        cur.execute(query)
        data = cur.fetchall()
        data.insert(0,tuple(cols))
        cur.close()
        conn.close()
        return data

    headers = {}

    # Grabs the owner ids to match with employee names
    # DAVID: Replace this with hs.get_owners()
    owner_url = "https://api.hubapi.com/owners/v2/owners?hapikey="+api_key

    r = requests.get(url = owner_url)

    owner_response = json.loads(r.text)
    owner_dict = [('','')]
    for x in owner_response:
        owner_dict.append((str(x['ownerId']),x['firstName']+' '+x['lastName']))

        
    owner_dict = dict(owner_dict)

    print('Owners have been retrieved.')

    base_properties = ['hs_object_id','firstname','lastname','createdate',
                    'five9_create_date','num_associated_deals','address','lead_source','swf_lead_id','market_region',
                    'qualified_contact','five9_disposition','hubspot_owner_id','top_deal_sources_contacts_','lead_type',
                    'zip','five9_notes','zombie','email','phone','five9_dnq_reason','five9_dnq_agent','cac_lead_source','n3pl_lead_source']


    #testing pulling from SQL database
    contacts_parsed_raw = query_selector(base_properties,'contacts')
    contacts_parsed_raw = [list(x) for x in contacts_parsed_raw[1::]]

        
    #eliminating None objects and transforming them to ''

    contacts_parsed = []
    for x in contacts_parsed_raw:
        line_item = []
        for i in x:
            if i is None:
                line_item.append('')
            else:
                line_item.append(i)
        contacts_parsed.append(line_item)

    def test_eliminator(x):
        if x == None:
            return 1
        elif x.lower().find('test') >= 0:
            return 1
        else:
            return 0
              


    def date_to_string(x):
        if x!=x:
            return x
        elif x == 0:
            return ''
        elif x =='0':
            return ''
        elif x == '':
            return x
        else:
            string_date = x.strftime('%#m/%#d/%Y')
            return string_date



    # section to get mapping for CAC lead sources and covering for issues with Hubspot


    cols = ['name','cac_name','cac_category']
    data = query_selector(cols,'lead_sources')


    cac_lead_source_map = {}
    for x in data:
        cac_lead_source_map[x[0]] = x[1]

    cac_category_map = {}
    for x in data:
        cac_category_map[x[0]] = x[2]



    #setting times
    cd_year_cutoff = datetime.datetime(2020,1,1)
    now = datetime.datetime.now()
    today = datetime.datetime.strftime(now,'%Y-%m-%d')

    df = pd.DataFrame(data = contacts_parsed, columns=base_properties)
    df.drop(df.index[df['five9_create_date']==''],inplace=True)
    df['createdate'] = pd.to_datetime(df['createdate'],errors='coerce')
    df['five9_create_date'] = pd.to_datetime(df['five9_create_date'],errors='coerce')
    df.drop(df.index[df['lead_type']=='service_internal'],inplace=True)
    df.drop(df.index[df['five9_create_date']<cd_year_cutoff],inplace=True)
    df.drop(df.index[df['five9_create_date']>=today],inplace=True)
    df['hs_object_id'] = df['hs_object_id'].astype('object')

    df['Test FirstName'] = df['firstname'].apply(test_eliminator)
    df['Test LastName'] = df['lastname'].apply(test_eliminator)

    df.drop(df.index[df['Test FirstName']==1],inplace=True)
    df.drop(df.index[df['Test LastName']==1],inplace=True)
    df.drop(['Test FirstName', 'Test LastName'],axis=1,inplace=True)


    df.drop(df.index[df['firstname']==''],inplace=True)
    # df.drop(df.index[df['zombie']=='yes'],inplace=True)
    # df.drop(df.index[df['five9_disposition']=='Wrong Number'],inplace=True)
    # df.drop(df.index[df['five9_disposition']=='Agent Error'],inplace=True)
    # df.drop(df.index[df['five9_disposition']=='Dial Error'],inplace=True)
    # df.drop(df.index[df['five9_disposition']=='Spam Suspected'],inplace=True)
    # df.drop(df.index[df['five9_disposition']=='Wrong Number - No Inquiry'],inplace=True)
    df['hubspot_owner_id'] = df['hubspot_owner_id'].map(owner_dict)

    #mapping cac_lead_source and category then printing out if there are any errors
    df['lead_source'] = np.where(((df['lead_source'].isnull()) | (df['lead_source'] == '') | (df['lead_source'] == ' ')),'Other',df['lead_source'])
    df['cac_lead_source'] = df['lead_source'].map(cac_lead_source_map)
    df['n3pl_lead_source'] = df['lead_source'].map(cac_category_map)
    

    # Datetime string converter -- Change datetime object to string format


    # modified section to cut down for upload but not to affect old exports
    df = df[['hs_object_id','firstname','lastname','createdate','five9_create_date','num_associated_deals','address','lead_source','swf_lead_id','market_region','qualified_contact',
        'five9_disposition','hubspot_owner_id','top_deal_sources_contacts_','zip','five9_notes','email','phone','cac_lead_source','n3pl_lead_source']]

    # sunpower_df = df[df]

    df.sort_values(by='five9_create_date',ascending=True,inplace=True)
    df['createdate'] = df['createdate'].apply(date_to_string)
    df['five9_create_date'] = df['five9_create_date'].apply(date_to_string)

    df.fillna('',inplace=True)

    #summarizing for easier upload

    df = df[['five9_create_date','market_region','lead_source','cac_lead_source','n3pl_lead_source']]
    df['count'] = 1
    df = df.groupby(by=['five9_create_date','market_region','lead_source','cac_lead_source','n3pl_lead_source'],as_index=False).sum()
    summary = df.values.tolist()
    summary.insert(0,list(df.columns))

    requested_ID = os.environ['contacts_sheet_id']
    ws = client.open_by_key(requested_ID).worksheet('Filtered')
    client.open_by_key(requested_ID).values_clear('Filtered!A:F')
    ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option="USER_ENTERED")

    print('Contacts have been uploaded.')

if __name__ == "__main__":
    run_grab()
# getting Owner, Sales region, Deal Stage Keys to Map Out Items So It Won't need to be down in subsequent functions
def map_deals_list(parsed):

    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import numpy as np
    import pandas as pd
    import datetime
    import time
    import sys, os, os.path
    import math
    import requests
    import json



    # sales regions
    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']
    hs_api_key = os.environ['hapikey']


    creds_dict = json.loads(token)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)
    sales_team_id = os.environ['sales_teams_sheet']

    sales_ws = client.open_by_key(sales_team_id).worksheet('Sales Teams')
    sales_teams_raw = sales_ws.get("A2:B")
    sales_map = {}
    for x in sales_teams_raw:
        sales_map[x[0]] = x[1]

    deal_list = []
    for x in parsed:
        line_item = []
        for i in x:
            if i is None:
                line_item.append('')
            else:
                line_item.append(i)
        deal_list.append(line_item)

    deal_stage_map = {
        'appointmentscheduled': 'Appointment Scheduled',
        '1676636': 'Working - 1',
        '1913549': 'Working - 2',
        '1913557': 'Working - 3',
        '1913578': 'Working - 4',
        '1913579': 'Working - 5',
        'closedwon':'Closed Won',
        'closedlost':'Closed Lost',
        '5839747': 'Appointment Scheduled',
        '5839748': 'Working - 1',
        '5839749': 'Working - 2',
        '5839750': 'Working - 3',
        '5839751': 'Working - 4',
        '5839788': 'Working - 5',
        '5839752':'Closed Won',
        '5839753':'Closed Lost',
    }

    
    owner_url = "https://api.hubapi.com/owners/v2/owners?hapikey="+hs_api_key

    r = requests.get(url = owner_url)

    owner_response = json.loads(r.text)
    owner_dict = [('','')]
    for x in owner_response:
        owner_dict.append((str(x['ownerId']),x['firstName']+' '+x['lastName'])) 
    owner_dict = dict(owner_dict)


    def datetime_converter(x):
        if x!=x:
            return ''
        elif x == 0:
            return ''
        elif x =='0':
            return ''
        elif x == '':
            return x
        elif x == None:
            return ''
        elif len(x) == 10:
            date_parsed = datetime.datetime.strptime(x,'%Y-%m-%d')
            return date_parsed
        else:
            try:
                date_parsed = datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%S.%fZ')
                return date_parsed
            except:
                date_parsed = datetime.datetime.strptime(x,'%Y-%m-%dT%H:%M:%SZ')
                return date_parsed    

    def date_to_string(x):
        if x!=x:
            return ''
        elif x == 0:
            return ''
        elif x =='0':
            return ''
        elif x == '':
            return x
        else:
            string_date = x.strftime('%m/%d/%Y %H:%M:%S')
            return string_date

    df = pd.DataFrame(data=deal_list[1::],columns=deal_list[0])
    
    df['hubspot_owner_id'] = df['hubspot_owner_id'].map(owner_dict)
    df['dealstage'] = df['dealstage'].map(deal_stage_map)
    df['appointment_ran_by'] = df['appointment_ran_by'].map(owner_dict)
    df['original_deal_owner'] = np.where(df['original_deal_owner'].isnull(),df['hubspot_owner_id'],df['original_deal_owner'])
    
    df['closedate'] = pd.to_datetime(df['closedate'],errors='coerce')-pd.Timedelta('05:00:00')
    df['closedate'] = df['closedate'].apply(date_to_string)
    df['createdate'] = pd.to_datetime(df['createdate'],errors='coerce')
    df['createdate'] = df['createdate'].apply(date_to_string)
    df['appointment_date'] = pd.to_datetime(df['appointment_date'],errors='coerce')
    df['appointment_date'] = df['appointment_date'].apply(date_to_string)
    df['closed_lost_date'] = pd.to_datetime(df['closed_lost_date'],errors='coerce')
    df['closed_lost_date'] = df['closed_lost_date'].apply(date_to_string)

    

    return_list = df.values.tolist()
    return_list.insert(0,df.columns.tolist())

    return return_list


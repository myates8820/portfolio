def deposit_integration_check():
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
    import psycopg2

    import requests
    import json

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
    deal_columns = ['hs_object_id','project_sunrise_id','dealname','hubspot_owner_id','send_deposit_to_accounting','dealstage',
                'amount_in_home_currency','closedate','closed_by_proposal_tool','matt_deposit_check','deposit_sent_integration_failure']
    deal_list = query_selector(deal_query,deal_columns,'full_cleaned')
    df = pd.DataFrame(data=deal_list[1::],columns=deal_list[0]).fillna('')
    cutoff = datetime.datetime(2021,1,1)

    df['closedate'] = pd.to_datetime(df['closedate'],errors='coerce')
    df = df[df['closedate']>=cutoff]

    df = df[(df.project_sunrise_id.isnull()==False) & (df.project_sunrise_id != '') & (df.project_sunrise_id != ' ')]
    df['project_sunrise_id'] = df['project_sunrise_id'].apply(lambda x: str(x).replace(',','').replace('.0',''))
    df['project_sunrise_id'] = df['project_sunrise_id'].astype('int64')
    df['project_sunrise_id'] = df['project_sunrise_id'].apply(lambda x: str(x))

    sold_df = df[(df.closed_by_proposal_tool == 'Yes') & (df.dealstage == 'Closed Won')
                        & (df.deposit_sent_integration_failure != 'Success')].copy()
    sold_df = sold_df[deal_columns]

    hs_api_key = os.environ['hapikey']
    url_base = os.environ['sunrise_alltasks_base']
    api_key = os.environ['sunrise_api_key']
    org_key = os.environ['sunrise_org_key']
    headers = {'api-key': api_key}

    print('Sunrise grab started.')

    sunrise_data = [deal_columns]
    sunrise_data[0].append('tasks')

    sunrise_list = sold_df.values.tolist()
    for x in sunrise_list:
        try:
            lead_id = x[1]
            url = url_base.replace('{orgId}',org_key).replace('{project_id}',lead_id)
            r = requests.get(url = url, headers = headers)
            owner_response = json.loads(r.text)
            sleep(0.1)
            task_array = owner_response['body']['tasks']
            if task_array:
                x.append({'tasks': task_array})
            else:
                x.append({'tasks':['No Project Found']})
            sunrise_data.append(x)
        except:
            sunrise_data.append({'tasks':['Error']})
            print(x)


    task_index = int(sunrise_data[0].index('tasks'))
    deposit_sent_index = int(sunrise_data[0].index('send_deposit_to_accounting'))

    deposit_check_list = []
    print(len(sunrise_data))
    if len(sunrise_data) <= 1:
        print('All deposit sent integrations have been successfully transferred.')
    else:      
        for x in sunrise_data[1::]:

            row = [x[0],x[1],x[2],x[deposit_sent_index]]
            task_array = x[task_index]['tasks']
            if len(task_array)>1:
                for i in task_array:
                    if i['name'].lower() == 'confirm deposit sent to accounting':
                        row.append(i['is_complete'])
            else:
                row.append(task_array[0])

            deposit_check_list.append(row)

    error_list = []
    okay_projects = []
    missing_projects = []
    missing_deposit_sent = []
    for x in deposit_check_list:
        if x[4] == False:
            missing_deposit_sent.append(x)
        elif x[4] == 'No Project Found':
            missing_projects.append(x)
        elif x[4] == 'Error':
            error_list.append(x)       
        else:
            okay_projects.append(x)

    for x in deposit_check_list:
        if len(x)>5:
            x.pop()

    missing_sent_df = pd.DataFrame(data=missing_deposit_sent, columns = ['hs_object_id','project_sunrise_id','dealname','send_deposit_hs','send_deposit_sunrise'])
    missing_sent_df = missing_sent_df[missing_sent_df.send_deposit_hs == 'true']

    okay_df = pd.DataFrame(data = deposit_check_list, columns = ['hs_object_id','project_sunrise_id','dealname','send_deposit_hs','send_deposit_sunrise'])
    okay_df = okay_df[(okay_df.send_deposit_sunrise == True) & (okay_df.send_deposit_hs == 'true')]

    base_url = "https://api.hubapi.com/crm/v3/objects/deals/dealId"

    missing_length = len(missing_sent_df)
    okay_length = len(okay_df)

    print(f"There are {missing_length} deposit failures.")
    print(f"There are {okay_length} unlogged successes.")

    if missing_length>0:

        print('HS Failure Upload started')

        missing_results = missing_sent_df.values.tolist()

        if missing_results:
            error_count = 0
            for x in missing_results:
                if error_count>5:
                    print('Failed')
                    break

                else:
                    url = base_url.replace("dealId",str(x[0]).replace(',','').replace('.0',''))

                    props = {"properties": 
                        {
                        "deposit_sent_integration_failure": "Failure",
                        }
                    }

                    payload = json.dumps(props)

                    querystring = {"hapikey": hs_api_key}

                    headers = {
                    'accept': "application/json",
                    'content-type': "application/json"
                    }

                    try:
                        response = requests.request("PATCH", url, data=payload, headers=headers, params=querystring)
                        time.sleep(0.2)

                        if response.status_code != 200:
                            print(response.text)
                            time.sleep(5)
                            error_count+=1
                    except:
                        print(response)
                        error_count+=1

            print('Deposit Failures Uploaded')

    if okay_length>0:

        print('HS Okay Upload started')

        okay_results = okay_df.values.tolist()

        if okay_results:
            error_count = 0
            for x in okay_results:
                if error_count>5:
                    print('Failed')
                    break

                else:
                    url = base_url.replace("dealId",str(x[0]).replace(',','').replace('.0',''))

                    props = {"properties": 
                        {
                        "deposit_sent_integration_failure": "Success",
                        }
                    }

                    payload = json.dumps(props)

                    querystring = {"hapikey": hs_api_key}

                    headers = {
                    'accept': "application/json",
                    'content-type': "application/json"
                    }

                    try:
                        response = requests.request("PATCH", url, data=payload, headers=headers, params=querystring)
                        time.sleep(0.2)

                        if response.status_code != 200:
                            print(response.text)
                            time.sleep(5)
                            error_count+=1
                    except:
                        print(response)
                        error_count+=1

            print('Matt Deposit Check Updated')
    
    print('Deposit Integration Failure Check Complete.')
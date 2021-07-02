def run_tests():

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
    from all_properties import all_properties

    import requests
    import json
    from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check
    from subfunctions.data_tests.duplicate_deal_check import duplicate_check
    from subfunctions.data_tests.deal_date_check import close_date_check
    from subfunctions.data_tests.sunrise_data_checks import sunrise_data_checks
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
    deal_list = query_selector(deal_query,all_properties,'full_cleaned')
    sold_list = query_selector(deal_query,'*','deals_cleaned')


    #section for once db has updater fixed by David
    # import psycopg2

    url_base = os.environ['sunrise_alltasks_base']
    sunrise_api_key = os.environ['sunrise_api_key']
    sunrise_org_key = os.environ['sunrise_org_key']
    headers = {'api-key': sunrise_api_key}

    # deposit_integration_check(deal_list)
    duplicate_check()
    sunrise_data_checks(sold_list)

    # try:
    #     deposit_integration_check(deal_list)
    # except:
    #     print('Deposit Check Failed')


    # try:
    #     duplicate_check()
    # except Exception as e:
    #     print('Duplicate Check Failed')
    #     print(e)
    # # close_date_check()
    # try:
    #     sunrise_data_checks(sold_list)
    # except Exception as e:
    #     print('Sunrise Data Checks Failed')
    #     print(e)

    
    

if __name__ == "__main__":
    pass
    # import os
    # import psycopg2
    # from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check
    # from subfunctions.deal_parser import map_deals_list

    # print('Starting Tests.')
    # run_tests()
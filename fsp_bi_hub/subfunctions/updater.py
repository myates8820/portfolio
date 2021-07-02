import subfunctions.contacts_grab as contacts_grab
import subfunctions.deals_grab as deals_grab
from subfunctions import data_test_master
from subfunctions.Hubspot_API import HubspotApi
import numpy as np
import pandas as pd
import os
from time import sleep
import json, requests
import gspread
from oauth2client.service_account import ServiceAccountCredentials
# from subfunctions.deal_parser import map_deals_list
# from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check
# from subfunctions.tasks_db_updater import update_task_database
from subfunctions.data_test_master import run_tests
# from subfunctions.sold_deals_grab import sold_deals_grab
from subfunctions.deals_grab import run_grab
from all_properties import all_properties 


hs_api_key = os.environ['hapikey']

contact_tries = 0
while contact_tries < 3:
    try:
        contacts_grab.run_grab()
        break
    except:
        contact_tries+=1
        sleep(60)

print('Deal Updater Started')

        

# hubspot = HubspotApi(hs_api_key)
# results = hubspot.get_all_deals(all_properties)
# deal_list_raw = hubspot.parse_deals()
# deal_list = map_deals_list(deal_list_raw)

# print('Deals have been parsed.')

run_grab()
run_tests()
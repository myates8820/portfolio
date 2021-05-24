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
from subfunctions.deal_parser import map_deals_list

def run_grab(all_deals):


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
    from subfunctions.deal_parser import map_deals_list

    print('Function started')

    #Grabbing API key from environment
    api_key = os.environ['hapikey']

    # owner_url = "https://api.hubapi.com/owners/v2/owners?hapikey="+api_key

    # r = requests.get(url = owner_url)

    # owner_response = json.loads(r.text)
    # owner_dict = [('','')]
    # for x in owner_response:
    #     owner_dict.append((str(x['ownerId']),x['firstName']+' '+x['lastName']))

        
    # owner_dict = dict(owner_dict)

    print('Owners have been retrieved.')

    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']


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

    print('EC regions have been retrieved.')


    #section for once db has updater fixed by David
    import psycopg2

    # db_database = os.environ['database']
    # db_user = os.environ['user']
    # db_password = os.environ['password']
    # db_host = os.environ['host']

    # # will load in environmental variables once David updates db
    # conn = psycopg2.connect(database=db_database, user=db_user, 
    #                         password = db_password,
    #                         host = db_host)

    # cur = conn.cursor()
    # cur.execute("rollback;")
    # query = """ SELECT owner_id FROM owners """
    # cur.execute(query)
    # owner_list_full = cur.fetchall()
    # current_owner_list = [x[0] for x in owner_list_full]
    # quick_list_owners = owner_dict


    # def test_eliminator(x):
    #     if x.lower().find('test') >= 0:
    #         return 1
    #     else:
    #         return 0


    # all_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','agent_name',
    #                         'appointment_date','top_deal_sources','appointment_type','deal_lead_source',
    #                         'deal_marketing_lead_category','hubspot_owner_id','dealtype',
    #                         'market_region','run_status','appointment_ran_by','appointment_disposition','swf_lead_id',
    #                         'closed_lost_notes','call_center_return_notes','closed_lost_reason','createdate',
    #                         'has_been_rescheduled','closed_by_proposal_tool','module_type',
    #                         'original_deal_owner','commission','system_size','tesla_powerwall_quantity','tesla_powerwall_revenue',
    #                         'module_quantity','closed_lost_date','closedate','cc_notes_important',
    #                         'call_center_reschedule_disposition','closed_won_reason','appointment_date_passed',
    #                         'promotion_rebate','promotion_discount','sunpower_rebate_adder','financing_fees','mumd_promotion',
    #                         'true_promotion','promotion_adder','cac_lead_source','cac_category_lead_source','full_promotion_true_paid_fsp',
    #                         'full_promotion_total','full_promotion_mumd_fsp','sunpower_rebate_total','promotion_true_paid_by_fsp',
    #                         'promotion_mumd_fsp','promotion_total','utility','canvasser_name','net_revenue','powerwall_split_portion','cash_loan',
    #                         'send_deposit_to_accounting','matt_deposit_check','project_sunrise_id']    

    # hubspot = HubspotApi(api_key)
    # results = hubspot.get_all_deals(all_properties)
    # deal_list_raw = hubspot.parse_deals()

    #eliminating None objects and transforming them to ''

    # deal_list = []
    # for x in deal_list_raw:
    #     line_item = []
    #     for i in x:
    #         if i is None:
    #             line_item.append('')
    #         else:
    #             line_item.append(i)
    #     deal_list.append(line_item)

    deal_list = all_deals       

    print('')    
    print('Deals have been retrieved.')
    # print(deal_list[0:2])

    # Area for selecting which columns to grab for each data upload

    appointment_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','agent_name',
                            'appointment_date','top_deal_sources','appointment_type','deal_lead_source',
                            'canvasser_name','hubspot_owner_id','dealtype',
                            'market_region','run_status','appointment_disposition','swf_lead_id',
                            'cac_lead_source','cac_category_lead_source','closed_lost_reason','createdate',
                            'has_been_rescheduled','closed_by_proposal_tool']

    sold_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','closedate',
                    'appointment_date','appointment_type','market_region','hubspot_owner_id','module_type',
                    'original_deal_owner','commission','run_status','swf_lead_id','system_size','cash_loan',
                    'deal_lead_source','tesla_powerwall_quantity','tesla_powerwall_revenue','module_quantity',
                    'closed_lost_date','closed_by_proposal_tool','dealtype','financing_fees','full_promotion_true_paid_fsp','full_promotion_mumd_fsp',
                    'promotion_total','sunpower_rebate_total','promotion_discount','cac_lead_source','cac_category_lead_source','net_revenue','powerwall_split_portion']


    cancel_properties = ['hs_object_id','dealname','swf_lead_id','amount_in_home_currency','closedate','dealstage',
                        'hubspot_owner_id','top_deal_sources','closed_lost_date','closed_lost_reason','closed_lost_notes',
                        'market_region','system_size','deal_lead_source','tesla_powerwall_quantity','tesla_powerwall_revenue',
                        'closed_by_proposal_tool','dealtype','cac_lead_source','cac_category_lead_source','financing_fees','net_revenue','original_deal_owner',
                        'agent_name','canvasser_name']

    cross_funnel_properties = ['associated_contact','hs_object_id','dealname','dealname','dealname','amount_in_home_currency','dealstage',
                        'appointment_date','appointment_disposition','cc_notes_important','call_center_return_notes',
                            'run_status','call_center_reschedule_disposition','closed_lost_reason','closed_lost_notes',
                            'hubspot_owner_id','closed_by_proposal_tool','createdate','agent_name','dealtype']

    # Add additional properties to additional_play_properties as ad hoc analyses call for, this way I don't need to mutilate the uploads to deal with non-essential requests

    play_appointment_properties = list(appointment_properties)
    additional_play_appointment_properties = ['utility']
    for x in additional_play_appointment_properties:
        play_appointment_properties.append(x)

    play_sold_properties = list(sold_properties)
    additional_play_sold_properties = ['utility']
    for x in additional_play_sold_properties:
        play_sold_properties.append(x)


    now = datetime.datetime.now()
    today = datetime.datetime.strftime(now,'%Y-%m-%d')
    cd_year_cutoff = datetime.datetime(2020,1,1)

    # handling missing and non-conforming data to be able to transform dataframes

    def null_handler(x):
        if x == '':
            return 0
        else:
            return x
        
    def deal_stage_corrector(x):
        if x == "closedwon":
            return "Closed Won"
        elif x == "closedlost":
            return "Closed Lost"
        else:
            return x
        
    def keep_deal(x):
        if x == "closedwon":
            return "Yes"
        elif x == "closedlost":
            return "Yes"
        else:
            return x
    
    def run_number(x):
        if x == 'Run':
            return 1
        else:
            return 0
        

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
            string_date = x.strftime('%m/%d/%Y')
            return string_date


    #update in future if any change or connect to db with updater    
    # deal_stage_map = {
    #     'appointmentscheduled': 'Appointment Scheduled',
    #     '1676636': 'Working - 1',
    #     '1913549': 'Working - 2',
    #     '1913557': 'Working - 3',
    #     '1913578': 'Working - 4',
    #     '1913579': 'Working - 5',
    #     'closedwon':'Closed Won',
    #     'closedlost':'Closed Lost',
    #     '5839747': 'Appointment Scheduled',
    #     '5839748': 'Working - 1',
    #     '5839749': 'Working - 2',
    #     '5839750': 'Working - 3',
    #     '5839751': 'Working - 4',
    #     '5839788': 'Working - 5',
    #     '5839752':'Closed Won',
    #     '5839753':'Closed Lost',
    # }

    # Separated each grab into its own function and running here for legibility and quicker editing

    # creating the separate dataframes for loading data. At a later point, need to modify for creating Tableau/SQL databases
    # Framework is as follows (not always in order). Subject to change when deploying online:
    #   Load full dataframe --> create slices for each export using the respective property lists
    #   Convert Hubspot dates to date objects in order to perform operations
    #   Add and drop rows based on criteria, drop columns if not needed
    #   Change datetime objects to strings
    #   Create csv export for local copy and for debugging
    #   Upload to google

    # creating function decorator to attempt each upload 3 times in case of timeouts, etc.
    def attempt_grabs(func):
        def wrapper_attempter():
            x = 0
            while x < 3:
                try:
                    func()
                    break
                except:
                    x+=1
                    print(f'Attempt {x} failed.')
                    print('Upload Failed. Sleeping for 60 seconds and starting again. Will attempt 3 times.')           
                    sleep(60)
        return wrapper_attempter

    @attempt_grabs
    def appointments_grab():

        appointments_df = pd.DataFrame(data = deal_list[1::], columns=deal_list[0])
        appointments_df = appointments_df[appointment_properties]
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']==''],inplace=True)

        # converting create date and appointment date to datetime format
        appointments_df['createdate'] = pd.to_datetime(appointments_df['createdate'],errors='coerce')
        appointments_df['appointment_date'] = pd.to_datetime(appointments_df['appointment_date'],errors='coerce')

        # Removing all appointments from today to future, and all before 2020 to keep upload small
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']>=today],inplace=True)
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']<cd_year_cutoff],inplace=True)

        # mapping owners and deal stages -- Needs to be updated if new deal stages are added
        # appointments_df['hubspot_owner_id'] = appointments_df['hubspot_owner_id'].map(owner_dict)
        # appointments_df['dealstage'] = appointments_df['dealstage'].map(deal_stage_map)
        # appointments_df['appointment_ran_by'] = appointments_df['appointment_ran_by'].map(owner_dict)

        # Adding run number and reorganizing to match Sheets
        appointments_df['run_number'] = appointments_df['run_status'].apply(run_number)
        appointment_properties.insert(14,'run_number')
        appointments_df = appointments_df[appointment_properties]

        # removing columns and then sorting
        appointments_df.drop(['has_been_rescheduled'],axis=1,inplace=True)
        appointments_df.drop(['closed_by_proposal_tool'],axis=1,inplace=True)

        appointments_df.sort_values(by='appointment_date',ascending=False,inplace=True)


        # Converting datetime objects to strings
        appointments_df['createdate'] = appointments_df['createdate'].apply(date_to_string)
        appointments_df['appointment_date'] = appointments_df['appointment_date'].apply(date_to_string)


        appointments_df.fillna('',inplace=True)
       
        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Appointments Export')
        client.open_by_key(sheet_id).values_clear("Appointments Export!A:U")

        ws.update([appointments_df.columns.values.tolist()] + appointments_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Appointments have been uploaded.')

    @attempt_grabs
    def sold_grab():
        # creating the separate dataframes for loading data. At a later point, need to modify for creating Tableau/SQL databases
        # Follows same structure as above with datetime objects and drop criteria

        sold_df = pd.DataFrame(data = deal_list[1::], columns=deal_list[0])
        sold_df = sold_df[sold_properties]
        sold_df.drop(sold_df.index[sold_df['closed_by_proposal_tool']!='Yes'],inplace=True)
        sold_df.drop(sold_df.index[sold_df['dealtype']!='newbusiness'],inplace=True)
        sold_df.drop(sold_df.index[sold_df['closedate']==''],inplace=True)
        sold_df['closedate'] = pd.to_datetime(sold_df['closedate'],errors='coerce')
        sold_df['appointment_date'] = pd.to_datetime(sold_df['appointment_date'],errors='coerce')
        sold_df['closed_lost_date'] = pd.to_datetime(sold_df['closed_lost_date'],errors='coerce')
        sold_df.drop(['dealtype'],axis=1,inplace=True)
        sold_df = sold_df[(sold_df.dealstage == 'Closed Won') | (sold_df.dealstage == 'Closed Lost')]

        #Testing due to dropoff
        # sold_df.drop(sold_df.index[sold_df['closedate']>=today],inplace=True)
        sold_df.drop(sold_df.index[sold_df['closedate']<cd_year_cutoff],inplace=True)
        sold_df.sort_values(by='closedate',ascending=False,inplace=True)
        sold_df.fillna('',inplace=True)


        sold_df['amount_in_home_currency'] = sold_df['amount_in_home_currency'].apply(null_handler)
        sold_df['system_size'] = sold_df['system_size'].apply(null_handler)
        sold_df['swf_lead_id'] = sold_df['swf_lead_id'].apply(null_handler)

        # sold_df['closed_lost_date'] = sold_df['closed_lost_date'].apply(null_handler)
        # sold_df['appointment_date'] = sold_df['appointment_date'].apply(null_handler)

        # sold_df['closed_lost_date'] = sold_df['closed_lost_date'].apply(datetime_converter)
        # sold_df['appointment_date'] = sold_df['appointment_date'].apply(datetime_converter)

        sold_df['closedate'] = sold_df['closedate'].apply(date_to_string)
        sold_df['closed_lost_date'] = sold_df['closed_lost_date'].apply(date_to_string)
        sold_df['appointment_date'] = sold_df['appointment_date'].apply(date_to_string)

        sold_df['amount_in_home_currency'] = sold_df['amount_in_home_currency'].astype('float64')
        sold_df['system_size'] = sold_df['system_size'].astype('float64')
        sold_df['swf_lead_id'] = sold_df['swf_lead_id'].astype('float64')


        # sold_df['hubspot_owner_id'] = sold_df['hubspot_owner_id'].map(owner_dict)
        # sold_df['original_deal_owner'] = sold_df['original_deal_owner'].map(owner_dict)


        sold_df.drop(['closed_lost_date','closed_by_proposal_tool'],axis=1,inplace=True)


        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Closed Deals Export')
        client.open_by_key(sheet_id).values_clear("Closed Deals Export!A:AD")
        ws.update([sold_df.columns.values.tolist()] + sold_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Sold deals have been uploaded.')

    @attempt_grabs   
    def cancel_grab():
        # creating the separate dataframes for loading data. At a later point, need to modify for creating Tableau/SQL databases
        cancel_df = pd.DataFrame(data = deal_list[1::], columns=deal_list[0])
        cancel_df = cancel_df[cancel_properties]
        cancel_df.drop(cancel_df.index[cancel_df['closedate']==''],inplace=True)
        cancel_df.drop(cancel_df.index[cancel_df['closed_by_proposal_tool']!='Yes'],inplace=True)
        cancel_df.drop(cancel_df.index[cancel_df['dealtype']!='newbusiness'],inplace=True)
        cancel_df['closedate'] = pd.to_datetime(cancel_df['closedate'],errors='coerce')
        cancel_df['closed_lost_date'] = pd.to_datetime(cancel_df['closed_lost_date'],errors='coerce')
        cancel_df.drop(['dealtype'],axis=1,inplace=True)


        cancel_df.drop(cancel_df.index[cancel_df['closedate']<cd_year_cutoff],inplace=True)
        cancel_df['amount_in_home_currency'] = cancel_df['amount_in_home_currency'].apply(null_handler)
        cancel_df['system_size'] = cancel_df['system_size'].apply(null_handler)
        cancel_df['swf_lead_id'] = cancel_df['swf_lead_id'].apply(null_handler)

        cancel_df['amount_in_home_currency'] = cancel_df['amount_in_home_currency'].astype('float64')
        cancel_df['system_size'] = cancel_df['system_size'].astype('float64')
        cancel_df['swf_lead_id'] = cancel_df['swf_lead_id'].astype('float64')


        cancel_df.drop(cancel_df.index[cancel_df['dealstage']!="Closed Lost"],inplace=True)


        # cancel_df['hubspot_owner_id'] = cancel_df['hubspot_owner_id'].map(owner_dict)
        # cancel_df['original_deal_owner'] = cancel_df['original_deal_owner'].map(owner_dict)
        cancel_df['ec_region'] = cancel_df['original_deal_owner'].map(sales_map)
        cancel_df.sort_values(by='closed_lost_date',ascending=False,inplace=True)

        cancel_df['closedate'] = cancel_df['closedate'].apply(date_to_string)
        cancel_df['closed_lost_date'] = cancel_df['closed_lost_date'].apply(date_to_string)

        cancel_df.fillna('',inplace=True)

        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Cancellations Upload')
        client.open_by_key(sheet_id).values_clear("Cancellations Upload!A:Y")
        ws.update([cancel_df.columns.values.tolist()] + cancel_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Cancellations have been uploaded.')


    # Need to modify this script to update SQL database with the Funnel DF

    def funnel_grab():
        pass
        # funnel_df = pd.DataFrame(data = deal_list[1::], columns=all_properties)
        # funnel_df = funnel_df[cross_funnel_properties]
        # funnel_df.drop(funnel_df.index[funnel_df['dealtype']!='newbusiness'],inplace=True)
        # funnel_df.drop(['dealtype'],axis=1,inplace=True)
        # funnel_df.drop(funnel_df.index[funnel_df['associated_contact']==''],inplace=True)
        # funnel_df['associated_contact'] = funnel_df['associated_contact'].astype('float64')

        # funnel_df.drop(funnel_df.index[funnel_df['appointment_date']==''],inplace=True)
        # funnel_df['appointment_date'] = funnel_df['appointment_date'].apply(datetime_converter)
        # funnel_df['createdate'] = funnel_df['createdate'].apply(datetime_converter)

        # funnel_df.drop(funnel_df.index[funnel_df['appointment_date']<cd_year_cutoff],inplace=True)


        # funnel_df['amount_in_home_currency'] = funnel_df['amount_in_home_currency'].apply(null_handler)

        # funnel_df['amount_in_home_currency'] = funnel_df['amount_in_home_currency'].astype('float64')

        # funnel_df['dealstage'] = funnel_df['dealstage'].map(deal_stage_map)

        # funnel_df['hubspot_owner_id'] = funnel_df['hubspot_owner_id'].map(owner_dict)

        # funnel_df['appointment_date'] = funnel_df['appointment_date'].apply(date_to_string)
        # funnel_df['createdate'] = funnel_df['createdate'].apply(date_to_string)
        # funnel_df.fillna('',inplace=True)


        # # importing and cleaning contacts database for merge


        # contacts_df = pd.read_csv(data_path_base + 'contacts_export.csv')
        # contacts_df.fillna('',inplace=True)

        # contacts_df.rename(columns = {'hs_object_id': 'Contact ID'},inplace=True)
        # funnel_df.rename(columns = {'associated_contact': 'Contact ID'},inplace=True)

        # # Commenting out line below to add in Hubspot mappings
        # # contacts_df['lead_source_corrected'] = contacts_df['lead_source'].apply(contacts_lead_map)

        # contacts_df.drop(columns = ['lead_type','zombie'],axis=1,inplace=True)
        # contacts_df['Contact ID'] = contacts_df['Contact ID']

        # merged = contacts_df.merge(funnel_df, on = 'Contact ID', how = 'left')
        # merged.fillna('',inplace=True)
        # merged.drop(columns = ['dealname','hs_object_id'],axis=1,inplace=True)
        # merged.rename(columns = {'hubspot_owner_id_x': 'Contact Owner',
        #                         'hubspot_owner_id_y': 'Deal Owner',
        #                         'createdate_y': 'Appointment Create Date'},
        #             inplace=True)

        # merged_headers = merged.columns.values.tolist()

        # merged.to_excel(data_path_base + 'funnel_data.xlsx',index=False)
        # merged_cleaned = merged.values.tolist()
        # merged_cleaned.insert(0,merged_headers)

        # # splitting merge due to size. Going to work in two parts for the moment

        # length = len(merged_cleaned)
        # half_point = math.floor(length/2)

        # cleaned1 = merged_cleaned[0:half_point]
        # cleaned2 = merged_cleaned[half_point:length]


        # merged_id = '1tCouR5cOgezklym3f0tJCDn0ymgedJy2-Dgnkv4pXJY'
        # merged_range1 = 'Contacts Import!A1:AJ'+str(len(cleaned1))
        # merged_clear_range1 = 'Contacts Import!A1:AJ'

        # google_update(merged_id,merged_range1,cleaned1,merged_clear_range1)

        # merged_range2 = 'Contacts Import!A'+str(half_point)+':AJ'+str(len(merged_cleaned))
        # merged_clear_range2 = 'Contacts Import!A'+str(half_point)+':AJ'

        # print()
        # print('First half of marketing funnel items uploaded.')


        # google_update(merged_id,merged_range2,cleaned2,merged_clear_range2)

        # print('Second half of marketing funnel items have been uploaded.')

        # # area to update marketing funnel's date so marketing can see latest update time


        # now = datetime.datetime.now().strftime("%m/%d/%Y %H:%M:%S")
        # body_update = {'values': [[now]]}

    
        # if os.path.exists(sheets_token):
        #     with open(sheets_token, 'rb') as token:
        #         creds = pickle.load(token)
        # service = build('sheets', 'v4', credentials=creds)

        # sheet = service.spreadsheets()
        
        # requested_ID = ''
        # sheets_range = 'Marketing Funnel - Overview!J1'

        # sheet.values().update(spreadsheetId=requested_ID, range = sheets_range,
        #                                 body=body_update,valueInputOption='USER_ENTERED').execute()

        # print('Timestamp Updated.')


    #Run individual grab functions
    appointments_grab()
    sold_grab()
    cancel_grab()

    try:
        funnel_grab()
    except:
        print('Funnel DF Failed')



    print('')
    print('All uploads completed.')

    return deal_list

    
if __name__ == "__main__":
    api_key = os.environ['hapikey']
    hubspot = HubspotApi(api_key)

    all_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','agent_name',
                            'appointment_date','top_deal_sources','appointment_type','deal_lead_source',
                            'deal_marketing_lead_category','hubspot_owner_id','dealtype',
                            'market_region','run_status','appointment_ran_by','appointment_disposition','swf_lead_id',
                            'closed_lost_notes','call_center_return_notes','closed_lost_reason','createdate',
                            'has_been_rescheduled','closed_by_proposal_tool','module_type',
                            'original_deal_owner','commission','system_size','tesla_powerwall_quantity','tesla_powerwall_revenue',
                            'module_quantity','closed_lost_date','closedate','cc_notes_important',
                            'call_center_reschedule_disposition','closed_won_reason','appointment_date_passed',
                            'promotion_rebate','promotion_discount','sunpower_rebate_adder','financing_fees','mumd_promotion',
                            'true_promotion','promotion_adder','cac_lead_source','cac_category_lead_source','full_promotion_true_paid_fsp',
                            'full_promotion_total','full_promotion_mumd_fsp','sunpower_rebate_total','promotion_true_paid_by_fsp',
                            'promotion_mumd_fsp','promotion_total','utility','canvasser_name','net_revenue','powerwall_split_portion','cash_loan',
                            'send_deposit_to_accounting','matt_deposit_check','project_sunrise_id','deposit_sent_integration_failure',
                            'hold_type','powerwall_only','payment_option']            

    results = hubspot.get_all_deals(all_properties)
    deal_list_raw = hubspot.parse_deals()
    deal_list = map_deals_list(deal_list_raw)
    run_grab(deal_list)
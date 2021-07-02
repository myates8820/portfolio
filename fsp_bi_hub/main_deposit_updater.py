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
from subfunctions.deal_parser import map_deals_list
from subfunctions.data_tests.deposit_integration_checker import deposit_integration_check

hs_api_key = os.environ['hapikey']

print('Deal Updater Started')

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

# hubspot = HubspotApi(hs_api_key)
# results = hubspot.get_all_deals(all_properties)
# deal_list_raw = hubspot.parse_deals()
# deal_list = map_deals_list(deal_list_raw)

# print('Deals have been parsed.')

deposit_integration_check()
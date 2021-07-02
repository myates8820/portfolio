import os

from sqlalchemy.sql.sqltypes import BIGINT
from subfunctions.Hubspot_API import HubspotApi
from subfunctions.deal_parser import map_deals_list
import subfunctions.contacts_grab as contacts_grab
import numpy as np
import pandas as pd
import datetime
from time import sleep
import psycopg2
from typing import Iterator, Optional
import io
from oauth2client.service_account import ServiceAccountCredentials
import gspread

import json,requests
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, BigInteger


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

from all_properties import all_properties          

hubspot = HubspotApi(hs_api_key)
results = hubspot.get_all_deals(all_properties)
deal_list_raw = hubspot.parse_deals()
deal_list = map_deals_list(deal_list_raw)
df = pd.DataFrame(data = deal_list[1::], columns=deal_list[0])



print('Function started')

#Grabbing API key from environment
api_key = os.environ['hapikey']

#creating sqlalchemy engine for creating tables
engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
meta = MetaData()

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



print('')    
print('Deals have been retrieved.')




# Add additional properties to additional_play_properties as ad hoc analyses call for, this way I don't need to mutilate the uploads to deal with non-essential requests

now = datetime.datetime.now()
today = datetime.datetime.strftime(now,'%Y-%m-%d')
cd_year_cutoff = datetime.datetime(2020,1,1)
df_year_cutoff = datetime.datetime(2019,1,1)

# handling missing and non-conforming data to be able to transform dataframes

def null_handler(x):
    if x == '':
        return 0
    elif x is None:
        return 0
    else:
        return x

def date_null_handler(x):
    if x == '':
        return pd.NaT
    elif x is None:
        return pd.NaT
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


# few more functions for cleaning and uploading
def id_handler(x):
    if x is None or x == '':
        return None
    else:
        return str(int(x))

def float_converter(column_array,df):
    for x in column_array:
        df[x].fillna(0.0,inplace=True)
        df[x] = df[x].apply(null_handler)
        df[x] = df[x].apply(lambda x: str(x).replace(',',''))
        df[x] = df[x].astype('float64')

class StringIteratorIO(io.TextIOBase):
    def __init__(self, iter: Iterator[str]):
        self._iter = iter
        self._buff = ''

    def readable(self) -> bool:
        return True

    def _read1(self, n: Optional[int] = None) -> str:
        while not self._buff:
            try:
                self._buff = next(self._iter)
            except StopIteration:
                break
        ret = self._buff[:n]
        self._buff = self._buff[len(ret):]
        return ret

    def read(self, n: Optional[int] = None) -> str:
        line = []
        if n is None or n < 0:
            while True:
                m = self._read1()
                if not m:
                    break
                line.append(m)
        else:
            while n > 0:
                m = self._read1(n)
                if not m:
                    break
                n -= len(m)
                line.append(m)
        return ''.join(line)


# takes sql table and matching df and uploads data. need to drop/clear table if already exists
def item_uploader(table_name,upload_df):

    df_values_list = upload_df.values.tolist()
    df_columns = upload_df.columns.tolist()

    upload_conn = psycopg2.connect(database=os.environ['database'], user=os.environ['user'], 
                            password = os.environ['password'],
                            host = os.environ['host'])


    try:
#     modified function from above link, simplified to work with paginated list and structure instead of dictionaries
        def string_io_iterator(connection,table,columns,rows: Iterator[list]) -> None:
            with connection.cursor() as cursor:
                    row_string_iterator = StringIteratorIO((
                        row + '\n'
                        for row in rows
                    ))
                    cursor.copy_from(row_string_iterator,table,columns=columns,sep=";", null='missing_data')
                    connection.commit()

        # quick loop to enable pagination for the upload
        item_number = 0
        full_length = len(df_values_list)
        final_page = False
        paginate = True
        while paginate:

            pagination_length = 500
            end_number = item_number+pagination_length
            if end_number>=full_length:
                end_index = full_length
                final_page = True
            else:
                end_index = end_number
            # print(end_index)

            value_list = []
            for x in df_values_list[item_number:end_index]:
                string_row = []
                for i in x:
                    if i is None:
                        string_row.append('missing_data')
                    elif i == 'nan':
                        string_row.append('missing_data')
                    elif i == '':
                        string_row.append('missing_data')
                    else:
                        string_row.append(str(i).replace(';',':').replace('\n',':').replace('"','').replace('\.','\\')) #need to fix any instances of ; so it doesn't break the delimiter
                row = ";".join(string_row)
                value_list.append(str(row))

            string_io_iterator(upload_conn,table_name,df_columns,value_list)

            if final_page:
                paginate=False
            else:
                item_number+=pagination_length
    except Exception as e:
        print(e)



# creating function decorator to attempt each upload 3 times in case of timeouts, etc.
def attempt_grabs(func):
    def wrapper_attempter():
        x = 0
        while x < 3:
            try:
                func()
                break
            except Exception as e:
                x+=1
                print(f'Attempt {x} failed. The following errors was produced:')
                print(e)
                print('Upload Failed. Sleeping for 60 seconds and starting again. Will attempt 3 times.')           
                sleep(60)
    return wrapper_attempter

# @attempt_grabs
def upload():

    df = pd.DataFrame(data = deal_list[1::], columns=deal_list[0])
    date_properties = ['createdate','appointment_date','closedate','closed_lost_date']
    for x in date_properties:
        df[x] = pd.to_datetime(df[x],errors='coerce')

    float_properties = ['amount_in_home_currency','commission','system_size','tesla_powerwall_quantity','tesla_powerwall_revenue','module_quantity',
            'financing_fees','full_promotion_true_paid_fsp','full_promotion_mumd_fsp','adders_price','adders_cost','sunpower_rebate_adder',
            'promotion_total','sunpower_rebate_total','promotion_discount','net_revenue','run_number','promotion_rebate','mumd_promotion','true_promotion',
            'promotion_adder','full_promotion_total','full_promotion_total','full_promotion_mumd_fsp','sunpower_rebate_total','promotion_true_paid_by_fsp',
            'promotion_mumd_fsp','promotion_total']

    df['ec_region'] = df['original_deal_owner'].map(sales_map)

    # Adding run number and reorganizing to match Sheets
    df['run_number'] = df['run_status'].apply(run_number)
    df.replace({'nan': None,np.nan:None},inplace=True)
    df['project_sunrise_id'] = df['project_sunrise_id'].apply(id_handler)
    float_converter(float_properties,df)
    df.replace({'nan': None,np.nan:None,'':None},inplace=True)

    # creating engine and resetting table to upload data
    engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
    meta = MetaData()
    with engine.connect() as conn:
        conn.execute("DROP TABLE IF EXISTS full_cleaned;")

    full_cleaned = Table(
        'full_cleaned', meta,
        Column('associated_contact',String),
        Column('hs_object_id',BigInteger),
        Column('dealname',String),
        Column('amount_in_home_currency', Float),
        Column('dealstage',String),
        Column('agent_name',String),
        Column('appointment_date',DateTime),
        Column('top_deal_sources',String),
        Column('appointment_type',String),
        Column('deal_lead_source',String),
        Column('deal_marketing_lead_category',String),
        Column('hubspot_owner_id',String),
        Column('dealtype',String),
        Column('market_region',String),
        Column('run_status',String),
        Column('appointment_ran_by',String),
        Column('appointment_disposition',String),
        Column('swf_lead_id',String),
        Column('closed_lost_notes',String),
        Column('call_center_return_notes',String),
        Column('closed_lost_reason',String),
        Column('createdate',DateTime),
        Column('has_been_rescheduled',String),
        Column('closed_by_proposal_tool',String),
        Column('module_type',String),
        Column('original_deal_owner',String),
        Column('commission',Float),
        Column('system_size',Float),
        Column('tesla_powerwall_quantity',Float),
        Column('tesla_powerwall_revenue',Float),
        Column('module_quantity',Float),
        Column('closed_lost_date',DateTime),
        Column('closedate',DateTime),
        Column('cc_notes_important',String),
        Column('call_center_reschedule_disposition',String),
        Column('closed_won_reason',String),
        Column('appointment_date_passed',String),
        Column('promotion_rebate',Float),
        Column('promotion_discount',Float),
        Column('sunpower_rebate_adder',Float),
        Column('financing_fees',Float),
        Column('mumd_promotion',Float),
        Column('true_promotion',Float),
        Column('promotion_adder',Float),
        Column('cac_lead_source',String),
        Column('cac_category_lead_source',String),
        Column('full_promotion_true_paid_fsp',Float),
        Column('full_promotion_total',Float),
        Column('full_promotion_mumd_fsp',Float),
        Column('sunpower_rebate_total',Float),
        Column('promotion_true_paid_by_fsp',Float),
        Column('promotion_mumd_fsp',Float),
        Column('promotion_total',Float),
        Column('utility',String),
        Column('canvasser_name',String),
        Column('net_revenue',Float),
        Column('powerwall_split_portion',String),
        Column('cash_loan',String),
        Column('send_deposit_to_accounting',String),
        Column('matt_deposit_check',String),
        Column('project_sunrise_id',BigInteger),
        Column('deposit_sent_integration_failure',String),
        Column('hold_type',String),
        Column('powerwall_only',String),
        Column('payment_option',String),
        Column('adders_price',Float),
        Column('adders_cost',Float),
        Column('ec_region',String),
        Column('run_number',Float),
        Column('bonus_eligible',String),
        Column('sales_bonus_region',String),
        Column('team_bonus',String),
    )

    meta.create_all(engine)
    item_uploader('full_cleaned',df)

    print ('Uploaded Cleaned Deals Table')


upload()
print('All uploads completed.')

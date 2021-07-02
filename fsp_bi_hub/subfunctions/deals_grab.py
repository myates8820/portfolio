import os
from subfunctions.Hubspot_API import HubspotApi
from subfunctions.deal_parser import map_deals_list

def run_grab():
    import gspread
    from oauth2client.service_account import ServiceAccountCredentials
    import numpy as np
    import pandas as pd
    import datetime
    from time import sleep
    import os
    import psycopg2
    from typing import Iterator, Optional
    import io

    import json
    from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, BigInteger

    print('Function started')

    from all_properties import all_properties 

    #Grabbing API key from environment
    api_key = os.environ['hapikey']

    #creating sqlalchemy engine for creating tables
    engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
    meta = MetaData()


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
    deal_list = query_selector(deal_query,'*','full_cleaned')
    df = pd.DataFrame(data=deal_list[1::],columns=deal_list[0]).fillna('')
    # deal_list = all_deals       

    # print('')    
    # print('Deals have been retrieved.')
    # print(deal_list[0:2])

    # Area for selecting which columns to grab for each data upload

    appointment_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','agent_name',
                            'appointment_date','top_deal_sources','appointment_type','deal_lead_source',
                            'canvasser_name','hubspot_owner_id','dealtype',
                            'market_region','run_status','appointment_disposition','project_sunrise_id',
                            'cac_lead_source','cac_category_lead_source','closed_lost_reason','createdate',
                            'has_been_rescheduled','closed_by_proposal_tool','powerwall_split_portion']

    appointment_float_properties = ['amount_in_home_currency','run_number']

    sold_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','closedate',
                    'appointment_date','appointment_type','market_region','hubspot_owner_id','module_type',
                    'original_deal_owner','commission','run_status','project_sunrise_id','system_size','cash_loan',
                    'deal_lead_source','tesla_powerwall_quantity','tesla_powerwall_revenue','module_quantity',
                    'closed_lost_date','closed_by_proposal_tool','dealtype','financing_fees','full_promotion_true_paid_fsp','full_promotion_mumd_fsp',
                    'promotion_total','sunpower_rebate_total','promotion_discount','cac_lead_source','cac_category_lead_source',
                    'net_revenue','powerwall_split_portion','adders_price','powerwall_only','send_deposit_to_accounting','utility',
                    'payment_option','hold_type','adders_cost']

    sold_float_properties = ['amount_in_home_currency','commission','system_size','tesla_powerwall_quantity','tesla_powerwall_revenue','module_quantity',
                    'financing_fees','full_promotion_true_paid_fsp','full_promotion_mumd_fsp','adders_price','adders_cost',
                    'promotion_total','sunpower_rebate_total','promotion_discount','net_revenue']


    cancel_properties = ['hs_object_id','dealname','project_sunrise_id','amount_in_home_currency','closedate','dealstage',
                        'hubspot_owner_id','top_deal_sources','closed_lost_date','closed_lost_reason','closed_lost_notes',
                        'market_region','system_size','deal_lead_source','tesla_powerwall_quantity','tesla_powerwall_revenue',
                        'closed_by_proposal_tool','dealtype','cac_lead_source','cac_category_lead_source','financing_fees','net_revenue','original_deal_owner',
                        'agent_name','canvasser_name','powerwall_split_portion','powerwall_only']

    cancel_float_properties = ['amount_in_home_currency','system_size','tesla_powerwall_quantity','tesla_powerwall_revenue',
                        'financing_fees','net_revenue']


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
    df_year_cutoff = datetime.datetime(2019,1,1)

    # handling missing and non-conforming data to be able to transform dataframes

    def null_handler(x):
        if x == '':
            return 0
        elif x is None:
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
                        else:
                            string_row.append(str(i).replace(';',':')) #need to fix any instances of ; so it doesn't break the delimiter
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

    @attempt_grabs
    def appointments_grab():

        appointments_df = df.copy()
        appointments_df = appointments_df[appointment_properties]
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']==''],inplace=True)

        # converting create date and appointment date to datetime format
        appointments_df['createdate'] = pd.to_datetime(appointments_df['createdate'],errors='coerce')
        appointments_df['appointment_date'] = pd.to_datetime(appointments_df['appointment_date'],errors='coerce')

        # Removing all appointments from today to future, and all before 2020 to keep upload small
        appointments_df.drop(appointments_df.index[(appointments_df['powerwall_split_portion']=='Yes') & (appointments_df['closed_by_proposal_tool'] == 'Yes')],inplace=True)

        # Adding run number and reorganizing to match Sheets
        appointments_df['run_number'] = appointments_df['run_status'].apply(run_number)
        appointment_properties.insert(14,'run_number')
        appointments_df = appointments_df[appointment_properties]

        # removing columns and then sorting
        appointments_df.drop(['has_been_rescheduled'],axis=1,inplace=True)
        appointments_df.drop(['closed_by_proposal_tool'],axis=1,inplace=True)
        appointments_df.drop(['powerwall_split_portion'],axis=1,inplace=True)

        appointments_df.sort_values(by='appointment_date',ascending=False,inplace=True)

        appointments_df.replace({'nan': None,np.nan:None},inplace=True)
        appointments_df['project_sunrise_id'] = appointments_df['project_sunrise_id'].apply(id_handler)
        float_converter(appointment_float_properties,appointments_df)
        appointments_df.replace({'nan': None,np.nan:None},inplace=True)

        # creating engine and resetting table to upload data
        engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
        meta = MetaData()
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS appointments_cleaned;")

        appointments_cleaned = Table(
            'appointments_cleaned', meta,
            Column('hs_object_id',BigInteger),
            Column('dealname',String),
            Column('amount_in_home_currency', Float),
            Column('dealstage',String),
            Column('agent_name',String),
            Column('appointment_date',DateTime),
            Column('top_deal_sources',String),
            Column('appointment_type',String),
            Column('deal_lead_source',String),
            Column('canvasser_name',String),
            Column('hubspot_owner_id',String),
            Column('dealtype',String),
            Column('market_region',String),
            Column('run_status',String),
            Column('run_number',Float),
            Column('appointment_disposition',String),
            Column('project_sunrise_id',BigInteger),
            Column('cac_lead_source',String),
            Column('cac_category_lead_source',String),
            Column('closed_lost_reason',String),
            Column('createdate',DateTime),
            Column('powerwall_split_portion',String)
            
        )

        meta.create_all(engine)
        item_uploader('appointments_cleaned',appointments_df)

        print ('Uploaded Cleaned Appointments Table')

        # Converting datetime objects to strings
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']>=today],inplace=True)
        appointments_df.drop(appointments_df.index[appointments_df['appointment_date']<cd_year_cutoff],inplace=True)
        appointments_df['createdate'] = appointments_df['createdate'].apply(date_to_string)
        appointments_df['appointment_date'] = appointments_df['appointment_date'].apply(date_to_string)


        appointments_df.fillna('',inplace=True)
       
        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Appointments Export')
        client.open_by_key(sheet_id).values_clear("Appointments Export!A:U")

        ws.update([appointments_df.columns.values.tolist()] + appointments_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Appointments have been uploaded.')
        del appointments_df

    @attempt_grabs
    def sold_grab():
        # creating the separate dataframes for loading data. At a later point, need to modify for creating Tableau/SQL databases
        # Follows same structure as above with datetime objects and drop criteria

        sold_df = df.copy()
        sold_df = sold_df[sold_properties]
        sold_df.drop(sold_df.index[sold_df['closed_by_proposal_tool']!='Yes'],inplace=True)
        sold_df.drop(sold_df.index[sold_df['dealtype']!='newbusiness'],inplace=True)
        sold_df.drop(sold_df.index[sold_df['closedate']==''],inplace=True)
        sold_df = sold_df[(sold_df.project_sunrise_id != '') & (sold_df.project_sunrise_id.isnull()==False)]
        sold_df['closedate'] = pd.to_datetime(sold_df['closedate'],errors='coerce')
        sold_df['appointment_date'] = pd.to_datetime(sold_df['appointment_date'],errors='coerce')
        sold_df['closed_lost_date'] = pd.to_datetime(sold_df['closed_lost_date'],errors='coerce')
        sold_df.drop(['dealtype'],axis=1,inplace=True)
        sold_df = sold_df[(sold_df.dealstage == 'Closed Won') | (sold_df.dealstage == 'Closed Lost')]

        #Testing due to dropoff
        # sold_df.drop(sold_df.index[sold_df['closedate']>=today],inplace=True)
        sold_df.drop(sold_df.index[sold_df['closedate']<df_year_cutoff],inplace=True)
        sold_df.sort_values(by='closedate',ascending=False,inplace=True)


        sold_df['amount_in_home_currency'] = sold_df['amount_in_home_currency'].apply(null_handler)
        sold_df['system_size'] = sold_df['system_size'].apply(null_handler)


        sold_df['amount_in_home_currency'] = sold_df['amount_in_home_currency'].astype('float64')
        sold_df['system_size'] = sold_df['system_size'].astype('float64')

        sold_df['project_sunrise_id'] = sold_df['project_sunrise_id'].apply(lambda x: str(x).replace(',','').replace('.0',''))
        sold_df['project_sunrise_id'] = sold_df['project_sunrise_id'].astype('int64')
        sold_df['project_sunrise_id'] = sold_df['project_sunrise_id'].apply(lambda x: str(x))
        sold_df['powerwall_split_portion'].replace({'',None},inplace=True)
        float_converter(sold_float_properties,sold_df)
        sold_df.replace({'nan': None,np.nan:None},inplace=True)

        sold_df['original_deal_owner'] = np.where(sold_df.original_deal_owner.isnull(),sold_df.hubspot_owner_id,sold_df.original_deal_owner)
        sold_df['ec_region'] = sold_df['original_deal_owner'].map(sales_map)
        
        # sold_df.rename(columns={'hubspot_owner_id': 'hubspot_owner'},inplace=True)

        # creating engine and resetting table to upload data
        engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
        meta = MetaData()
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS deals_cleaned;")

        deals_cleaned = Table(
            'deals_cleaned', meta,
            Column('hs_object_id',BigInteger),
            Column('dealname',String),
            Column('amount_in_home_currency', Float),
            Column('dealstage',String),
            Column('closedate',DateTime),
            Column('appointment_date',DateTime),
            Column('appointment_type',String),
            Column('market_region',String),
            Column('hubspot_owner_id',String),
            Column('module_type',String),
            Column('original_deal_owner',String),
            Column('commission',Float),
            Column('run_status',String),
            Column('project_sunrise_id',BigInteger),
            Column('system_size',Float),
            Column('cash_loan',String),
            Column('deal_lead_source',String),
            Column('tesla_powerwall_quantity',Float),
            Column('tesla_powerwall_revenue',Float),
            Column('module_quantity',Float),
            Column('financing_fees',Float),
            Column('full_promotion_true_paid_fsp',Float),
            Column('full_promotion_mumd_fsp',Float),
            Column('promotion_total',Float),
            Column('sunpower_rebate_total',Float),
            Column('promotion_discount',Float),
            Column('cac_lead_source',String),
            Column('cac_category_lead_source',String),
            Column('net_revenue',Float),
            Column('powerwall_split_portion',String),
            Column('closed_lost_date',DateTime),
            Column('closed_by_proposal_tool',String),
            Column('ec_region',String),
            Column('adders_price',Float),
            Column('powerwall_only',String),
            Column('send_deposit_to_accounting',String),
            Column('utility',String),
            Column('payment_option',String),
            Column('hold_type',String),
            Column('adders_cost',Float),

        )

        meta.create_all(engine)
        item_uploader('deals_cleaned',sold_df)

        print ('Uploaded Cleaned Deals Table')


        # moving to strings for google upload
        sold_df.drop(sold_df.index[sold_df['closedate']<cd_year_cutoff],inplace=True)
        sold_df.fillna('',inplace=True)
        sold_df['closedate'] = sold_df['closedate'].apply(date_to_string)
        sold_df['closed_lost_date'] = sold_df['closed_lost_date'].apply(date_to_string)
        sold_df['appointment_date'] = sold_df['appointment_date'].apply(date_to_string)


        sold_df.drop(['closed_lost_date','closed_by_proposal_tool','ec_region','adders_price','powerwall_only',
            'send_deposit_to_accounting','utility','payment_option','hold_type','adders_cost'],axis=1,inplace=True)


        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Closed Deals Export')
        client.open_by_key(sheet_id).values_clear("Closed Deals Export!A:AD")
        ws.update([sold_df.columns.values.tolist()] + sold_df.values.tolist(), value_input_option="USER_ENTERED")

        sold_list = sold_df.values.tolist()
        sold_list.insert(0,sold_df.columns.tolist())
        
        print('Sold deals have been uploaded.')
        del sold_df

    @attempt_grabs   
    def cancel_grab():
        # creating the separate dataframes for loading data. At a later point, need to modify for creating Tableau/SQL databases
        cancel_df = df.copy()
        cancel_df = cancel_df[cancel_properties]
        cancel_df.drop(cancel_df.index[cancel_df['closedate']==''],inplace=True)
        cancel_df.drop(cancel_df.index[cancel_df['closed_by_proposal_tool']!='Yes'],inplace=True)
        cancel_df.drop(cancel_df.index[cancel_df['dealtype']!='newbusiness'],inplace=True)
        cancel_df['closedate'] = pd.to_datetime(cancel_df['closedate'],errors='coerce')
        cancel_df['closed_lost_date'] = pd.to_datetime(cancel_df['closed_lost_date'],errors='coerce')
        cancel_df.drop(['dealtype'],axis=1,inplace=True)


        cancel_df.drop(cancel_df.index[cancel_df['closedate']<df_year_cutoff],inplace=True)
        cancel_df['amount_in_home_currency'] = cancel_df['amount_in_home_currency'].apply(null_handler)
        cancel_df['system_size'] = cancel_df['system_size'].apply(null_handler)

        cancel_df['amount_in_home_currency'] = cancel_df['amount_in_home_currency'].astype('float64')
        cancel_df['system_size'] = cancel_df['system_size'].astype('float64')
        cancel_df.drop(cancel_df.index[cancel_df['dealstage']!="Closed Lost"],inplace=True)
        cancel_df['ec_region'] = cancel_df['original_deal_owner'].map(sales_map)
        cancel_df.sort_values(by='closed_lost_date',ascending=False,inplace=True)

        cancel_df.replace({'nan': None,np.nan:None},inplace=True)
        cancel_df['project_sunrise_id'] = cancel_df['project_sunrise_id'].apply(id_handler)
        float_converter(cancel_float_properties,cancel_df)
        cancel_df.replace({'nan': None,np.nan:None},inplace=True)
        cancel_df['powerwall_split_portion'].replace({'',None},inplace=True)
        cancel_df.columns.tolist()

        engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
        meta = MetaData()
        with engine.connect() as conn:
            conn.execute("DROP TABLE IF EXISTS cancels_cleaned;")

        cancels_cleaned = Table(
            'cancels_cleaned', meta,
            Column('hs_object_id',BigInteger),
            Column('dealname',String),
            Column('project_sunrise_id',BigInteger),
            Column('amount_in_home_currency', Float),
            Column('closedate',DateTime),
            Column('dealstage',String),
            Column('hubspot_owner_id',String),
            Column('top_deal_sources',String),
            Column('closed_lost_date',DateTime),
            Column('closed_lost_reason',String),
            Column('closed_lost_notes',String),
            Column('market_region',String),
            Column('system_size',Float),
            Column('deal_lead_source',String),
            Column('tesla_powerwall_quantity',Float),
            Column('tesla_powerwall_revenue',Float),
            Column('closed_by_proposal_tool',String),
            Column('cac_lead_source',String),
            Column('cac_category_lead_source',String),
            Column('financing_fees',Float),
            Column('net_revenue',Float),
            Column('original_deal_owner',String),
            Column('agent_name',String),
            Column('canvasser_name',String),
            Column('ec_region',String),
            Column('powerwall_split_portion',String),
            Column('powerwall_only',String),
        )

        meta.create_all(engine)
        item_uploader('cancels_cleaned',cancel_df)
        print('Uploaded Cleaned Cancels Table')


        cancel_df.drop(cancel_df.index[cancel_df['closedate']<cd_year_cutoff],inplace=True)
        cancel_df['closedate'] = cancel_df['closedate'].apply(date_to_string)
        cancel_df['closed_lost_date'] = cancel_df['closed_lost_date'].apply(date_to_string)

        cancel_df.fillna('',inplace=True)

        sheet_id = os.environ['appointments_sheet_id']
        ws = client.open_by_key(sheet_id).worksheet('Cancellations Upload')
        client.open_by_key(sheet_id).values_clear("Cancellations Upload!A:Y")
        ws.update([cancel_df.columns.values.tolist()] + cancel_df.values.tolist(), value_input_option="USER_ENTERED")

        print('Cancellations have been uploaded.')
        del cancel_df


    # Need to modify this script to update SQL database with the Funnel DF
    def funnel_grab():
        pass
        # cross_funnel_properties = ['associated_contact','hs_object_id','dealname','dealname','dealname','amount_in_home_currency','dealstage',
        #                 'appointment_date','appointment_disposition','cc_notes_important','call_center_return_notes',
        #                     'run_status','call_center_reschedule_disposition','closed_lost_reason','closed_lost_notes',
        #                     'hubspot_owner_id','closed_by_proposal_tool','createdate','agent_name','dealtype']
        # funnel_df = df.copy()
        # funnel_df = funnel_df[sold_properties]
        # funnel_df.drop(funnel_df.index[funnel_df['dealtype']!='newbusiness'],inplace=True)
        # funnel_df.drop(['dealtype'],axis=1,inplace=True)
        # funnel_df.drop(funnel_df.index[funnel_df['associated_contact']==''],inplace=True)
        # funnel_df['associated_contact'] = funnel_df['associated_contact'].astype('float64')
        # funnel_df.drop(funnel_df.index[funnel_df['appointment_date']==''],inplace=True)
        # funnel_df['appointment_date'] = pd.to_datetime(funnel_df['appointment_date'],errors='coerce')
        # funnel_df['closedate'] = pd.to_datetime(funnel_df['closedate'],errors='coerce')
        # funnel_df.fillna('',inplace=True)

        # contacts_raw = query_selector()

    #Run individual grab functions
    appointments_grab()
    sold_grab()
    cancel_grab()

    try:
        funnel_grab()
    except Exception as e:
        print('Funnel DF Failed')
        print(e)



    print('')
    print('All uploads completed.')

    # returning sold_list
    def get_sold_list():
        sold_df = df.copy()
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

        sold_df['closedate'] = sold_df['closedate'].apply(date_to_string)
        sold_df['closed_lost_date'] = sold_df['closed_lost_date'].apply(date_to_string)
        sold_df['appointment_date'] = sold_df['appointment_date'].apply(date_to_string)

        sold_df['amount_in_home_currency'] = sold_df['amount_in_home_currency'].astype('float64')
        sold_df['system_size'] = sold_df['system_size'].astype('float64')




        sold_list = sold_df.values.tolist()
        sold_list.insert(0,sold_df.columns.tolist())
        del sold_df
        
        print('Sold deals have been uploaded.')
        return sold_list

    # temporary fix for getting the sold deals df
    try:
        sold_values = get_sold_list()
        return sold_values
    except:
        print("Sold List Unable to Be Returned")

    
if __name__ == "__main__":
    # pass
    run_grab()
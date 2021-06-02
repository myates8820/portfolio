""" Still WIP """


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
import pandas as pd
import datetime
from time import sleep
import time
import sys, os
from sqlalchemy import create_engine, insert, update, delete, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, select, BigInteger
import requests
import json
from subfunctions.Hubspot_API import HubspotApi
from subfunctions.deal_parser import map_deals_list
import psycopg2


def update_task_database(data,full_reset):
    import json
    import requests
    import pandas as pd
    import numpy as np
    import psycopg2
    from time import sleep
    from oauth2client.service_account import ServiceAccountCredentials
    import os
    import pickle
    import datetime
    from sqlalchemy import create_engine, insert, update, delete, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, select, BigInteger
    import psycopg2

    # authorizing gspread
    token = os.environ['sheets_token']
    scopes = ['https://spreadsheets.google.com/feeds']


    creds_dict = json.loads(token)
    creds_dict["private_key"] = creds_dict["private_key"].replace("\\\\n", "\n")
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scopes)
    client = gspread.authorize(creds)

    # setting up task names for database
    old_task_names = ['upload_design_packet_pe_stamp_set', 'upload_structural_letter', 'post_install_call', 'complete_utility_inspection', 'schedule_utility_inspection', 'utility_bill', 
                  'quality_assurance_complete', 'signed_contract', 'upload_eddie_layout', 'final_proposal', 'upload_aurora_layout', 'confirm_deposit_sent_to_accounting', 'upload_proposed_svl',
                  'purchasing_complete', 'complete_monitoring', 'register_warranty', 'receive_final_payment', 'commission_system', 'welcome_call_complete', 'upload_report_photos_to_server', 
                  'crew_installation_complete', 'upload_pto_letter', 'upload_commissioning_report', 'confirm_deposit_received', 'schedule_shutdown', 'schedule_installation', 'upload_approved_bom', 
                  'submit_utility_rebate_application', 'complete_city_inspection', 'quality_review_complete', 'allocate_site_assessment', 'submit_architectural_improvement_request', 
                  'upload_design_packet_construction_set', 'contract_review_complete', 'upload_permit_application_approval', 'receive_sop_payment', 'submit_m2_documents', 'close_project', 
                  'upload_interconnection_application_approval', 'determine_permitting_requirements', 'happiness_call_complete', 'request_design', 'submit_interconnection_application', 
                  'schedule_city_inspection', 'upload_pif_invoice', 'submit_permit_application', 'send_to_intake', 'upload_utility_rebate_application_approval', 'submit_m1_documents', 
                  'upload_design_packet_as_built_set', 'submit_installation_notice', 'receive_rebate_payment', 'complete_closeout_packet', 'dispatch_site_assessment', 'complete_site_assessment', 
                  'upload_architectural_improvement_request_approval']

    # 'complete_closeout_packet' in case to switch with close project

    task_names = ['send_to_intake','signed_contract','confirm_deposit_received','complete_site_assessment','upload_design_packet_construction_set','submit_architectural_improvement_request',
                'submit_permit_application','submit_interconnection_application','submit_utility_rebate_application','upload_architectural_improvement_request_approval',
                'upload_permit_application_approval','upload_interconnection_application_approval','upload_utility_rebate_application_approval','schedule_installation','crew_installation_complete',
                'complete_city_inspection','complete_utility_inspection','close_project','allocate_site_assessment']


    hs_api_key = os.environ['hapikey']
    db_database = os.environ['database']
    db_user = os.environ['user']
    db_password = os.environ['password']
    db_host = os.environ['host']

    def query_selector(query,cols,table):
        conn = psycopg2.connect(database=db_database, user=db_user, 
                            password = db_password,
                            host = db_host)
        cur = conn.cursor()
        cur.execute("rollback;")
        
        query_build = ', '.join(cols)
        query = query.replace('[cols]', query_build).replace('[table]', table)
        cur.execute(query)
        data = cur.fetchall()
        data.insert(0,tuple(cols))
        cur.close()
        conn.close()
        return data

    start_date = datetime.datetime(2019,1,1)
    all_deals_df = pd.DataFrame(data=data[1::],columns = data[0])
    all_deals_df = all_deals_df[all_deals_df.project_sunrise_id.isnull()==False]
    all_deals_df['project_sunrise_id'] = all_deals_df['project_sunrise_id'].apply(lambda x: str(x).replace(',',''))    
    sold_df = all_deals_df[(all_deals_df.closed_by_proposal_tool == 'Yes') & ((all_deals_df.dealstage == 'Closed Won') | (all_deals_df.dealstage == 'Closed Lost'))].copy()
    sold_df['closedate'] = pd.to_datetime(sold_df['closedate'],errors='coerce')
    sold_df = sold_df[sold_df.closedate >= start_date]
    sold_df = sold_df[['hs_object_id','project_sunrise_id','dealname','closedate','appointment_date','amount_in_home_currency','system_size','payment_option',
                    'module_type','module_quantity','tesla_powerwall_quantity','powerwall_only','utility','market_region','hold_type','dealstage','closed_lost_date']]
    sold_df.drop_duplicates(subset='project_sunrise_id',inplace=True)
    sold_df_list = sold_df.values.tolist()
    data_columns = sold_df.columns.tolist()
    df_columns = sold_df.columns.tolist()

    conn_string = os.environ['sql_alchemy_conn_string']
    engine = create_engine(conn_string, echo=False)
    meta = MetaData()
    tasks_wide = Table('tasks_wide', meta, autoload_with=engine)

    # getting columns from table and stripping task_df to these columns in later element. Must make sure to 
    conn = engine.connect()
    s = select([tasks_wide])
    result = conn.execute(s)
    table_keys = result.keys()

    projects_query = """ SELECT [cols] FROM [table] WHERE close_project_completed IS NOT NULL"""
    projects_list = [list(x) for x in query_selector(projects_query,table_keys, 'tasks_wide')]
    projects_df = pd.DataFrame(data=projects_list[1::],columns=projects_list[0])
    close_project_index = projects_df.columns.tolist().index('close_project_completed')
    sunrise_id_index = projects_df.columns.tolist().index('project_sunrise_id')
    projects_df['project_sunrise_id'] = projects_df['project_sunrise_id'].apply(lambda x: str(x))

    finished_ids = projects_df['project_sunrise_id'].values.tolist()


    url_base = os.environ['sunrise_alltasks_base']
    api_key = os.environ['sunrise_api_key']
    org_key = os.environ['sunrise_org_key']
    headers = {'api-key': api_key}


    sunrise_list = []
    raw_sold_df = sold_df_list
    for x in raw_sold_df:
        if x[sunrise_id_index] not in finished_ids:
            sunrise_list.append(x)

    print("Sunrise Grab Started.")
    failures = []     
    failure_responses = []
    sunrise_data = []        
    for x in sunrise_list:
        try_count = 0
        while try_count<3:
            try:
                lead_id = x[sunrise_id_index]
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
                break
            except Exception as e:
                try_count+=1
        if try_count == 3:
            try:
                x.append({'tasks':['No Project Found']})
                sunrise_data.append(x)
                failures.append(x)
                failure_responses.append(json.loads(r.text))        
            except Exception as e:
                print(e) 

    print("Sunrise grab completed.")
    df = pd.DataFrame(columns = ['hs_object_id','project_sunrise_id','dealname','task_dates'])

    for project in sunrise_data:
        try:
            task_dictionary = {}
            task_names_projects = []
            task_array = project[len(project)-1]['tasks']

            # fills dictionary with nulls if no tasks have been created yet, otherwise normal path
            if task_array[0] == 'No Project Found':
                for x in task_names:
                    task_dictionary[x+'_created'] = None
                    task_dictionary[x+'_completed'] = None
                    task_dictionary[x+'_completed_by'] = None

            else:
                for x in task_array:
                    task_formatted = x['name'].lower().replace(' - ','_').replace(' ','_')
                    task_names_projects.append(task_formatted)
                    for i in task_names:        
                        if task_formatted == i:
                            task_dictionary[task_formatted+'_created'] = x['created_at']

                            # adding try catch due to variable data structure. NoSQL rules for APIs
                            try:
                                task_dictionary[task_formatted+'_completed'] = x['completed_at']
                            except:
                                task_dictionary[task_formatted+'_completed'] = None
                            try:
                                task_dictionary[task_formatted+'_completed_by'] = x['completed_by']
                            except:
                                task_dictionary[task_formatted+'_completed_by'] = None
                for x in task_names:
                    if x not in task_names_projects:
                        task_dictionary[x+'_created'] = None
                        task_dictionary[x+'_completed'] = None
                        task_dictionary[x+'_completed_by'] = None
            project.pop()
            project.append(task_dictionary)
        except Exception as e:
            print(e)


    full_task_list = []
    for x in task_names:
        full_task_list.append(x+'_created')
        full_task_list.append(x+'_completed')
        full_task_list.append(x+'_completed_by')
        
    # adding in task names to data_columns
    for x in full_task_list:
        data_columns.append(x)
        
    # creating rows for df

    df_rows = []
    for x in sunrise_data:
        try:
            row = x[0:len(x)-1]

            # use full_task_list to make sure ordering is correct
            for task in full_task_list:
                row.append(x[len(x)-1][task])
            df_rows.append(row)
        except:
            print(x)

    # old test for finding duplicates
    # set([x for x in data_columns if data_columns.count(x) > 1])
    # len(data_columns)
            
    task_df = pd.DataFrame(data=df_rows,columns=data_columns)
    task_df.drop_duplicates(subset='project_sunrise_id',inplace=True)
    for x in full_task_list:
        task_df[x] = pd.to_datetime(task_df[x],unit='ms',errors='coerce') - pd.Timedelta('05:00:00')
        
    additional_date_columns = ['closedate','appointment_date']
    for x in additional_date_columns:
        task_df[x] = pd.to_datetime(task_df[x],errors='coerce')


    # need to switch these and either get property made or self logic to design all_approve and all_submit date. Current iteration just finds max, not if all are completed
    task_df['all_submit_date'] = task_df[['submit_architectural_improvement_request_completed',
                'submit_permit_application_completed','submit_interconnection_application_completed','submit_utility_rebate_application_completed']].max(axis=1)
    task_df['all_approve_date'] = task_df[['upload_architectural_improvement_request_approval_completed','upload_permit_application_approval_completed',
                                        'upload_interconnection_application_approval_completed','upload_utility_rebate_application_approval_completed']].max(axis=1)
    task_df['installation_complete_raw'] = task_df['crew_installation_complete_completed']
    task_df['appointment_to_contract'] = ''
    task_df['contract_to_design'] = ''
    task_df['contract_to_install'] = ''
    task_df['handoff_date'] = task_df['all_approve_date']
    task_df['schedule_installation_raw'] = task_df['schedule_installation_completed']
    task_df['hours'] = ''

    # making a copy to keep Metrics sheet up and running. Eventually will convert everything to fsp_sql, but intermediate stages
    metrics_df = task_df.copy()

    # replacing all Nulls with None for postgres upload, otherwise will fail. Then converting to values
    task_df = task_df.replace({np.nan: None})
    task_df = task_df.replace({'': None})
    task_df.drop_duplicates(subset='project_sunrise_id',inplace=True)
    task_values_list = task_df.values.tolist()

    # getting all columns from df and putting into list. create dictionary in order to get locations of different columns. Makes adding new columns much easier to manage
    sql_columns = task_df.columns.tolist()
    columns_tuple = tuple(sql_columns)
    column_dictionary = {}
    for x in sql_columns:
        column_dictionary[x] = sql_columns.index(x)

    # creating list of row value dictionaries, for old way
    # row_values = []
    # row_length = len(sql_columns)
    # for row in task_values_list:
    #     row_dictionary = {}
    #     for i in range(0,row_length):
    #         row_dictionary[sql_columns[i]] = row[i]
    #     row_values.append(row_dictionary)

    upload_conn = psycopg2.connect(database=db_database, user=db_user, 
                        password = db_password,
                        host = db_host)


    upload_cursor = upload_conn.cursor()
    print("before execute")
    upload_cursor.execute("""DELETE FROM tasks_wide
                            WHERE project_sunrise_id IN (
                                SELECT project_sunrise_id
                                WHERE close_project_completed IS NULL);""")
    upload_conn.commit()
    conn.close()
    print("Deleted Successfully")

    import time
    time.sleep(10)

    # batch updater, can increase batch number by increasing pagination length but no issues for now

    # first grabbing iteration function
    # lifted iterator code from following article --> https://hakibenita.com/fast-load-data-python-postgresql

    from typing import Iterator, Optional
    import io

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

    print('Task DB Update Started')

    upload_conn = psycopg2.connect(database=db_database, user=db_user, 
                            password = db_password,
                            host = db_host)


    try:
        # modified function from above link, simplified to work with paginated list and structure instead of dictionaries
        def string_io_iterator(connection,table,columns,rows: Iterator[list]) -> None:
            with connection.cursor() as cursor:
                    beers_string_iterator = StringIteratorIO((
                        row + '\n'
                        for row in rows
                    ))
                    cursor.copy_from(beers_string_iterator,table,columns=columns,sep=";", null='missing_data')
                    connection.commit()

        # quick loop to enable pagination for the upload
        item_number = 0
        full_length = len(task_values_list)
        final_page = False
        paginate = True
        while paginate:
            
            pagination_length = 50
            end_number = item_number+pagination_length
            if end_number>=full_length:
                end_index = full_length
                final_page = True
            else:
                end_index = end_number
            # print(end_index)
                
            value_list = []
            for x in task_values_list[item_number:end_index]:
                string_row = []
                for i in x:
                    if i is None:
                        string_row.append('missing_data')
                    else:
                        string_row.append(str(i).replace(';',':')) #need to fix any instances of ; so it doesn't break the delimiter
                row = ";".join(string_row)
                value_list.append(str(row))
                
            string_io_iterator(upload_conn,'tasks_wide',columns_tuple,value_list)
            
            if final_page:
                paginate=False
            else:
                item_number+=pagination_length
    except:
        print('Task DB Upload Failed')


    # adding finished_df back to task df

    task_df = pd.concat([projects_df,task_df],ignore_index=True)


    metrics_columns = ['project_sunrise_id','dealname','appointment_date','signed_contract_completed','closedate','confirm_deposit_received_completed','complete_site_assessment_completed',
                    'upload_design_packet_construction_set_completed','submit_architectural_improvement_request_completed','submit_permit_application_completed',
                    'submit_interconnection_application_completed','submit_utility_rebate_application_completed','all_submit_date','upload_architectural_improvement_request_approval_completed',
                    'upload_permit_application_approval_completed','upload_interconnection_application_approval_completed','upload_utility_rebate_application_approval_completed','all_approve_date',
                    'schedule_installation_created','schedule_installation_completed','crew_installation_complete_completed','complete_city_inspection_completed',
                    'complete_utility_inspection_completed','close_project_completed','schedule_installation_raw','installation_complete_raw','appointment_to_contract','contract_to_design','contract_to_install',
                    'system_size','amount_in_home_currency','payment_option','module_type','module_quantity','tesla_powerwall_quantity','powerwall_only','hours','hold_type','utility',
                    'market_region','dealstage','closed_lost_date']

    metrics_date_columns = ['appointment_date','signed_contract_completed','closedate','confirm_deposit_received_completed','complete_site_assessment_completed',
                    'upload_design_packet_construction_set_completed','submit_architectural_improvement_request_completed','submit_permit_application_completed',
                    'submit_interconnection_application_completed','submit_utility_rebate_application_completed','all_submit_date','upload_architectural_improvement_request_approval_completed',
                    'upload_permit_application_approval_completed','upload_interconnection_application_approval_completed','upload_utility_rebate_application_approval_completed','all_approve_date',
                    'schedule_installation_created','schedule_installation_completed','crew_installation_complete_completed','complete_city_inspection_completed',
                    'complete_utility_inspection_completed','close_project_completed','schedule_installation_raw','installation_complete_raw','closed_lost_date']


    # making a copy to keep Metrics sheet up and running. Eventually will convert everything to fsp_sql, but intermediate stages
    metrics_df = task_df.copy()
    metrics_df.drop_duplicates(subset='project_sunrise_id',inplace=True)
    for x in metrics_date_columns:
        metrics_df[x] = pd.to_datetime(metrics_df[x].fillna(pd.NaT),errors='coerce')

    metrics_df = metrics_df[metrics_columns].sort_values(by='closedate').copy()

    def date_converter(x):
        if x == 'NaT' or x == 'NaN':
            return ''
        else:
            return x

    #for exporting
    for x in metrics_date_columns:
        metrics_df[x] = metrics_df[x].dt.strftime('%m/%d/%Y').apply(date_converter)
        
    metrics_df.fillna('',inplace=True)
    # correcting for some dtype issues in sheets upload
    for x in ['system_size','amount_in_home_currency','module_quantity']:
        metrics_df[x] = metrics_df[x].replace('',0.0)
        metrics_df[x] = metrics_df[x].fillna(0.0)
        metrics_df[x] = metrics_df[x].astype('float64')
    metrics_df.fillna('',inplace=True)



    sheet_id = os.environ['metrics_sheet_id']
    ws = client.open_by_key(sheet_id).worksheet('Project Data')
    client.open_by_key(sheet_id).values_clear("Project Data!A:AP")

    ws.update([metrics_df.columns.values.tolist()] + metrics_df.values.tolist(), value_input_option="USER_ENTERED")

    print('Metrics Deck has been updated.')


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
    update_task_database(deal_list,full_reset=False)
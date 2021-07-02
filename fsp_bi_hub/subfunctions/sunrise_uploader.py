import gspread
import numpy as np
import pandas as pd
import datetime
from time import sleep
import os
from sqlalchemy import create_engine, insert, update, delete, Table, Column, Integer, String, MetaData, Date, DateTime, Float, Boolean, select, BigInteger, TEXT
import requests
import json
import psycopg2
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from typing import Iterator, Optional
import io


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


host = os.environ['sunrise_elastic_host'] # For example, my-test-domain.us-east-1.es.amazonaws.com
region = os.environ['sunrise_elastic_region'] # e.g. us-west-1


service = 'es'
# credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(os.environ['sunrise_elastic_access_id'], os.environ['sunrise_elastic_access_key'], region, service)

es = Elasticsearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

query = {
    'query': {
        'bool': {
            'must': [
                {
                    'term': {
                        'organization_id':  os.environ['sunrise_elastic_organization_id']
                    }
                }
            ]
        }
    }
}

organization = es.search(index='organization',body=query)['hits']['hits'][0]

deal_mappings = organization['_source']['hubspot_mappings']['deals']

deal_dictionary = {}
for x in deal_mappings:
    deal_dictionary[x] = deal_mappings[x]['name']

key_mappings = {
    'hold_type': '296ae530-3719-4049-829c-4bf7692f6dc4',
}

project_list = []
query = {
    'size':1000,
    'query': {
        'match_all': {}
    }
}


projects = es.search(index="project",body = query, scroll = "3m")

for x in projects['hits']['hits']:
    project_list.append(x)
scroll_id = projects['_scroll_id']


result_length = len(projects['hits']['hits'])
while result_length>0:
    results = es.scroll(scroll='3m',scroll_id = scroll_id)
    result_length = len(results['hits']['hits'])
    for x in results['hits']['hits']:
        project_list.append(x)
    scroll_id = results['_scroll_id']

print(f"There are {len(project_list)} total projects in Project Sunrise.")

critical_path = es.search(index="critical_path",body = query)['hits']['hits']

critical_path_dict = {}
for path_stage, info_list in enumerate(critical_path[1]['_source']['stages'],start=1):
    critical_path_dict[info_list['id']] = {'name': info_list['name'],
                                        'critical_path_priority': path_stage}


project_table = []
fail_count = 0
for x in project_list:
    sunrise_id = x['_id']
    created_at = x['_source']['created_at']
    hold_status = x['_source']['on_hold']
    try:
        path_stage = critical_path_dict[x['_source']['critical_path']['stage_id']]['name']
        path_prio = critical_path_dict[x['_source']['critical_path']['stage_id']]['critical_path_priority']
    except:
        path_stage = None
        path_prio = None
    try:
        for i in x['_source']['fields']:
            if i['id'] == key_mappings['hold_type']:
                hold_type = i['text']
    except:
        hold_type = None
        fail_count += 1
    project_table.append([sunrise_id,created_at,hold_status,hold_type,path_stage,path_prio,x['_source']])

print(f"{fail_count} projects did not have an on_hold mapping.")    

for x in project_table:
    if x[2] == True and x[3]!='Cancelled':
        active_hold = True
    else:
        active_hold = False
    x.append(active_hold)

def create_date_fixer(x):
    if x.hour == 0 and x.minute == 0 and x.second == 0:
        return x
    elif x >= datetime.datetime(2021,5,1):
        return x - datetime.timedelta(hours=5)
    else:
        return x


# add back full_array at some point or just parse out useful info and add
sunrise_df = pd.DataFrame(data=project_table,columns=['project_sunrise_id','closedate','hold_status','hold_type','critical_path_stage','critical_path_prio','full_array','active_hold'])
sunrise_df = sunrise_df[['project_sunrise_id','hold_status','hold_type','active_hold','critical_path_stage','critical_path_prio','closedate']]
sunrise_df = sunrise_df[sunrise_df.project_sunrise_id.isnull()==False]
sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].apply(lambda x: str(x).replace(',',''))
sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].astype('int64')
sunrise_df['project_sunrise_id'] = sunrise_df['project_sunrise_id'].apply(lambda x: str(x))
sunrise_df['closedate'] = pd.to_datetime(sunrise_df['closedate'],unit='ms',errors='coerce').apply(create_date_fixer)
sunrise_df['critical_path_prio'] = sunrise_df['critical_path_prio'].fillna('').apply(lambda x: str(x).replace(',','').replace('.0',''))
print(f"There are {len(sunrise_df[sunrise_df.hold_status.isnull()])} projects missing a null status.")
sunrise_df['hold_status'] = np.where(sunrise_df['hold_status'].isnull(),False,sunrise_df['hold_status'])

# creating engine and resetting table to upload data
engine = create_engine(os.environ['sql_alchemy_conn_string'], echo=False)
meta = MetaData()
with engine.connect() as conn:
    conn.execute("DROP TABLE IF EXISTS sunrise_projects;")

sunrise_projects = Table(
    'sunrise_projects', meta,
    Column('project_sunrise_id',BigInteger),
    Column('hold_status',String),
    Column('hold_type',String),
    Column('active_hold', String),
    Column('critical_path_stage',String),
    Column('critical_path_prio',String),
    Column('closedate',DateTime),
    # Column('full_array',TEXT),
)

meta.create_all(engine)
item_uploader('sunrise_projects',sunrise_df)

print('Sunrise Projects Uploaded')
def generate_metrics_f2p(base_route):
    import numpy as np
    import pandas as pd
    import time
    import datetime
    from dateutil.relativedelta import relativedelta

    import pickle
    import os.path, sys, os
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    import csv

    import warnings
    warnings.filterwarnings('ignore')

    variables_path_base = base_route+'\\env_variables\\'
    data_path_base = base_route+'\\data_dumps\\'
    sheets_token = variables_path_base + 'token.pickle'
    sheets_id_dictionary_pickle = variables_path_base + 'sheets_id_dictionary.pickle'

    with open(sheets_id_dictionary_pickle, 'rb') as id:
        sheets_id_dictionary = pickle.load(id)

    def grab_sheet(sheet_id,grab_range):

        if os.path.exists(sheets_token):
                with open(sheets_token, 'rb') as token:
                    creds = pickle.load(token)
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                        range=grab_range).execute()
        result_list = result.get('values', [])
        return result_list


    # need to switch this section over to fsp_sql, but for now it's fine using the exports we have up
    data = pd.read_csv(data_path_base + 'metrics.csv').values.tolist()
    sold_data = pd.read_csv(data_path_base + 'projects.csv').values.tolist()
    index_data = pd.read_csv(data_path_base + 'index.csv').values.tolist()

    df = pd.DataFrame(data=data[1::],columns=data[0])

    df = df[['ID','Customer Name','Sale Date','Installation Complete Date','System Size','System Price','Market']]
    df.drop(df.index[df['Installation Complete Date']=='Cancelled'], inplace=True) # getting rid of cancels
    df.drop(df.index[df['Installation Complete Date']=='New Construction'], inplace=True) # getting rid of new constructions
    df.rename(columns={'Installation Complete Date': 'install_date','System Size': 'system_size','System Price':'sale_price',
                    'Market': 'market_region', 'Sale Date':'sale_date_metrics',
                    'Customer Name':'customer_name','ID':'id'}, inplace=True)

    df.drop_duplicates(subset=['id'], inplace=True)

    sold_df = pd.DataFrame(data=sold_data[1::],columns=sold_data[0])[['ID','Corrected Sale Date']]
    index_df = pd.DataFrame(data=index_data[1::],columns=index_data[0])[['Lead ID','System Size','System Price','Market']]

    df = df.merge(sold_df,how='left',left_on='id',right_on='ID')[['id','Corrected Sale Date','install_date',
                                                                'market_region']]
    df = df.merge(index_df,how='left',left_on='id',right_on='Lead ID')[['id','Corrected Sale Date','install_date',
                                                                'System Size', 'System Price', 'Market']]

    df.rename(columns={'Corrected Sale Date': 'sale_date', 'id': 'object_id','System Size': 'system_size',
                    'System Price': 'sale_price', 'Market': 'market_region'},inplace=True)
    df.sale_price = [str(x).strip("$").replace(",","") for x in df.sale_price]
    df.system_size = [str(x).strip("kW") for x in df.system_size]
    df[['sale_price','system_size']].fillna(0,inplace=True)

    for x in df.index[df['sale_price']==""]:
        df.at[x,'sale_price'] = 0

    for x in df.index[df['system_size']==""]:
        df.at[x,'system_size'] = 0

    df.sale_price = df.sale_price.astype('float64')
    df.system_size = df.system_size.astype('float64')
    df.drop(df.index[df['sale_price']==0],inplace=True)
    df['amount'] = 1
    df['object_type'] = 'Install'
    df.drop_duplicates(subset=['object_id'], inplace=True)

    df['install_date'] = pd.to_datetime(df['install_date'],errors='coerce',infer_datetime_format=True)
    df['sale_date'] = pd.to_datetime(df['sale_date'],errors='coerce',infer_datetime_format=True)

    date_ranges = []
    for x in range(2019,2022):
        for i in range(1,13):
            if datetime.datetime(x,i,1)<datetime.datetime.today():
                date_ranges.append(datetime.datetime(x,i,1))


    backlogs = []
    for start_month in date_ranges:
        for market in list(df.market_region.unique()):
            next_month = start_month+relativedelta(months=1)
            backlog_count = df[((df['sale_date']<next_month) & ((df['install_date']>=next_month) | (df['install_date'].isnull()))
                            & (df['market_region']==market))]['object_id'].count()
            system_size = df[((df['sale_date']<next_month) & ((df['install_date']>=next_month) | (df['install_date'].isnull()))
                            & (df['market_region']==market))]['system_size'].sum()
            sale_price = df[((df['sale_date']<next_month) & ((df['install_date']>=next_month) | (df['install_date'].isnull()))
                            & (df['market_region']==market))]['sale_price'].sum()
            backlogs.append(['testing',start_month,start_month,system_size,sale_price,market,backlog_count,'Backlog'])

    backlog_df = pd.DataFrame(data=backlogs, columns=list(df.columns))
    df = df.append(backlog_df, ignore_index=True)

    if False: #change to true to test backlog totals against f2p calcs
        for x in date_ranges:
            print([x, df[(df['install_date']==x) &(df['object_type']=='Backlog')]['amount'].sum()])

    df['data_type'] = 'Actual'
    df['lead_source'] = 'N/A'
    df = df[['object_id','install_date','market_region','lead_source','object_type','data_type',
            'sale_price','system_size','amount']]
    df.rename(columns={'install_date':'date'}, inplace=True)
    df.drop(df.index[(df['date']>=datetime.datetime.today()) & (df['object_type']=='Install')],inplace=True)

    def generate_indicative(object_type, df):
        
        from datetime import timedelta
        import calendar
        
        today = datetime.datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
        today_day = today.day
        today_month = today.month
        today_year = today.year

        current_month_days = calendar.monthrange(today_year,today_month)[1]
        current_month_days_remaining = current_month_days - today_day + 1 #add back a day since today is not included
        two_weeks_ago = today - timedelta(days=14)
        
        current_month_indicative_dates = []
        for i in range(today_day,current_month_days+1):
            date = str(today_year) + '-' + str(today_month) + '-' + str(i)
            current_month_indicative_dates.append(date)
        
        
        regions = list(df['market_region'].unique())
        lead_sources = list(df['lead_source'].unique())
        
        average_list = []
        for x in regions:
            for i in lead_sources:
                count = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['date'].count()
                sum_price = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['sale_price'].sum()
                sum_size = df[(df['market_region']==x) & (df['lead_source']==i) & (df['date']>=two_weeks_ago) & (df['date']<today) & (df['object_type']==object_type)]['system_size'].sum()
                average_count = round(count/14,2)
                average_price = round(sum_price/14,2)
                average_size = round(sum_size/14,2)
                entry = [x,i,average_price,average_size,average_count]
                average_list.append(entry)
            
        id_increment = 1
        current_month_indicative_forecast = []
        for x in current_month_indicative_dates:
            for i in average_list:
                market = i[0]
                lead_source = i[1]
                sum_price = i[2]
                sum_size = i[3]
                average_count = i[4]
                id = str(id_increment)+'-L'
                entry = [id,x,market,lead_source,object_type,'Indicative',sum_price,sum_size,average_count]
                current_month_indicative_forecast.append(entry)
                id_increment += 1
            
        current_month_indicative_forecast.insert(0,list(df.columns.values))
        indicative_df = pd.DataFrame(data=current_month_indicative_forecast[1::],columns=current_month_indicative_forecast[0])
        return indicative_df

    indicative_install_df = generate_indicative("Install", df)
    df = df.append(indicative_install_df, ignore_index=True)
    change_list = [('30260','2020-9-25'),('30631','2020-10-12'),('33853','2020-12-29')]
    for x in change_list:
        index = df.index[df.object_id == x[0]][0]
        df.at[index,'date'] = x[1]
    df.to_csv(data_path_base + 'installs.csv',index=False)
    return df


if __name__=='__main__':
    base_route = "C:\\Users\\myate\\Desktop\\Python\\fsp_general"
    transform_data(base_route)
""" Local script using csv exports to transform various datasets into usable form for management metrics within Tableau. Focused on aggregating sales/marketing side
by lead source and overall by region/time. Incorporates forecasting to the end of month by using two week averages for a few metrics"""


def transform_data(base_route):
    import numpy as np
    import pandas as pd
    import os, sys
    import pickle
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request

    from datetime import datetime
    from datetime import timedelta
    if __name__=="__main__":
        from metrics_transformation import generate_metrics_f2p
    else:
        from tableau_transformation.metrics_transformation import generate_metrics_f2p

    import calendar

    installs_df = generate_metrics_f2p(base_route)
    variables_path_base = base_route+'\\env_variables\\'
    data_path_base = base_route+'\\data_dumps\\'
    sheets_token = variables_path_base + 'token.pickle'
    sheets_id_dictionary_pickle = variables_path_base + 'sheets_id_dictionary.pickle'

    with open(sheets_id_dictionary_pickle, 'rb') as id:
        sheets_id_dictionary = pickle.load(id)


    def google_pull(sheet_id,target_range):
        if os.path.exists(sheets_token):
            with open(sheets_token, 'rb') as token:
                creds = pickle.load(token)
        service = build('sheets', 'v4', credentials=creds)

        # Call the Sheets API
        sheet = service.spreadsheets()
        a = sheet.values().get(spreadsheetId=sheet_id,
                                            range=target_range).execute()
        results_google = a.get('values',[])
        return results_google

    def summary_build():
        
        contacts_df = pd.read_csv(data_path_base + 'contacts_export.csv')
        contacts_df['count'] = 'count'
        contacts_df = contacts_df[['five9_create_date','market_region','n3pl_lead_source','count']]
        contacts_df.rename(columns={'n3pl_lead_source': 'lead_source','five9_create_date': 'date'},inplace=True)
        contacts_df = contacts_df.groupby(['lead_source','market_region','date'],as_index=False).count()
        contacts_df['object_type'] = 'Contact'
        contacts_df['data_type'] = 'Actual'
        contacts_df.head()
        
        appointments_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        appointments_df['count'] = 'count'
        appointments_df = appointments_df[['appointment_date','market_region','top_deal_sources','count']]
        appointments_df.rename(columns={'appointment_date': 'date','top_deal_sources': 'lead_source'},inplace=True)
        appointments_df = appointments_df.groupby(['lead_source','market_region','date'],as_index=False).count()
        appointments_df['object_type'] = 'Appointment'
        appointments_df['data_type'] = 'Actual'
        
        merged_df = pd.concat([contacts_df,appointments_df],axis=0)
        merged_df.to_csv(data_path_base + 'tableau_test_summary.csv')
        return merged_df

    def general_build():
        
        contacts_df = pd.read_csv(data_path_base + 'contacts_export.csv')
        contacts_df = contacts_df[['hs_object_id','five9_create_date','market_region','n3pl_lead_source']]
        contacts_df.rename(columns={'hs_object_id':'object_id','n3pl_lead_source': 'lead_source','five9_create_date': 'date'},inplace=True)
        contacts_df['object_type'] = 'Contact'
        contacts_df['data_type'] = 'Actual'
        
        appointments_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        appointments_df = appointments_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source']]
        appointments_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        appointments_df['object_type'] = 'Appointment'
        appointments_df['data_type'] = 'Actual'
        
        ran_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        ran_df = ran_df[ran_df['run_status']=='Run']
        ran_df = ran_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source']]
        ran_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        ran_df['object_type'] = 'Ran Appt'
        ran_df['data_type'] = 'Actual'    
        
        sold_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        # sold_df = sold_df[sold_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        sold_df = sold_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size']]
        sold_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        sold_df['object_type'] = 'Contracts'
        sold_df['data_type'] = 'Actual'    

        aged_cancels_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        aged_cancels_df = aged_cancels_df[aged_cancels_df['dealstage']=='Closed Lost'] #creating aged cancels
        aged_cancels_df = aged_cancels_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size']]
        aged_cancels_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        aged_cancels_df['object_type'] = 'Aged Cancels'
        aged_cancels_df['data_type'] = 'Actual'    

        current_cancels_df = pd.read_csv(data_path_base + 'cancelled_deals_export.csv')
        # current_cancels_df = current_cancels_df[current_cancels_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        current_cancels_df = current_cancels_df[['hs_object_id','closed_lost_date','market_region','cac_category_lead_source','amount_in_home_currency','system_size']]
        current_cancels_df.rename(columns={'hs_object_id': 'object_id','closed_lost_date': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        current_cancels_df['object_type'] = 'Current Cancels'
        current_cancels_df['data_type'] = 'Actual'    

        
        merged_df = pd.concat([contacts_df,appointments_df,ran_df,sold_df,current_cancels_df, aged_cancels_df],axis=0)
        merged_df['sale_price'].fillna(0,inplace=True)
        merged_df['system_size'].fillna(0,inplace=True)
        merged_df.fillna('',inplace=True)
        merged_df.to_csv(data_path_base + 'f2p_test.csv')
        merged_df['amount'] = 1
        
        return merged_df


    def canvassing_build():

        sales_teams_raw = pd.read_csv(data_path_base + 'sales_teams.csv').values.tolist()
        sales_map = {}
        for x in sales_teams_raw:
            sales_map[x[0]] = x[1]
        
        contacts_df = pd.read_csv(data_path_base + 'contacts_export.csv')
        contacts_df = contacts_df[['hs_object_id','five9_create_date','market_region','n3pl_lead_source','agent_name','canvasser_name']]
        contacts_df.rename(columns={'hs_object_id':'object_id','n3pl_lead_source': 'lead_source','five9_create_date': 'date'},inplace=True)
        contacts_df['object_type'] = 'Contact'
        contacts_df['data_type'] = 'Actual'
        
        appointments_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        appointments_df = appointments_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source','agent_name','canvasser_name']]
        appointments_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        appointments_df['object_type'] = 'Appointment'
        appointments_df['data_type'] = 'Actual'
        
        ran_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        ran_df = ran_df[ran_df['run_status']=='Run']
        ran_df = ran_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source','agent_name','canvasser_name']]
        ran_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        ran_df['object_type'] = 'Ran Appt'
        ran_df['data_type'] = 'Actual'    
        
        sold_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        # sold_df = sold_df[sold_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        sold_df = sold_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        sold_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        sold_df['object_type'] = 'Contracts'
        sold_df['data_type'] = 'Actual'    

        aged_cancels_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        aged_cancels_df = aged_cancels_df[aged_cancels_df['dealstage']=='Closed Lost'] #creating aged cancels
        aged_cancels_df = aged_cancels_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        aged_cancels_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        aged_cancels_df['object_type'] = 'Aged Cancels'
        aged_cancels_df['data_type'] = 'Actual'    

        current_cancels_df = pd.read_csv(data_path_base + 'cancelled_deals_export.csv')
        # current_cancels_df = current_cancels_df[current_cancels_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        current_cancels_df = current_cancels_df[['hs_object_id','closed_lost_date','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        current_cancels_df.rename(columns={'hs_object_id': 'object_id','closed_lost_date': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        current_cancels_df['object_type'] = 'Current Cancels'
        current_cancels_df['data_type'] = 'Actual'    

        
        merged_df = pd.concat([contacts_df,appointments_df,ran_df,sold_df,current_cancels_df, aged_cancels_df],axis=0)
        merged_df['sale_price'].fillna(0,inplace=True)
        merged_df['system_size'].fillna(0,inplace=True)
        merged_df.fillna('',inplace=True)
        merged_df.to_csv(data_path_base + 'f2p_test.csv')
        merged_df['amount'] = 1
        
        return merged_df

    def play_build():

        sales_teams_raw = pd.read_csv(data_path_base + 'sales_teams.csv').values.tolist()
        sales_map = {}
        for x in sales_teams_raw:
            sales_map[x[0]] = x[1]
        
        contacts_df = pd.read_csv(data_path_base + 'contacts_export.csv')
        contacts_df = contacts_df[['hs_object_id','five9_create_date','market_region','n3pl_lead_source','agent_name','canvasser_name']]
        contacts_df.rename(columns={'hs_object_id':'object_id','n3pl_lead_source': 'lead_source','five9_create_date': 'date'},inplace=True)
        contacts_df['object_type'] = 'Contact'
        contacts_df['data_type'] = 'Actual'
        
        appointments_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        appointments_df = appointments_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source','agent_name','canvasser_name']]
        appointments_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        appointments_df['object_type'] = 'Appointment'
        appointments_df['data_type'] = 'Actual'
        
        ran_df = pd.read_csv(data_path_base + 'appointments_export.csv')
        ran_df = ran_df[ran_df['run_status']=='Run']
        ran_df = ran_df[['hs_object_id','appointment_date','market_region','cac_category_lead_source','agent_name','canvasser_name']]
        ran_df.rename(columns={'hs_object_id': 'object_id','appointment_date': 'date','cac_category_lead_source': 'lead_source'},inplace=True)
        
        ran_df['object_type'] = 'Ran Appt'
        ran_df['data_type'] = 'Actual'    
        
        sold_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        # sold_df = sold_df[sold_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        sold_df = sold_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        sold_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        sold_df['object_type'] = 'Contracts'
        sold_df['data_type'] = 'Actual'    

        aged_cancels_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        aged_cancels_df = aged_cancels_df[aged_cancels_df['dealstage']=='Closed Lost'] #creating aged cancels
        aged_cancels_df = aged_cancels_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        aged_cancels_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        aged_cancels_df['object_type'] = 'Aged Cancels'
        aged_cancels_df['data_type'] = 'Actual'    

        current_cancels_df = pd.read_csv(data_path_base + 'cancelled_deals_export.csv')
        # current_cancels_df = current_cancels_df[current_cancels_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        current_cancels_df = current_cancels_df[['hs_object_id','closed_lost_date','market_region','cac_category_lead_source','amount_in_home_currency','system_size','agent_name','canvasser_name']]
        current_cancels_df.rename(columns={'hs_object_id': 'object_id','closed_lost_date': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        
        current_cancels_df['object_type'] = 'Current Cancels'
        current_cancels_df['data_type'] = 'Actual'    

        
        merged_df = pd.concat([contacts_df,appointments_df,ran_df,sold_df,current_cancels_df, aged_cancels_df],axis=0)
        merged_df['sale_price'].fillna(0,inplace=True)
        merged_df['system_size'].fillna(0,inplace=True)
        merged_df.fillna('',inplace=True)
        merged_df.to_csv(data_path_base + 'f2p_test.csv')
        merged_df['amount'] = 1
        
        return merged_df

    def cancels_build():

        sales_teams_raw = pd.read_csv(data_path_base + 'sales_teams.csv').values.tolist()
        sales_map = {}
        for x in sales_teams_raw:
            sales_map[x[0]] = x[1]
        
        sold_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        # sold_df = sold_df[sold_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        sold_df = sold_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','original_deal_owner']]
        sold_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        sold_df['ec_region'] = sold_df['original_deal_owner'].map(sales_map)
        
        sold_df['object_type'] = 'Contracts'
        sold_df['data_type'] = 'Actual'    

        aged_cancels_df = pd.read_csv(data_path_base + 'closed_deals_export.csv')
        aged_cancels_df = aged_cancels_df[aged_cancels_df['dealstage']=='Closed Lost'] #creating aged cancels
        aged_cancels_df = aged_cancels_df[['hs_object_id','closedate','market_region','cac_category_lead_source','amount_in_home_currency','system_size','original_deal_owner']]
        aged_cancels_df.rename(columns={'hs_object_id': 'object_id','closedate': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        aged_cancels_df['ec_region'] = aged_cancels_df['original_deal_owner'].map(sales_map)
        
        aged_cancels_df['object_type'] = 'Aged Cancels'
        aged_cancels_df['data_type'] = 'Actual'    

        current_cancels_df = pd.read_csv(data_path_base + 'cancelled_deals_export.csv')
        # current_cancels_df = current_cancels_df[current_cancels_df['dealstage']=='Closed Won'] #commenting out since we need gross and net
        current_cancels_df = current_cancels_df[['hs_object_id','closed_lost_date','market_region','cac_category_lead_source','amount_in_home_currency','system_size','original_deal_owner']]
        current_cancels_df.rename(columns={'hs_object_id': 'object_id','closed_lost_date': 'date','cac_category_lead_source': 'lead_source','amount_in_home_currency': 'sale_price'},inplace=True)
        current_cancels_df['ec_region'] = current_cancels_df['original_deal_owner'].map(sales_map)

        current_cancels_df['object_type'] = 'Current Cancels'
        current_cancels_df['data_type'] = 'Actual'    

        
        merged_df = pd.concat([sold_df,current_cancels_df, aged_cancels_df],axis=0)
        merged_df['sale_price'].fillna(0,inplace=True)
        merged_df['system_size'].fillna(0,inplace=True)
        merged_df.fillna('',inplace=True)
        merged_df.to_csv(data_path_base + 'f2p_test.csv')
        merged_df['amount'] = 1
        merged_df = pd.concat([merged_df,installs_df],axis=0)
        merged_df['amount'] = merged_df['amount'].astype('float64')
        
        return merged_df

    f2p = general_build()
    values_list = f2p.values.tolist()
    f2p['date'] = pd.to_datetime(f2p['date'],errors='coerce')
    f2p['sale_price'] = f2p['sale_price'].astype('float64')
    f2p['system_size'] = f2p['system_size'].astype('float64')

    cancels = cancels_build()
    values_list = cancels.values.tolist()
    cancels['date'] = pd.to_datetime(cancels['date'],errors='coerce')
    cancels['sale_price'] = cancels['sale_price'].astype('float64')
    cancels['system_size'] = cancels['system_size'].astype('float64')
    cancels.to_csv(data_path_base + 'cancels_tableau.csv', index=False)

    today = datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
    today_day = today.day
    today_month = today.month
    today_year = today.year

    current_month_days = calendar.monthrange(today_year,today_month)[1]
    current_month_days_remaining = current_month_days - today_day + 1 #add back a day since today is not included
    two_weeks_ago = today - timedelta(days=14)

    # Setting up regions and lead sources
    regions = list(f2p['market_region'].unique())
    lead_sources = list(f2p['lead_source'].unique())

    average_list = []
    for x in regions:
        for i in lead_sources:
            count = f2p[(f2p['market_region']==x) & (f2p['lead_source']==i) & (f2p['date']>=two_weeks_ago) & (f2p['date']<today) & (f2p['object_type']=='Contact')]['date'].count()
            average = round(count/14,2)
            entry = [x,i,average]
            average_list.append(entry)

    # Generating entries for indicative forecast
    def generate_indicative(object_type, df):
        
        from datetime import timedelta
        import calendar
        
        today = datetime.today().replace(minute=0,hour=0,second=0,microsecond=0)
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

    indicative_contact_df = generate_indicative("Contact", f2p)
    indicative_appointment_df = generate_indicative("Appointment", f2p)
    indicative_ran_df = generate_indicative("Ran Appt", f2p)
    indicative_sold_df = generate_indicative("Contracts", f2p)
    indicative_current_cancel_df = generate_indicative("Current Cancels", f2p)
    indicative_aged_cancel_df = generate_indicative("Aged Cancels", f2p)




    f2p = pd.concat([f2p,indicative_contact_df,indicative_appointment_df,indicative_ran_df,indicative_sold_df,indicative_current_cancel_df,
        indicative_aged_cancel_df,installs_df],axis=0).fillna('')
    f2p['date'] = pd.to_datetime(f2p['date'])
    f2p.fillna('',inplace=True)
    f2p.to_excel(data_path_base + 'f2p_test.xlsx', index=False)

    print('Transformation Complete')


if __name__=="__main__":
    base_route = "C:\\Users\\myate\\Desktop\\Python\\fsp_general"
    transform_data(base_route)
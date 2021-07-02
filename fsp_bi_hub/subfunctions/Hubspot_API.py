from __future__ import print_function

import requests
import json
import time
import sys
import pickle

class HubspotApi():
    def __init__(self, api_key):
        self.api_key = api_key
        self.root = "https://api.hubapi.com/crm/v3/objects"
        self.all_contacts = []
        self.contact_properties = []
        self.deal_properties = []
        self.all_deals = []
        self.parsed = []
        self.parsed_contacts = []
        self.parsed_deals = []

    def get_owners(self):
        owner_url = "https://api.hubapi.com/owners/v2/owners?hapikey=" + self.api_key
        r = requests.get(url = owner_url)
        return r.text

    def get_all_contacts(self, property_array):

        # property array must be turned into simple list without brackets
        properties = ','.join(property_array)
        headers = {'accept': 'application/json'}
        offset = 0
        count = 100
        error_count = 0
        full_results = []

        get_url = self.root+'/contacts'


        while int(offset)>=0:

            query_string = {
                "limit":count,
                "after":offset,
                "hapikey":self.api_key,
                "properties": properties,
                "paginateAssociations":"false",
                "archived":"false"
                }

            try:
                r = requests.get(url=get_url, headers = headers, params=query_string)
                response_dict = json.loads(r.text)
                results = response_dict['results']
                full_results.append(results)           

                # Grabs starting point for next pagination, and if there are no more contacts, then sets next_offset to False to break loop
                try: 
                    offset = response_dict['paging']["next"]["after"]
                    time.sleep(0.3)
                except: 
                    offset = -1
                    time.sleep(0.3)

            except:
                error_count += 1
                print(r)
                print(offset)
                time.sleep(5)

                # Preventing loop of unending requests in case of error and terminates program to not skip any contacts
                if error_count>9:
                    print('Too many errors encountered, please check the error messages and code. Exiting Script.')
                    sys.exit('Response Errors Limit.')


        # Setting self variables to allow further methods for transformation and returning results in case someone wants to parse themselves 
        list_results = []     
        for x in full_results:
            for i in x:
                list_results.append(i)

        self.contact_properties = property_array
        self.all_contacts = list_results
        return list_results


    def parse_contacts(self):
        parsed = []
        parsed.append(self.contact_properties)
        for x in self.all_contacts:
            line_item = []
            for i in self.contact_properties:
                try:
                    line_item.append(x['properties'][i])
                except:
                    line_item.append('')
            parsed.append(line_item)

        self.parsed_contacts = parsed
        return parsed

    def get_all_deals(self, property_array):

        # property array must be turned into simple list without brackets
        properties = ','.join(property_array)
        headers = {'accept': 'application/json'}
        offset = 0
        count = 100
        error_count = 0
        full_results = []

        get_url = self.root+'/deals'


        while int(offset)>=0:

            query_string = {
                "limit":count,
                "after":offset,
                "hapikey":self.api_key,
                "properties": properties,
                "paginateAssociations":"false",
                "archived":"false",
                "associations": 'contacts'
                }

            try:
                r = requests.get(url=get_url, headers = headers, params=query_string)
                response_dict = json.loads(r.text)
                results = response_dict['results']
                full_results.append(results)           

                # Grabs starting point for next pagination, and if there are no more contacts, then sets next_offset to False to break loop
                try: 
                    offset = response_dict['paging']["next"]["after"]
                    time.sleep(0.3)
                except: 
                    offset = -1
                    time.sleep(0.3)

            except:
                error_count += 1
                print(r)
                print(offset)
                time.sleep(5)

                # Preventing loop of unending requests in case of error and terminates program to not skip any contacts
                if error_count>9:
                    print('Too many errors encountered, please check the error messages and code. Exiting Script.')
                    sys.exit('Response Errors Limit.')


        # Setting self variables to allow further methods for transformation and returning results in case someone wants to parse themselves 
        list_results = []     
        for x in full_results:
            for i in x:
                list_results.append(i)

        del full_results

        self.deal_properties = property_array
        self.all_deals = list_results
        return list_results

    def parse_deals(self):
        parsed = []
        parsed.append(self.deal_properties)
        for x in self.all_deals:
            line_item = []
            try:        
                line_item.append(x['associations']['contacts']['results'][0]['id'])
            except:
                line_item.append('')
            for i in self.deal_properties:
                try:
                    line_item.append(x['properties'][i])
                except:
                    line_item.append('')
            parsed.append(line_item)
        parsed[0].insert(0,'associated_contact')
        self.parsed_deals = parsed
        return parsed        

if __name__ == "__main__":
    with open('hapikey.txt','r') as foo:
        api_key = foo.read()
    base_properties = ['hs_object_id','dealname','amount_in_home_currency','dealstage','agent_name',
                            'appointment_date','top_deal_sources','appointment_type','deal_lead_source',
                            'deal_marketing_lead_category','hubspot_owner_id','dealtype',
                            'market_region','run_status','appointment_ran_by','appointment_disposition','swf_lead_id',
                            'closed_lost_notes','call_center_return_notes','closed_lost_reason','createdate',
                            'has_been_rescheduled','closed_by_proposal_tool']
    test = HubspotApi(api_key)
    results = test.get_all_deals(base_properties)
    with open('deals_full.data','wb') as file_name:
        pickle.dump(results,file_name)

    parsed = test.parse_deals()
    print(len(results))
    print(len(parsed))
    print(results[10:12])
    



    
    
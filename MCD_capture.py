'''
Gather up to 1000 McDonald’s in NY state by zip code and Lat/Lon.  
Use www.zipcodebase.com & PGEO & Nimble.
Collect all relevant info such as:
Zip Code
Street Address 
Lat / Lon
Finally, save the output to a CSV file.
'''
import pgeocode
import requests
import csv
import time
import math

## gather PGEO data for the US
nomi = pgeocode.Nominatim('US')

##Build call to zipcodebase.com
#ZCB_headers = {
#    "apikey": "0e841810-243e-11ef-83b6-05f92a38b75d"
#}
#params = (
#   ("state_name", "New York"),
#   ("country", "US"),
#);
#zip_codes = requests.get('https://app.zipcodebase.com/api/v1/code/state', headers=ZCB_headers, params=params);
#this one broke after overuse. I think they want my CC.
        
"""sanity checking function for missing keys"""
def sanity(dictio : dict) -> bool:
    answer = True
    if not 'parsing' in dictio.keys() or \
        not 'entities' in dictio['parsing'].keys() or \
        not 'SearchResult' in dictio['parsing']['entities'].keys() : 
            answer = False
    return answer

"""check for all NaNs ie: 18373"""
def ruNaN(nanny : object) -> bool:
    answer = True
    if math.isnan(nanny['latitude']) or math.isnan(nanny['longitude']) :
        answer = False
    return answer

## read in zip codes from file
with open('/Users/douglas/anaconda3/NY_zipcodes1','r') as file:
    temp = file.read()
    file.close()
temp=temp.strip()
zip_codes=temp.split(',')

## build call to Nimble
nimble_headers = {
    'Authorization': 'Basic YWNjb3VudC1uaW1ibGUtcGlwZWxpbmUtbmltYmxlZGF0YTo1NnU4MHRMNzZVb0w=',
    'Content-Type': 'application/json',
}

nimble_data_raw = {
    'parse': True,
    'search_engine': 'google_maps_search',
    'query': 'mcdonalds',
    'domain': 'com',
    'coordinates': {
        'latitude': 'NaN',
        'longitude': 'NaN',
    },
    'format': 'json',
    'render': True,
    'country': 'US',
    'locale': 'en',
}

keys = [
    'address',
    'phone_number',
    'zip_code',
    'latitude',
    'longitude',
    'nimble_place_link'
]

# set structures
mydict = dict.fromkeys(keys, 0)
##mylist = []
uniqueness = []
cc = 0

# move csv output writer higher up, since program is randomly failing
filename = "MCDinNY.csv"
output_file = open(filename, "a")
dict_writer = csv.DictWriter(output_file, keys)
dict_writer.writeheader()

## loop over zip codes and extract all LATLONs calling Nimble
for code in zip_codes:
    cc += 1
    locale = nomi.query_postal_code(code)
    if (not ruNaN(locale)) :
##        print("Found all NaN return from PGEO, skipping.")
        continue

    nimble_data_raw['coordinates']['latitude'] = locale['latitude']
    nimble_data_raw['coordinates']['longitude'] = locale['longitude']

    nimble_serp = requests.post('https://api.webit.live/api/v1/realtime/serp', headers=nimble_headers, json=nimble_data_raw)
##    time.sleep(2.0)

    if nimble_serp.status_code != 200 :
        print("non-200 API response", nimble_serp.status_code,", skipping.")
        continue
    
    jjresp = nimble_serp.json()
    print("Working on: ",code," ",cc,"/",len(zip_codes),"zip codes.")
    if not sanity(jjresp) :  ##trying a sanity function to smooth things over
##        print("Missing Key Values, skipping this zip code:",code)
        continue
    
## loop over Nimble results and aggregate data in a list of dict's
    for result in jjresp['parsing']['entities']['SearchResult']:
        if "NY" not in result['address'] :  #SERP returns wide geography -- need to limit to NY
##            print("Non-NY")
            continue
        uniqueness.append(result['nimble_place_link']) #first one must be unique
        
        ## to check for unique values/locations, use set() and len()
        flag = 0
        flag = len(set(uniqueness)) == len(uniqueness)
        if not flag :
            uniqueness.pop()
            continue
            ## We have a unique entry, so save it
        temp_dict = dict.fromkeys(keys, 0)
        # adding some checks here, seeing random failures...
        if not 'zip_code' in result.keys() :
            print ("We dont have a zip code? Weird.")
            continue
        if 'address' in result.keys() :
            temp_dict['address']=result['address']
        if 'phone_number' in result.keys() :
            temp_dict['phone_number']=result['phone_number']
        temp_dict['zip_code']=result['zip_code']
        temp_dict['latitude']=result['latitude']
        temp_dict['longitude']=result['longitude']
        temp_dict['nimble_place_link']=result['nimble_place_link']
##        mylist.append(temp_dict) #append to my list
        dict_writer.writerow(temp_dict)
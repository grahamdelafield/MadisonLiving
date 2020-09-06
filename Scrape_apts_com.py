from bs4 import BeautifulSoup
from dataclasses import dataclass
from time import sleep
from datetime import date
import pandas as pd 
import altair as alt
import os
import re
import requests
from selenium.webdriver import Chrome
from geopy.geocoders import Nominatim

def parse_price(element, expected_length, is_range=True):
    '''
    Function to parse and return [low, high] value of
    rent string.

    args:   price_str (str) webscraped rent price
            is_range  (bool) flag to indicate if ' - '
                             needs to be removed
    '''
    assert isinstance(expected_length, int), 'expected_length must be type(int)'
    assert isinstance(element, list), 'Rent must enter function as type(list)'

    highs, lows = [], []
    pat1 = re.compile(r'[\$\,]')
    for r in element:
        try:
            price_str = r.text
            s = re.sub(pat1, '', price_str)
            if is_range:
                pat2 = re.compile(r'\s\-\s')
                s = re.split(pat2, s)
                low, high= s[0], s[1]
                highs.append(high)
                lows.append(low)
                continue
            highs.append(high)
            lows.append(low)
        
        except:
            highs.append(None)
            lows.append(None)
    
    lows = fill_missing(lows, expected_length)
    highs = fill_missing(highs, expected_length)
    
    return lows, highs


def get_page_range(element_text):
    '''
    Function to return [first_page, last_page] of webdriver element.

    args:   element_text (str) webdriver element.text
    '''
    assert isinstance(element_text, str), '''Function can only parse type(str)'''

    pat = re.compile(r'[0-9]+')
    matches = re.finditer(pat, element_text)
    if matches:
        return [int(m.group()) for m in matches]

def get_amenities(amen_element, expected_length):
    if amen_element == []:
        return [None] * expected_length

    all_amens = []
    for a in amen_element:
        try:
            items = a.find_elements_by_tag_name('li')
            amenities = [item.get_attribute('title') for item in items]
            all_amens.append(amenities)

        except:
            all_amens.append(None)

    all_amens = fill_missing(all_amens, expected_length)

    return all_amens
        

def get_geoloc(loc_element, agent='googlev3', check_against=(), dist_tolerance=25.0):
    '''
    Function to take in location webdriver element
    and return geo coordinates.

    Function calls to package geopy, uses argument:agent,
    and returns latitude, longitude of street address.

    args:   >loc_element (list) webdriver list of locations
            >agent (str) user agent to be passed to 
            geopy.geolocator
            >check_against (tuple) optional argument that 
            provides lat and long coordinates as a reference
            (i.e. the center point of a city or a capital)
            >dist_tolerance (float): distance value (in miles)
            a location can be from the reference coords in 
            'check_against'. If too far, function breaks.
    '''

    asrt1 = f'argument loc_element must be type(list)'
    asrt2 = f'argument agent must be type(str)'
    asrt3 = f'argument loc_element must be type(tuple)'
    asrt4 = f'argument loc_element must be type(float)'

    assert isinstance(loc_element, list), asrt1
    assert isinstance(agent, str), asrt2
    assert isinstance(check_against, tuple), asrt3
    if isinstance(dist_tolerance, int):
        dist_tolerance = float(dist_tolerance)
    assert isinstance(dist_tolerance, float), asrt4

    geolocator = Nominatim(user_agent=agent)
    addresses = []
    lats, longs = [], []
    for loc in loc_element:
        address = loc.get_attribute('title')
        addresses.append(address)
        try:
            location = geolocator.geocode(address)
            lati, longi = location.latitude, location.longitude
            lats.append(lati)
            longs.append(longi)
        except:
            lats.append(None)
            longs.append(None)
    return addresses, lats, longs

def construct_frame(street_adds, lows, highs, amens, lats, longs):

    if not len(street_adds)==len(lows)==len(highs)==len(amens)==len(lats)==len(longs):
        print(f'len of street_adds = {len(street_adds)}')
        print(f'len of lows = {len(lows)}')
        print(f'len of highs = {len(highs)}')
        print(f'len of amens = {len(amens)}')
        print(f'len of lats = {len(lats)}')
        print(f'len of longs = {len(longs)}')

    today = str(date.today())

    inf_dict = pd.DataFrame({
        'query_date':[today for i in range(len(street_adds))],
        'street_address':street_adds,
        'rent_low':lows,
        'rent_high':highs,
        'latitude':lats,
        'longitude':longs
    })

    if amens != [None] * len(street_adds):
        amns = amens_to_dict(street_adds, amens)

        df = pd.merge(left=inf_dict, left_on='street_address',
                    right=amns, right_on='address', how='outer',
                    right_index=False).drop('address', axis=1)
        return df
    return pd.DataFrame(inf_dict)

def amens_to_dict(street_adds, amenities):
    df = pd.DataFrame()
    a_dict = {}
    for z in zip(addresses, amenities):
        # print(z)
        addr = z[0]
        amns = z[1]
        if amns is None:
            continue
        a_dict['address'] = [addr]
        for a in amns:
            a_dict[a] = a_dict.get(a, ['Y'])
        if df.empty:
            df = pd.DataFrame(a_dict)
        else:
            df = pd.concat([df, pd.DataFrame(a_dict)])
    return df

def fill_missing(data_list, expected):
    remaining = expected - len(data_list)
    data_list.extend([None]*remaining)
    return data_list

def combine_dicts(a_dict, b_dict):
    for key in b_dict:
        a_dict[key] = b_dict.get(key, None)

def pull_data(file_to_match='MadisonLiving.csv', directory='.'):
    '''
    Function searches through given directory to find existing data.

    args:   file_to_match   (str)   Filename to look for.
            directory       (str)   Directory to search.
    '''
    for root, dirs, files in os.walk(directory):
        for file in files:
            name = os.path.abspath(os.path.join(root, file))
            base = os.path.basename(name)
            if base == file_to_match:
                return pd.read_csv(name)
    text = [f'No existing data file found in {os.path.abspath(directory)}',
            'Initialize new dataframe? (y/n)']
    resp = input('\n'.join(text)).lower()
    if resp == 'y':
        return pd.DataFrame()
    if resp == 'n':
        raise Exception
    return

def handle_address(df):
    df1 = df.street_address.str.split(pat=',', expand=True)
    df1.columns = ['street_address', 'city', 'area_code']
    for i, addr, city, area in df1.itertuples():
        if len(addr.split(' '))==1:
            df1.loc[i, 'area_code'] = df1.loc[i, 'city']
            df1.loc[i, 'city'] = df1.loc[i, 'street_address']
            df1.loc[i, 'street_address'] = None

    df2 = df1.area_code.str.split(pat=' ', expand=True)
    df1.drop('area_code', axis=1, inplace=True)
    df2.columns = ['blank', 'state', 'area_code']
    df2.drop('blank', axis=1, inplace=True)

    return df1.join(df2, how='outer')

def join_address(master, small):
    df = master.drop('street_address', axis=1)
    df = pd.merge(left=df, left_on=df.index, right=small, 
                  right_on=small.index, right_index=False)
    df.drop('key_0', axis=1, inplace=True)
    cols = list(df.columns)
    cols[:] = cols[-4:] + cols[:-4]
    df = df[cols]
    df

url = r'https://www.apartments.com/madison-wi/?bb=wxr38t32gKv6mm81F'

driver = Chrome()
driver.get(url)
sleep(5)

num_pages = driver.find_element_by_class_name('pageRange')
_, last_page = get_page_range(num_pages.text)

store_under = 'MadisonLiving.csv'     # filename to be used for data storage/retrieval
df = pull_data(file_to_match=store_under)

for i in range(last_page):
    print(f'on page {i+1}')
    locs = driver.find_elements_by_class_name('location')
    addresses, lats, longs = get_geoloc(locs)

    rents = driver.find_elements_by_class_name('altRentDisplay')
    lows, highs = parse_price(rents, len(locs))

    amens = driver.find_elements_by_class_name('amenities')
    amenities = get_amenities(amens, len(locs))

    
    sub = construct_frame(addresses, lows, highs, amenities, lats, longs)
    if df.empty:
        df = sub
    else:
        df = pd.concat([df, sub], ignore_index=True)

    nxt_button = driver.find_element_by_class_name('next ')
    if i == last_page - 1:      # no more pages can be found
        break
    nxt_button.click()
    sleep(5)

        # print(len(locs), len(rents), len(amens))
driver.close()
print(df)
df.to_csv(store_under)
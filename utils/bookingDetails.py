from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.keys import Keys
from pymongo import MongoClient
import pandas as pd
import time, re
from datetime import timedelta
import datetime
import math
import concurrent.futures

def read_db():
    with open('db.txt') as f:
        for line in f:
            key, value = line.strip().split(': ')
            db[key] = value.split(',')
        
def write_db():
    with open('db.txt', 'w') as f:
        for key, value in db.items():
                f.write(f"{key}: {','.join(value)}\n")
db={}
def bookingDetails(threads):
    databaseName='booking'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'    
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['booking.com'][0]=date
    db['booking.com'][1]=timeStarted
    db['booking.com'][2]='-'
    db['booking.com'][3]='-'
    db['booking.com'][-1]='running'
    write_db()     
    
    def sendData(data, columns, collectionName):
        try:
            print(f'Collected {len(data)} records!')
            df = pd.DataFrame(data, columns=columns)
            mongo_insert_data=df.to_dict('records')
            print('Sending Data to MongoDB!')

            def get_database():
                CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
                client = MongoClient(CONNECTION_STRING)
                return client[databaseName]

            dbname = get_database()
            collection_name = dbname[collectionName]
            for index,instance in enumerate(mongo_insert_data):
                collection_name.update_one({'mainUrl':instance['mainUrl']},{'$set':instance},upsert=True)
            print('Data sent to MongoDB successfully')

        except Exception as e:
            print('Some error occured while sending data MongoDB! Following is the error.')
            print(e)
            print('-----------------------------------------')
            
    def get_hotel_data(url):
        checkin_date = re.search(r'checkin=([\d-]+)', url).group(1)
        checkout_date = re.search(r'checkout=([\d-]+)', url).group(1)
        new_checkin_dt = datetime.datetime.today() + timedelta(days=60)
        new_checkout_dt = datetime.datetime.today() + timedelta(days=61)
        new_checkin_date = new_checkin_dt.strftime('%Y-%m-%d')
        new_checkout_date = new_checkout_dt.strftime('%Y-%m-%d')
        new_url = re.sub(r'checkin=([\d-]+)', f'checkin={new_checkin_date}', url)
        new_url = re.sub(r'checkout=([\d-]+)', f'checkout={new_checkout_date}', new_url)

        driver = webdriver.Chrome(ChromeDriverManager().install())
        results = []
        driver.get(new_url+"&selected_currency=USD")
        text = driver.find_element(By.XPATH, "//h1").text
        num = int(re.search(r'\d{1,3}(?:,\d{3})*', text).group().replace(',', ''))
        loopEnd = (1025 if num >= 1025 else ((num // 25) + (num % 25 > 12)) * 25) if num > 12 else 25
        for i in range(0, loopEnd, 25):
            if i != 0:
                driver.get(new_url + "&selected_currency=USD" + f"&offset={i}")

            prices = [float(price.text.replace('US$', '').replace(',', '')) for price in driver.find_elements(By.XPATH, '//span[@data-testid="price-and-discounted-price"]')]
            pricingCriteria = [criteria.text for criteria in driver.find_elements(By.XPATH, '//div[@data-testid="price-for-x-nights"]')]
            urls = [url.get_attribute('href') for url in driver.find_elements(By.XPATH, '//a[@data-testid="title-link"]')]
            ids = [prop_id.split("matching_block_id=")[1].split('_')[0][:-2] for prop_id in urls]
            mainUrls = [url.get_attribute('href').split('?')[0] for url in driver.find_elements(By.XPATH, '//a[@data-testid="title-link"]')]
            results.extend(list(zip(ids, prices, pricingCriteria, urls, mainUrls)))

        driver.quit()
        return results            
    threads=threads
    def getUrls(threads):
        print('Updating URLs for booking.com!')
        link_URLs=['https://www.booking.com/searchresults.en-us.html?ss=Nairobi+CBD%2C+Nairobi%2C+Greater+Nairobi%2C+Kenya&ssne=Nairobi&ssne_untouched=Nairobi&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=8829&dest_type=district&ac_position=4&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c5824b2502dd023e&ac_meta=GhBjNTgyNGIyNTAyZGQwMjNlIAQoATICZW46CE5haXJvYmkgQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Westlands%2C+Nairobi%2C+Greater+Nairobi%2C+Kenya&ssne=Jomo+Kenyatta+International+Airport&ssne_untouched=Jomo+Kenyatta+International+Airport&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=7375&dest_type=district&ac_position=3&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=6d794b5f3a5200a7&ac_meta=GhA2ZDc5NGI1ZjNhNTIwMGE3IAMoATICZW46BG5haXJAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Nairobi+Giraffe+Centre%2C+Nairobi%2C+Greater+Nairobi%2C+Kenya&ssne=Westlands&ssne_untouched=Westlands&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=256563&dest_type=landmark&ac_position=4&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=537a4b68f551023a&ac_meta=GhA1MzdhNGI2OGY1NTEwMjNhIAQoATICZW46BW5haXJvQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Jomo+Kenyatta+International+Airport%2C+Nairobi%2C+Greater+Nairobi%2C+Kenya&ssne=Nairobi+Giraffe+Centre&ssne_untouched=Nairobi+Giraffe+Centre&lang=en-us&src=searchresults&dest_id=223&dest_type=airport&ac_position=2&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=cad44b73b9a202fe&ac_meta=GhBjYWQ0NGI3M2I5YTIwMmZlIAIoATICZW46A25haUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D2%3Bclass%3D3%3Bclass%3D4%3Bclass%3D5',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Jomo+Kenyatta+International+Airport&ssne_untouched=Jomo+Kenyatta+International+Airport&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=e9e04b7f3a900057&ac_meta=GhBlOWUwNGI3ZjNhOTAwMDU3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Jomo+Kenyatta+International+Airport&ssne_untouched=Jomo+Kenyatta+International+Airport&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=e9e04b7f3a900057&ac_meta=GhBlOWUwNGI3ZjNhOTAwMDU3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D208%3Bht_id%3D213%3Bht_id%3D216%3Bht_id%3D206%3Bht_id%3D221%3Bht_id%3D224%3Bht_id%3D214%3Bht_id%3D228%3Bht_id%3D203%3Bht_id%3D223%3Bht_id%3D225%3Bht_id%3D205%3Bht_id%3D210%3Bht_id%3D212%3Bht_id%3D227',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=d8b35072e6db03a7&ac_meta=GhBkOGIzNTA3MmU2ZGIwM2E3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=top_destinations%3D7760%3Btop_destinations%3D11895',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=d8b35072e6db03a7&ac_meta=GhBkOGIzNTA3MmU2ZGIwM2E3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=d8b35072e6db03a7&ac_meta=GhBkOGIzNTA3MmU2ZGIwM2E3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-2256513%3Buf%3D-2258197%3Buf%3D-2258110%3Buf%3D-2254842',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Kenya&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=109&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=d8b35072e6db03a7&ac_meta=GhBkOGIzNTA3MmU2ZGIwM2E3IAEoATICZW46BWtlbnlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-2258072%3Broomfacility%3D38',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Tanzania&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=208&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c72e53d4a7a90317&ac_meta=GhBjNzJlNTNkNGE3YTkwMzE3IAEoATICZW46CHRhbnphbmlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D3',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Tanzania&ssne=Kenya&ssne_untouched=Kenya&lang=en-us&src=searchresults&dest_id=208&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c72e53d4a7a90317&ac_meta=GhBjNzJlNTNkNGE3YTkwMzE3IAEoATICZW46CHRhbnphbmlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D5%3Bclass%3D2%3Bclass%3D4%3Bclass%3D1',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Uganda&ssne=Tanzania&ssne_untouched=Tanzania&lang=en-us&src=searchresults&dest_id=219&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=306d5411d04b0146&ac_meta=GhAzMDZkNTQxMWQwNGIwMTQ2IAAoATICZW46BnVnYW5kYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Uganda&ssne=Tanzania&ssne_untouched=Tanzania&lang=en-us&src=searchresults&dest_id=219&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=306d5411d04b0146&ac_meta=GhAzMDZkNTQxMWQwNGIwMTQ2IAAoATICZW46BnVnYW5kYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D3%3Bclass%3D5%3Bclass%3D2%3Bclass%3D1%3Bclass%3D4',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Uganda&ssne=Tanzania&ssne_untouched=Tanzania&lang=en-us&src=searchresults&dest_id=219&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=306d5411d04b0146&ac_meta=GhAzMDZkNTQxMWQwNGIwMTQ2IAAoATICZW46BnVnYW5kYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D3%3Bclass%3D5%3Bclass%3D2%3Bclass%3D1%3Bclass%3D4%3Bht_id%3D204',
         'https://www.booking.com/searchresults.en-us.html?ss=Rwanda&ssne=Uganda&ssne_untouched=Uganda&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=177&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=107e542a7d6201a8&ac_meta=GhAxMDdlNTQyYTdkNjIwMWE4IAAoATICZW46BnJ3YW5kYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Democratic+Republic+of+Congo&ssne=Rwanda&ssne_untouched=Rwanda&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=50&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c088543f71b80132&ac_meta=GhBjMDg4NTQzZjcxYjgwMTMyIAEoATICZW46BWNvbmdvQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Ethiopia&ssne=Democratic+Republic+of+the+Congo&ssne_untouched=Democratic+Republic+of+the+Congo&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=68&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=dae45495117702bf&ac_meta=GhBkYWU0NTQ5NTExNzcwMmJmIAEoATICZW46BGV0aGlAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Egypt&ssne=Ethiopia&ssne_untouched=Ethiopia&lang=en-us&src=searchresults&dest_id=63&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c99554a2b4eb0757&ac_meta=GhBjOTk1NTRhMmI0ZWIwNzU3IAAoATICZW46BUVneXB0QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D5%3Bclass%3D4',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Egypt&ssne=Ethiopia&ssne_untouched=Ethiopia&lang=en-us&src=searchresults&dest_id=63&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c99554a2b4eb0757&ac_meta=GhBjOTk1NTRhMmI0ZWIwNzU3IAAoATICZW46BUVneXB0QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D3',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Egypt&ssne=Ethiopia&ssne_untouched=Ethiopia&lang=en-us&src=searchresults&dest_id=63&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c99554a2b4eb0757&ac_meta=GhBjOTk1NTRhMmI0ZWIwNzU3IAAoATICZW46BUVneXB0QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D2%3Bclass%3D1%3Bht_id%3D204%3Bclass%3D0%3Bht_id%3D228',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Egypt&ssne=Ethiopia&ssne_untouched=Ethiopia&lang=en-us&dest_id=63&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c99554a2b4eb0757&ac_meta=GhBjOTk1NTRhMmI0ZWIwNzU3IAAoATICZW46BUVneXB0QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204%3Bclass%3D2%3Bclass%3D4%3Bclass%3D5%3Bclass%3D3%3Bclass%3D1',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Egypt&ssne=Ethiopia&ssne_untouched=Ethiopia&lang=en-us&dest_id=63&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=c99554a2b4eb0757&ac_meta=GhBjOTk1NTRhMmI0ZWIwNzU3IAAoATICZW46BUVneXB0QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204%3Bclass%3D0',
         'https://www.booking.com/searchresults.en-us.html?ss=Namibia&ssne=Egypt&ssne_untouched=Egypt&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=146&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=a0ff54b02fe80991&ac_meta=GhBhMGZmNTRiMDJmZTgwOTkxIAAoATICZW46B25hbWliaWFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bht_id%3D201%3Btdb%3D2%3Btdb%3D3%3Bht_id%3D208',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Cape+Town%2C+Western+Cape%2C+South+Africa&ssne=South+Africa&ssne_untouched=South+Africa&lang=en-us&src=searchresults&dest_id=-1217214&dest_type=city&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=5ef54da4f9b3000e&ac_meta=GhA1ZWY1NGRhNGY5YjMwMDBlIAAoATICZW46CGNhcGUgdG93QABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=hotelfacility%3D107',
         'https://www.booking.com/searchresults.en-us.html?ss=Johannesburg%2C+Gauteng%2C+South+Africa&ssne=Cape+Town&ssne_untouched=Cape+Town&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=-1240261&dest_type=city&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=dabc4e2e0efe052d&ac_meta=GhBkYWJjNGUyZTBlZmUwNTJkIAAoATICZW46A2pvaEAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Durban%2C+KwaZulu-Natal%2C+South+Africa&ssne=Johannesburg&ssne_untouched=Johannesburg&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=-1224926&dest_type=city&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=8f4d4e60a3c50294&ac_meta=GhA4ZjRkNGU2MGEzYzUwMjk0IAAoATICZW46BmR1cmJhbkAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Pretoria%2C+Gauteng%2C+South+Africa&ssne=Western+Cape&ssne_untouched=Western+Cape&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=-1273769&dest_type=city&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=51cd4e7a679302d3&ac_meta=GhA1MWNkNGU3YTY3OTMwMmQzIAAoATICZW46CHByZXRvcmlhQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-1240261%3Buf%3D-1273769',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-1224926%3Buf%3D-1257338%3Buf%3D-1208880',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-1273101%3Buf%3D-1225311%3Buf%3D-1246436%3Buf%3D-1236784%3Buf%3D437319%3Buf%3D-1240139%3Buf%3D-1273448',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=south+africa&ssne=Namibia&ssne_untouched=Namibia&lang=en-us&src=searchresults&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-1217214',
         'https://www.booking.com/searchresults.en-us.html?ss=Ghana&ssne=Accra&ssne_untouched=Accra&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=81&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=44be55536607007e&ac_meta=GhA0NGJlNTU1MzY2MDcwMDdlIAEoATICZW46BWdoYW5hQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Senegal&ssne=Senegal&ssne_untouched=Senegal&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=187&dest_type=country&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Gambia&ssne=Senegal&ssne_untouched=Senegal&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=78&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=48115586afcf0324&ac_meta=GhA0ODExNTU4NmFmY2YwMzI0IAAoATICZW46BmdhbWJpYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D4%3Bclass%3D5%3Btop_destinations%3D4992',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D4%3Bclass%3D5%3Btop_destinations%3D5024',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D3%3Btop_destinations%3D5024',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D3%3Btop_destinations%3D4988',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=top_destinations%3D4988',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=top_destinations%3D5011%3Btop_destinations%3D5003%3Btop_destinations%3D5030%3Btop_destinations%3D4990%3Btop_destinations%3D4993%3Btop_destinations%3D4994',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=top_destinations%3D4809',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Morocco&ssne=Gambia&ssne_untouched=Gambia&lang=en-us&src=searchresults&dest_id=143&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b4875594b93800ec&ac_meta=GhBiNDg3NTU5NGI5MzgwMGVjIAAoATICZW46B21vcm9jY29AAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=class%3D0%3Bclass%3D3%3Bclass%3D4%3Btop_destinations%3D4988%3Bclass%3D5',
         'https://www.booking.com/searchresults.en-us.html?ss=Ivory+Coast&ssne=Morocco&ssne_untouched=Morocco&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=53&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=xu&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=7063559f63c00004&ac_meta=GhA3MDYzNTU5ZjYzYzAwMDA0IAEoATICeHU6Bml2b3J5IEAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Nigeria&ssne=Ivory+Coast&ssne_untouched=Ivory+Coast&lang=en-us&src=searchresults&dest_id=155&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=14f255ab91f20767&ac_meta=GhAxNGYyNTVhYjkxZjIwNzY3IAEoATICZW46B25pZ2VyaWFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=ht_id%3D204',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Nigeria&ssne=Ivory+Coast&ssne_untouched=Ivory+Coast&lang=en-us&src=searchresults&dest_id=155&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=14f255ab91f20767&ac_meta=GhAxNGYyNTVhYjkxZjIwNzY3IAEoATICZW46B25pZ2VyaWFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-2017355%3Buf%3D-2011499%3Buf%3D393272',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Nigeria&ssne=Ivory+Coast&ssne_untouched=Ivory+Coast&lang=en-us&src=searchresults&dest_id=155&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=14f255ab91f20767&ac_meta=GhAxNGYyNTVhYjkxZjIwNzY3IAEoATICZW46B25pZ2VyaWFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-1997013%3Buf%3D900054624',
         'https://www.booking.com/searchresults.en-us.html?label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&ss=Nigeria&ssne=Ivory+Coast&ssne_untouched=Ivory+Coast&lang=en-us&src=searchresults&dest_id=155&dest_type=country&ac_position=1&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=14f255ab91f20767&ac_meta=GhAxNGYyNTVhYjkxZjIwNzY3IAEoATICZW46B25pZ2VyaWFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure&nflt=uf%3D-2010458%3Buf%3D-2006746%3Buf%3D-2006530%3Buf%3D-1996844%3Buf%3D-2024488%3Buf%3D-2011984%3Buf%3D-1998953%3Buf%3D-2011081%3Buf%3D-2018548%3Buf%3D-2018513%3Buf%3D-2001023%3Buf%3D-2011924%3Buf%3D-2026411%3Buf%3D-2011584%3Buf%3D-2001059%3Buf%3D-2000519%3Buf%3D-2019787%3Buf%3D-2014610',
         'https://www.booking.com/searchresults.en-us.html?ss=Angola&ssne=Nigeria&ssne_untouched=Nigeria&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=6&dest_type=country&ac_position=4&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=47ad55b774fc016b&ac_meta=GhA0N2FkNTViNzc0ZmMwMTZiIAQoATICZW46BmFuZ29sYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Mozambique&ssne=Angola&ssne_untouched=Angola&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=144&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=308555c337ad0040&ac_meta=GhAzMDg1NTVjMzM3YWQwMDQwIAAoATICZW46Cm1vemFtYmlxdWVAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Malawi&ssne=Mozambique&ssne_untouched=Mozambique&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=127&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=b25855d51534017a&ac_meta=GhBiMjU4NTVkNTE1MzQwMTdhIAAoATICZW46Bm1hbGF3aUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Zambia&ssne=Malawi&ssne_untouched=Malawi&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=236&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=1d2e55dea5c7021a&ac_meta=GhAxZDJlNTVkZWE1YzcwMjFhIAAoATICZW46BnphbWJpYUAASgBQAA%3D%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Zimbabwe&ssne=Zambia&ssne_untouched=Zambia&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=237&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=e35055edc60f00e4&ac_meta=GhBlMzUwNTVlZGM2MGYwMGU0IAAoATICZW46CFppbWJhYndlQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Botswana&ssne=Zimbabwe&ssne_untouched=Zimbabwe&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=28&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=e3f655fc266b00e0&ac_meta=GhBlM2Y2NTVmYzI2NmIwMGUwIAAoATICZW46CGJvdHN3YW5hQABKAFAA&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=Burundi&ssne=Somalia&ssne_untouched=Somalia&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=35&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=1&search_selected=true&search_pageview_id=cef75615ba04002f&ac_meta=GhBjZWY3NTYxNWJhMDQwMDJmIAAoATICZW46B2J1cnVuZGlAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure',
         'https://www.booking.com/searchresults.en-us.html?ss=South+Sudan&ssne=Burundi&ssne_untouched=Burundi&label=gen173nr-1FCAEoggI46AdIM1gEaKcCiAEBmAExuAEXyAEM2AEB6AEB-AECiAIBqAIDuAKx_Z6iBsACAdICJDFiMzc5MTkzLWRhNjYtNDY2NC1hOWRlLWRiMWI5OTU1MWZjNtgCBeACAQ&aid=304142&lang=en-us&sb=1&src_elem=sb&src=searchresults&dest_id=508&dest_type=country&ac_position=0&ac_click_type=b&ac_langcode=en&ac_suggestion_list_length=5&search_selected=true&search_pageview_id=e532561b43ba02e6&ac_meta=GhBlNTMyNTYxYjQzYmEwMmU2IAAoATICZW46CnNvdXRoIHN1ZGFAAEoAUAA%3D&checkin=2023-05-01&checkout=2023-05-15&group_adults=1&no_rooms=1&group_children=0&sb_travel_purpose=leisure']

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(get_hotel_data, url) for url in link_URLs]
            for future in concurrent.futures.as_completed(futures):
                results.extend(future.result())

        databaseName = 'booking'
        sendData(results, ['propertyId', 'price', 'pricingCriteria', 'url', 'mainUrl'], 'propertyURLs')    
        
    def sendDataDetails(data, columns, collectionName):
        try:
            print(f'Collected {len(data)} records!')
            df = pd.DataFrame(data, columns=columns)
            mongo_insert_data=df.to_dict('records')
            print('Sending Data to MongoDB!')

            def get_database():
                CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
                client = MongoClient(CONNECTION_STRING)
                return client[databaseName]

            dbname = get_database()
            collection_name = dbname[collectionName]
            for index,instance in enumerate(mongo_insert_data):
                collection_name.update_one({'variantId':instance['variantId']}, {'$set': instance}, upsert=True)
            print('Data sent to MongoDB successfully')

        except Exception as e:
            print('Some error occured while sending data MongoDB! Following is the error.')
            print(e)
            print('-----------------------------------------')

    def getData():
        print("Fetching Stored URLs.")
        print('')
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['booking']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['booking']
        return db['propertyURLs']


    def get_hotel_data(url_chunk):
        driver = webdriver.Chrome(ChromeDriverManager().install())
        driver.maximize_window()
        driver.get("https://www.booking.com/?selected_currency=USD")

        for url in url_chunk:
            roomType, beds = None, None
            try:
                driver.get(url)
                try:
                    title = driver.find_element(By.XPATH, "//div[@id='hp_hotel_name']//h2").text
                except:
                    continue

                try:
                    WebDriverWait(driver, 5).until(EC.element_to_be_clickable((By.XPATH, "//span[@class='bh-photo-grid-thumb-more-inner-2']"))).click()
                    driver.execute_script("document.querySelector('div.bh-photo-modal-thumbs-grid__below').scrollIntoView()")
                    time.sleep(3)
                    driver.find_element(By.XPATH, "//button[@title='Close']").click()
                    imagesXpath = "//img[@class='bh-photo-modal-grid-image']"
                except:
                    imagesXpath = "//div[@class='clearfix bh-photo-grid bh-photo-grid--space-down fix-score-hover-opacity']//img"

                propertyId = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//button[@id='wl--wl_entrypoint_hp_head']"))
                ).get_attribute('data-hotel-id')
                stars = len(driver.find_elements(By.XPATH, "//span[starts-with(@data-testid, 'rating-')]//span"))
                address = driver.find_element(By.XPATH, "//p[@id='showMap2']/span").text
                country, city = address.split(',')[-1].strip(), address.split(',')[-2].strip()
                images = [img.get_attribute('src') for img in driver.find_elements(By.XPATH, imagesXpath)]
                description = ''.join(driver.find_element(By.XPATH, "//div[@class='hp_desc_main_content']").text.split('\n')[1:])
                highlights = driver.find_element(By.XPATH, "//div[@class='property-highlights ph-icon-fill-color']").text.replace('Property Highlights\n', '').replace('\nReserve', '')
                categoryRating = [rating.text.replace('\n', ' : ') for rating in driver.find_elements(By.XPATH, '//div[@data-testid="PropertyReviewsRegionBlock"]//div[@data-testid="review-subscore"]')]

                closestAirports = driver.execute_script('''
                    try{
                        var list = [];
                        var items = document.evaluate("//div[text()='Closest Airports']/../../../ul//li", 
                            document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null);
                        for (var i = 0; i < items.snapshotLength; i++) {
                            list.push(items.snapshotItem(i).textContent.replace('Airport', 'Airport : '));
                        }
                        return list;
                    }
                    catch{
                        return null;
                    }
                ''')

                try:
                    checkIn = driver.find_element(By.XPATH, "//div[@id='checkin_policy']//span[@class='timebar__caption']").text
                except:
                    checkIn = driver.find_element(By.XPATH, "//div[@id='checkin_policy']").text.replace('Check-in', '').strip() 

                try:
                    checkOut = driver.find_element(By.XPATH, "//div[@id='checkout_policy']//span[@class='timebar__caption']").text
                except:
                    checkOut = driver.find_element(By.XPATH, "//div[@id='checkout_policy']").text.replace('Check-out', '').strip() 

                features = driver.find_element(By.XPATH, "//div[@data-testid='facility-list-most-popular-facilities']").text.split('\n')
                breakfastIncluded = True if 'Breakfast' in highlights else False
                areaInfo = [info.text.replace('\n', ' : ') for info in driver.find_elements(By.XPATH, '(//div[@data-testid="property-section--content"])[2]//li')]

                moreBtns = driver.find_elements(By.XPATH, "//div[@class='hprt-facilities-block']//a")
                for element in moreBtns:
                    #driver.execute_script("arguments[0].scrollIntoView();", element)
                    try:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
                    except:
                        WebDriverWait(driver, 10).until(EC.element_to_be_clickable(element)).click()
                try:
                    if driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]'):
                        rating = float(driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]').text.split('\n')[0])
                        reviews = int(driver.find_element(By.XPATH, '//div[@data-testid="review-score-right-component"]').text.split('\n')[-1].split(' ')[0].replace(',', ''))
                except:
                    rating, reviews = None, None

                variants = driver.find_elements(By.XPATH, "//table[@id='hprt-table']//tbody//tr")
                totalPrice = 0
                for room in variants:
                    try:
                        totalPrice += float(room.get_attribute("data-hotel-rounded-price"))
                    except TypeError:
                        totalPrice += float(room.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))

                avgPrice = totalPrice/len(variants)

                for i in variants:
                    variantId = i.get_attribute("data-block-id")
                    breakfast, discountPercent, savings, taxesIncluded, taxAmount, refundPolicy, prePayment, cancellationPolicy = None, None, None, None, None, None, None, None
                    if 'Max. people: ' in i.find_elements(By.TAG_NAME, "td")[-4].text:
                        sleeps = i.find_elements(By.TAG_NAME, "td")[-4].text.split('Max. people: ')[-1]
                    else:
                        sleeps = i.find_elements(By.TAG_NAME, "td")[-4].text.split(' - ')[1].split(' ')[0]

                    opts = [opts.text.replace('•\n', '') for opts in i.find_elements(By.TAG_NAME, "td")[-2].find_elements(By.TAG_NAME, "li")]
                    for opt in opts:
                        if 'breakfast' in opt.lower():
                            breakfast = opt
                        elif 'refund' in opt.lower():
                            refundPolicy = opt
                        elif 'prepayment' in opt.lower() or 'advance' in opt.lower():
                            prePayment = opt
                        elif 'cancel' in opt.lower():
                            cancellationPolicy = opt

                    try:
                        price = float(i.get_attribute("data-hotel-rounded-price"))
                    except TypeError:
                        price = float(room.find_element(By.CSS_SELECTOR, "span.prco-valign-middle-helper").text.replace('US$', ''))

                    priceOpts = i.find_elements(By.TAG_NAME, "td")[-3].text.split('\n')
                    for priceOpt in priceOpts:
                        if '% off' in priceOpt:
                            discountPercent = priceOpt
                        elif "You're saving US$" in priceOpt:
                            savings = int(priceOpt.replace("You're saving US$", ''))
                        elif 'taxes' in priceOpt:
                            taxesIncluded = True if 'Includes' in priceOpt else False
                            taxAmount = None if taxesIncluded else float(re.search(r"\$\d+(,\d+)*", priceOpt).group(0).replace('$', '').replace(",", ""))

                    if len(i.find_elements(By.TAG_NAME, "td")) == 5:

                        main = i.find_elements(By.TAG_NAME, "td")[0]
                        roomType = main.find_element(By.CSS_SELECTOR, "span.hprt-roomtype-icon-link").text
                        try:
                            beds = main.find_element(By.CSS_SELECTOR, "div.hprt-roomtype-bed").text.split('\n')
                        except:
                            beds = None
                        try:
                            roomAvailability = main.find_element(By.CSS_SELECTOR, "div.thisRoomAvailabilityNew").text
                        except:
                            roomAvailability = None

                        facilities = [facilities.text for facilities in main.find_elements(By.CSS_SELECTOR, "div.hprt-facilities-facility")]
                        otherFacilities = [otherFacilities.text for otherFacilities in main.find_elements(By.CSS_SELECTOR, "ul.hprt-facilities-others li")]
                        amenities = facilities + otherFacilities
                        size = [x for x in amenities if "m²" in x][0] if [x for x in amenities if "m²" in x] else None
                        balcony = True if [x for x in amenities if "balcony" in x.lower()] else False
                        views = [x for x in amenities if "view" in x.lower()] if [x for x in amenities if "view" in x.lower()] else None

                    priceStatus, priceDiff, priceChange = None, None, None
                    data = singleItem.find_one({"mainUrl": driver.current_url.split('?')[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, avgPrice) - min(oldPrice, avgPrice) if oldPrice else 0
                    priceChange = True if (priceDiff > 0) else False
                    if avgPrice != oldPrice:
                        priceStatus = 'increased' if (avgPrice > oldPrice) else 'decreased'
                    else:
                        priceStatus = None

                    results.append([variantId, driver.current_url.split('?')[0], driver.current_url, title, propertyId, stars, address, country, city, images, description, highlights, categoryRating, closestAirports, checkIn, checkOut, features, breakfastIncluded, rating, reviews, int(sleeps), breakfast, refundPolicy, prePayment, cancellationPolicy, price, discountPercent, savings, taxesIncluded, taxAmount, roomType, beds, roomAvailability, amenities, size, balcony, views, round(avgPrice, 2), priceStatus, round(priceDiff, 2), priceChange, "$", "1 night, 1 adult", areaInfo])

            except Exception as e:
                pass

        driver.quit()     
    try:
        getUrls(threads)
        databaseName = 'booking'
        datas = getData()
        links = [data['url'].strip() for data in datas]
        singleItem = continous_connection()

        urls_per_thread = math.ceil(len(links) / threads)
        url_chunks = [links[i:i+urls_per_thread] for i in range(0, len(links), urls_per_thread)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures = [executor.submit(get_hotel_data, url_chunk) for url_chunk in url_chunks]
            results = []

        columns = ["variantId", "mainUrl", "url", "title", "propertyId", "stars", "address", "country", "city", "images", "description", "highlights", "categoryRating", "closestAirports", "checkIn", "checkOut", "features", "breakfastIncluded", "rating", "reviews", "sleeps", "breakfast", "refundPolicy", "prePayment", "cancellationPolicy", "price", "discountPercent", "savings", "taxesIncluded", "taxAmount", "roomType", "beds", "roomAvailability", "amenities", "size", "balcony", "views", "avgPrice", "priceStatus", "priceDiff", "priceChange", "currency", "pricingCriteria", "areaInfo"]
        sendDataDetails(results, columns, 'propertyDetails')        
        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['booking.com'][2]=timeEnded
        db['booking.com'][3]=dateEnded
        db['booking.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['booking.com'][-1]=f'error occured-->{e}'
        write_db()    
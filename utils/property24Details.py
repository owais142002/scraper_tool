import concurrent.futures
import requests, math
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import datetime

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
def property24Details():
    print('starting scraping for property24.com')
    databaseName='property24'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'  
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['property24.com'][0]=date
    db['property24.com'][1]=timeStarted
    db['property24.com'][2]='-'
    db['property24.com'][3]='-'
    db['property24.com'][-1]='running'
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
                collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
            print('Data sent to MongoDB successfully')

        except Exception as e:
            print('Some error occured while sending data MongoDB! Following is the error.')
            print(e)
            print('-----------------------------------------')

    def get_links(page):
        url = f"https://www.property24.com/for-sale/all-cities/gauteng/{page}"
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.text, 'lxml')
        if soup.select_one("div.p24_errorContent") or soup.find(text='No Items Found'):
            return []
        return ["https://www.property24.com"+link['href'] for link in soup.select('label.checkbox a')]

    def getLinks(url):
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.text, 'lxml')
        pages = int(soup.select_one("ul.pagination li:last-child a")['data-pagenumber'])
        if pages != 0:
            for page in range(1, pages+1 ):
                response = requests.get(url + f"/p{page}", timeout=300)
                soup = BeautifulSoup(response.text, 'lxml')
                if soup.find(text='No properties found') or soup.select_one("div.p24_errorContent"):
                    return
                urls = ['https://www.property24.com'+prop['href'] for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile a")]
                prop_ids = [prop.split('/')[-1] if prop else None for prop in urls]
                titles = [prop['title'] if 'title' in prop.attrs else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile a")]
                prices = [prop['content'] if 'content' in prop.attrs else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile span.p24_price")]
                currencies = [prop['content'] if 'content' in prop.attrs else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile span.p24_price meta")]
                beds = [prop.select_one("span.p24_featureDetails[title='Bedrooms']").text.strip() if prop.select_one("span.p24_featureDetails[title='Bedrooms']") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                baths = [prop.select_one("span.p24_featureDetails[title='Bathrooms']").text.strip() if prop.select_one("span.p24_featureDetails[title='Bathrooms']") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                parking = [prop.select_one("span.p24_featureDetails[title='Parking Spaces']").text.strip() if prop.select_one("span.p24_featureDetails[title='Parking Spaces']") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                erfSizes = [prop.select_one("span.p24_size[title='Erf Size']").text.strip() if prop.select_one("span.p24_size[title='Erf Size']") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                floorSizes = [prop.select_one("span.p24_size[title='Floor Size']").text.strip() if prop.select_one("span.p24_size[title='Floor Size']") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                listingTypes = ['sale' if '/for-sale/' in url else 'rent' for i in range(len(urls))]
                descriptions = [desc.text.strip() for desc in soup.select("span[itemprop='description']")]
                imgUrls = [prop['lazy-src'] if 'lazy-src' in prop.attrs else prop['src'] for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile a img[itemprop='image']")]
                agents = [prop.select_one("span.p24_content > span > img")['alt'] if prop.select_one("span.p24_content > span > img") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile a")]
                cities = [prop.split('/')[5].capitalize() for prop in urls]
                districts = [prop.text for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile span.p24_location")]
                country = ['South Africa' for i in range(len(urls))]
                availability = [prop.select_one("ul.p24_badges li.p24_availableBadge").text.strip() if prop.select_one("ul.p24_badges li.p24_availableBadge") else None for prop in soup.select("div.js_listingResultsContainer div.p24_regularTile")]
                data.extend([[link, ids, title, price, currency, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, cities, districts, country, availability] for link, ids, title, price, currency, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, cities, districts, country, availability in zip(urls, prop_ids, titles, prices, currencies, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, cities, districts, country, availability)])
    try:
        databaseName = 'property24'
        threads = 16
        data, links, futures = [], [], []
        print('Gathering property links !')
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            pages = range(1, 170)
            results = executor.map(get_links, pages)
            for result in results:
                links += result

            links += [i.replace('/for-sale/', '/for-rent/') for i in links]

            for link in links:
                futures.append(executor.submit(getLinks, link))
            concurrent.futures.wait(futures)

        columns = ['url', 'propertyId', 'title', 'price', 'currency', 'beds', 'baths', 'parking', 'erfSize', 'floorSize', 'listingType', 'description', 'imgUrl', 'agent', 'city', 'district', 'country', 'availability']
        sendData(data, columns, 'propertyDetails')

        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['property24.com'][2]=timeEnded
        db['property24.com'][3]=dateEnded
        db['property24.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['property24.com'][-1]=f'error occured-->{e}'
        write_db()

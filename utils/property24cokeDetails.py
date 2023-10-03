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
def property24cokeDetails(threads):
    print('starting scraping for property24.co.ke')
    databaseName='property24'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'  
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['property24.co.ke'][0]=date
    db['property24.co.ke'][1]=timeStarted
    db['property24.co.ke'][2]='-'
    db['property24.co.ke'][3]='-'
    db['property24.co.ke'][-1]='running'
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

    def getLinks(url):
        response = requests.get(url, timeout=60)
        soup = BeautifulSoup(response.text, 'lxml')
        pages = int(soup.select_one("ul.pagination li:last-child a").text)
        if pages != 0:
            for page in range(1, pages+1 ):
                if 'ongata-rongai' in url:
                    response = requests.get(url + f"&Page={page}", timeout=60)
                else:
                    response = requests.get(url + f"?Page={page}", timeout=60)
                soup = BeautifulSoup(response.text, 'lxml')
                if soup.find(text='No results were found.') or soup.select_one("div.p24_errorContent"):
                    return

                urls = ['https://www.property24.co.ke'+prop['href'] for prop in soup.select("div.js_listingTile > a.title")]
                prop_ids = [prop.split('-')[-1] if prop else None for prop in urls]
                titles = [prop['title'] for prop in soup.select("div.js_listingTile > a.title")]
                prices = [float(prop.select_one("div.sc_listingTilePrice.primaryColor span").text.strip().replace('KSh', '').replace(' ', '')) if 'POA' not in prop.select_one("div.sc_listingTilePrice.primaryColor span").text else None for prop in soup.select("div.js_listingTile")]
                currencies = [prop.select_one("div.sc_listingTilePrice.primaryColor span").text.strip().split(' ')[0].replace('POA', '') if 'POA' not in prop.select_one("div.sc_listingTilePrice.primaryColor span").text else None for prop in soup.select("div.js_listingTile")]
                pricingCriteria = [prop.select_one("span.sc_listingTilePriceRentTermDescription").text.strip() if prop.select_one("span.sc_listingTilePriceRentTermDescription") else None for prop in soup.select("div.js_listingTile")]        
                beds = [float(prop.select_one("div.sc_listingTileIcons img.property24generic_icon_beds").find_next_sibling('span').text) if prop.select_one("div.sc_listingTileIcons img.property24generic_icon_beds") else None for prop in soup.select("div.js_listingTile")]
                baths = [float(prop.select_one("div.sc_listingTileIcons img.property24generic_icon_baths").find_next_sibling('span').text) if prop.select_one("div.sc_listingTileIcons img.property24generic_icon_baths") else None for prop in soup.select("div.js_listingTile")]
                parking = [float(prop.select_one("div.sc_listingTileIcons img.property24generic_icon_parking").find_next_sibling('span').text) if prop.select_one("div.sc_listingTileIcons img.property24generic_icon_parking") else None for prop in soup.select("div.js_listingTile")]
                erfSizes = [prop.find(lambda tag: tag.name == 'span' and 'Erf Size' in tag.text).text.replace('\r', '').replace(' ', '').strip().replace('\n\n', ' ').replace('ErfSize: ', '') if prop.find(lambda tag: tag.name == 'span' and 'Erf Size' in tag.text) else None for prop in soup.select("div.js_listingTile")]
                floorSizes = [prop.find(lambda tag: tag.name == 'span' and 'Floor Size' in tag.text).text.replace('\r', '').replace(' ', '').strip().replace('\n\n', ' ').replace('FloorSize: ', '') if prop.find(lambda tag: tag.name == 'span' and 'Floor Size' in tag.text) else None for prop in soup.select("div.js_listingTile")]
                listingTypes = ['sale' if '-for-sale-' in url else 'rent' for i in range(len(urls))]
                descriptions = [' '.join(prop.select_one("div.sc_listingTileTeaser").text.strip().split(' ')[:-1]) for prop in soup.select("div.js_listingTile")]
                imgUrls = [prop.select_one('image')['src'] for prop in soup.select("div.js_listingTile > a.title")]
                agents = [prop.select_one("div.sc_listingTileTeaser img")['alt'] if prop.select_one("div.sc_listingTileTeaser img") else None for prop in soup.select("div.js_listingTile")]
                address = [prop.select_one("div.sc_listingTileAddress").text.strip() if prop.select_one("div.sc_listingTileAddress").text.strip() else None for prop in soup.select("div.js_listingTile")]
                city = [prop.split(', ')[-1].capitalize() if prop else url.split('-')[-2].capitalize() for prop in address]
                districts = [prop.split(', ')[-2].capitalize() if prop else None for prop in address]
                country = ['Kenya' for i in range(len(urls))]
                data.extend([[link, ids, title, price, currency, criteria, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, address, cities, districts, country] for link, ids, title, price, currency, criteria, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, address, cities, districts, country in zip(urls, prop_ids, titles, prices, currencies, pricingCriteria, beds, baths, parking, erfSizes, floorSizes, listingTypes, descriptions, imgUrls, agents, address, city, districts, country)])
    try:
        databaseName = 'property24_co_ke'
        threads = 16
        data, links, futures = [], [], []
        print('Gathering property links !')
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            url = "https://www.property24.co.ke/"
            response = requests.get(url, timeout=60)
            soup = BeautifulSoup(response.text, 'lxml')
            links += ['https://www.property24.co.ke'+prop['href'] for prop in soup.select("div.sc_content li a")]
            response = requests.get(url+"to-rent", timeout=60)
            soup = BeautifulSoup(response.text, 'lxml')
            links += ['https://www.property24.co.ke'+prop['href'] for prop in soup.select("div.sc_content li a")]    

            for link in links:
                futures.append(executor.submit(getLinks, link))
            concurrent.futures.wait(futures)

        columns = ['url', 'propertyId', 'title', 'price', 'currency', 'pricingCriteria', 'beds', 'baths', 'parking', 'erfSize', 'floorSize', 'listingType', 'description', 'imgUrl', 'agent', 'address', 'city', 'district', 'country']
        sendData(data, columns, 'propertyDetails')
        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['property24.co.ke'][2]=timeEnded
        db['property24.co.ke'][3]=dateEnded
        db['property24.co.ke'][-1]='completed'
        write_db()
    except Exception as e:
        db['property24.co.ke'][-1]=f'error occured-->{e}'
        write_db()

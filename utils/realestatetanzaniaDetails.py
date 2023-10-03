import concurrent.futures
import requests, math, re, time
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
def realestatetanzaniaDetails():
    print('started realestatetanzania scraper.')
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['real-estate-tanzania.beforward.jp'][0]=date
    db['real-estate-tanzania.beforward.jp'][1]=timeStarted
    db['real-estate-tanzania.beforward.jp'][2]='-'
    db['real-estate-tanzania.beforward.jp'][3]='-'
    db['real-estate-tanzania.beforward.jp'][-1]='running'    
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
                collection_name.update_one({'url':instance['url']},{'$set':instance},upsert=True)
            print('Data sent to MongoDB successfully')

        except Exception as e:
            print('Some error occured while sending data MongoDB! Following is the error.')
            print(e)
            print('-----------------------------------------')
            
    def getUrls():
        print('Updating URLs for real estate tanzania.')
        cities = ['arusha', 'pwani', 'dodoma', 'iringa', 'shinyanga', 'lindi', 'mtwara', 'mbeya', 'kilimanjaro', 'mwanza', 'njombe', 'pemba', 'tabora', 'tanga', 'zanzibar', 'dar-es-salaam']
        def get_links(cityName, page):
            retries = 3
            delay = 30
            while retries > 0:
                try:
                    url = f"https://real-estate-tanzania.beforward.jp/city/{cityName}/page/{page}/"
                    response = requests.get(url, timeout=120)
                    soup = BeautifulSoup(response.content, 'lxml')
                    if (soup.find(name='h1', string='403 Forbidden') or soup.find(name='body', string='Too Many Requests.') or soup.find(name='p', string='Too Many Requests.')):
                        print("Too many requests. Waiting....")
                        time.sleep(300)
                        response = requests.get(link[0], timeout=60)
                        soup = BeautifulSoup(response.content, 'lxml')

                    if soup.find("h1", text="Oh oh! Page not found."):
                        print('not found', link[0])
                        return

    #                 print(url)
                    return [[link, ids, price] for link, ids, price in zip(
                            [a.get('href') for a in soup.select('div.listing-view.list-view.card-deck h2.item-title a[href]')],
                            ['BFR-'+span['data-listid'] for span in soup.select('span.hz-show-lightbox-js')],
                            [float(re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', price.text.split('/')[0])[-1].replace(',', '')) if (re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', price.text) != []) else None for price in soup.select('div.item-body.flex-grow-1 li.item-price')]
                        )]

                except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                    print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                    retries -= 1
                    time.sleep(delay)
                except Exception as e:
                    print(f"Failed to scrape data for {url}: {e}")
        databaseName='realestatetanzania'
        columns=['url', 'propertyTitle', 'propertyId', 'listingType', 'beds', 'baths', 'size', 'description', 'propertyType', 'state', 'country', 'city', 'address', 'location', 'dateUpdated', 'imgUrls', 'price', 'priceDiff', 'priceStatus', 'priceChange', 'pricingCriteria', 'agent', 'agentNumber']

        opt = 1
        threads = 16
        links, all_data = [], []
        if opt == 1:
            print('Gathering property links !')
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures, links, all_data = [], [], []
                for cityName in cities:
                    try:
                        res = requests.get(f"https://real-estate-tanzania.beforward.jp/city/{cityName}/", timeout=180)
                        soup1 = BeautifulSoup(res.content, 'lxml')
                        pages = math.ceil(int(soup1.select_one("div.listing-tabs.flex-grow-1").text.replace(' Properties', '').replace(' Property', ''))/9)
                        for page in range(1, pages+1):
                            futures.append(executor.submit(get_links, cityName, page))
                    except Exception as e:
                        print(e)

                for future in concurrent.futures.as_completed(futures):
                    try:
                        links += future.result()
                    except Exception as e:
                        print(e)

            sendData(links, ['url', 'propertyId', 'price'], 'propertyURLs')     
    
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
                try:
                    collection_name.update_one({'propertyId':instance['propertyId']},{'$set':instance},upsert=True)
                except:
                    continue
            print('Data sent to MongoDB successfully')

        except Exception as e:
            print('Some error occured while sending data MongoDB! Following is the error.')
            print(e)
            print('-----------------------------------------')
            
    def scrape_data(link):
        global singleItem
        retries = 3
        delay = 10
        while retries > 0:
            try:
                response = requests.get(link[0], timeout=120)
                soup = BeautifulSoup(response.content, 'lxml')
                if (soup.find(name='h1', string='403 Forbidden') or soup.find(name='body', string='Too Many Requests.') or soup.find(name='p', string='Too Many Requests.')):
                    print("Too many requests. Waiting....")
                    time.sleep(300)
                    response = requests.get(link[0], timeout=60)
                    soup = BeautifulSoup(response.content, 'lxml')

                if soup.find("h1", text="Oh oh! Page not found."):
                    print('not found', link[0])
                    return

                propertyId = soup.find(name='strong', string='Property ID:').parent.text.replace("Property ID:", '').strip() if (soup.find(name='strong', string='Property ID:')) else None
                propertyTitle = soup.select_one('div.page-title').text.strip() if (soup.select_one('div.page-title').text.strip() != '') else None
                size = soup.find(name='strong', string='Property Size:').parent.text.replace("Property Size:", '').strip() if (soup.find(name='strong', string='Property Size:')) else None
                propertyType = soup.find(name='strong', string='Property Type:').parent.text.replace("Property Type:", '').strip() if (soup.find(name='strong', string='Property Type:')) else None
                beds = int(soup.find(name='strong', string='Bedrooms:').parent.text.replace("Bedrooms:", '').strip()) if (soup.find(name='strong', string='Bedrooms:')) else None
                baths = int(soup.find(name='strong', string='Bathrooms:').parent.text.replace("Bathrooms:", '').strip()) if (soup.find(name='strong', string='Bathrooms:')) else None
                priceStr = soup.select_one('div.page-title-wrap li.item-price').text if (soup.select_one('div.page-title-wrap li.item-price')) else None
                price = float(re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', priceStr.split('/')[0])[-1].replace(',', '')) if (re.findall(r'\d{1,3}(?:,\d{3})*(?:\.\d+)?', priceStr.split('/')[0])) else None

                currency, priceStatus, priceDiff, pricingCriteria, listingType = None, None, None, None, None
                if (price is not None):
                    currency = re.search(r'[A-Z]{3}', priceStr).group() if (re.search(r'[A-Z]{3}', priceStr)) else None
                    priceType = ' '.join(priceStr.split('/')[1:])
                    pricingCriteria = None if (priceType.isdigit() or priceType == '') else priceType
                    listingType = soup.select_one('div.property-labels-wrap a').text.strip() if (soup.select_one('div.property-labels-wrap a')) else None

                    data = singleItem.find_one({"url": link[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                    if price != oldPrice:
                        priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                    else:
                        priceStatus = None

                imgUrls = [img['src'] for img in soup.select('#property-gallery-js img.img-fluid')] if (soup.select('#property-gallery-js img.img-fluid')) else None
                state = soup.select_one('li.detail-state span').text if (soup.select_one('li.detail-state span')) else None
                country = soup.select_one('li.detail-country span').text if (soup.select_one('li.detail-country span')) else None
                city = soup.select_one('li.detail-city span').text if (soup.select_one('li.detail-city span')) else None
                address = soup.select_one('li.detail-address span').text if (soup.select_one('li.detail-address span')) else None
                location = soup.select_one('.page-title-wrap address').text if (soup.select_one('.page-title-wrap address')) else None
                dateUpdated = datetime.datetime.strptime(soup.select_one('span.small-text.grey').text.replace('Updated on', '').split('at')[0].strip(), "%B %d, %Y") if (soup.select_one('span.small-text.grey')) else None
                description = soup.find(name='h2', string='Description').parent.parent.text.replace("Description", '').strip() if (soup.find(name='h2', string='Description')) else None
                agent = soup.select_one("div#property-contact-agent-wrap li.agent-name").text.strip() if (soup.select_one("div#property-contact-agent-wrap li.agent-name")) else None
                agentNumber = soup.select_one("div#property-contact-agent-wrap span.agent-phone").text.strip() if (soup.select_one("div#property-contact-agent-wrap span.agent-phone")) else None
#                 print(link[0], propertyTitle, propertyId)
                #print(link[0], propertyTitle, propertyId, listingType, imgUrls, beds, baths, size, description, imgUrls, price, priceDiff, priceStatus, pricingCriteria, agent, agentNumber)
                print('')
                all_data.append([link[0], propertyTitle, propertyId, listingType, beds, baths, size, description, propertyType, state, country, city, address, location, dateUpdated, imgUrls, price, currency, priceDiff, priceStatus, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, pricingCriteria, agent, agentNumber])
                return

            except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                retries -= 1
                time.sleep(delay)
            except Exception as e:
                retries -= 1
                print(f"Failed to scrape data for {link[0]}: {e}")

        print(f"Max retries reached. Could not scrape {link[0]}")

    def getData():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['realEstateTanzania']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['realEstateTanzania']
        return db['propertyURLs']

    databaseName='realEstateTanzania'
    columns=['url', 'propertyTitle', 'propertyId', 'listingType', 'beds', 'baths', 'size', 'description', 'propertyType', 'state', 'country', 'city', 'address', 'location', 'dateUpdated', 'imgUrls', 'price', 'currency', 'priceDiff', 'priceStatus', 'priceChange', 'pricingCriteria', 'agent', 'agentNumber']
    try:
        getUrls()
        threads = 16
        links, all_data = [], []
        datas = getData()
        links = [list(data['url'].strip().split()) for data in datas]

        singleItem = continous_connection()
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            executor.map(scrape_data, links)

        sendData(all_data, columns, 'propertyDetails')

        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['real-estate-tanzania.beforward.jp'][2]=timeEnded
        db['real-estate-tanzania.beforward.jp'][3]=dateEnded
        db['real-estate-tanzania.beforward.jp'][-1]='completed'
        write_db()
    except Exception as e:
        db['real-estate-tanzania.beforward.jp'][-1]=f'error occured-->{e}'
        write_db()

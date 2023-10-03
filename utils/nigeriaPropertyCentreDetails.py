import requests, re 
import pandas as pd
from bs4 import BeautifulSoup
import concurrent.futures
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
print('started kenyapropertycentre.com scraper')
def nigeriaPropertyCentreDetails():
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['nigeriapropertycentre.com'][0]=date
    db['nigeriapropertycentre.com'][1]=timeStarted
    db['nigeriapropertycentre.com'][2]='-'
    db['nigeriapropertycentre.com'][3]='-'
    db['nigeriapropertycentre.com'][-1]='running'    
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
        print('Updating nigeriaPropertyCentre URLs.')
        urls = [f"https://nigeriapropertycentre.com/for-rent/short-let?page={i}" for i in range(1, 500)]
        urls = [f"https://nigeriapropertycentre.com/for-rent?page={i}" for i in range(1, 1500)]
        urls += [f"https://nigeriapropertycentre.com/for-sale?page={i}" for i in range(1, 3400)]


        def extract_links(url):
            response = requests.get(url, timeout=120)
            soup = BeautifulSoup(response.text, 'lxml')
            if soup.find(string='Sorry we could not find any property for sale in  kenya matching your criteria.', name='p'):
                return
        #     print(url)
        #     print([[link, ids, price] for link, ids, price in zip(
        #             ['https://nigeriapropertycentre.com'+a.parent.get('href') for a in soup.select('div.row.property-list h4.content-title')],
        #             [i['id'].replace('fav-', '') for i in soup.select("div.row.property-list li.save-favourite-button[id]")],
        #             [float(price.text.replace(',', '')) for price in soup.select("span.price:nth-of-type(2)")]
        #         )])
            return [[link, ids, price] for link, ids, price in zip(
                    ['https://nigeriapropertycentre.com'+a.parent.get('href') for a in soup.select('div.row.property-list h4.content-title')],
                    [i['id'].replace('fav-', '') for i in soup.select("div.row.property-list li.save-favourite-button[id]")],
                    [float(price.text.replace(',', '')) for price in soup.select("span.price:nth-of-type(2)")]
                )]

        opt = 1
        threads = 16
        databaseName = 'nigeriaPropertyCentre'
        links = []


        if opt == 1:
            print('Gathering property links !')
            print('')
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                results = executor.map(extract_links, urls)
                for result in results:
                    links += result

            sendData(links, ['url', 'propertyId', 'price'], 'propertyURLs')
    getUrls()
    def scrape_data(link):
        retries = 3
        delay = 10
        while retries > 0:
            try:
                response = requests.get(link[0], timeout=120)
                soup = BeautifulSoup(response.text, 'lxml')

                propertyTitle = soup.select_one("h4.content-title").text if (soup.select_one("h4.content-title")) else None
                propertyId = soup.select_one("li.save-favourite-button[id]")['id'].replace('fav-', '') if (soup.select_one("li.save-favourite-button[id]")) else None
                addedOn = soup.find(string='Added On:', name='strong').parent.text.replace('Added On: ', '') if (soup.find(string='Added On:', name='strong')) else None
                lastUpdated = soup.find(string='Last Updated:', name='strong').parent.text.replace('Last Updated: ', '') if (soup.find(string='Last Updated:', name='strong')) else None
                marketStatus = soup.find(string='Market Status:', name='strong').parent.text.replace('Market Status: ', '') if (soup.find(string='Market Status:', name='strong')) else None
                propertyType = soup.find(string='Type:', name='strong').parent.text.replace('Type: ', '') if (soup.find(string='Type:', name='strong')) else None
                beds = float(soup.find(string='Bedrooms:', name='strong').parent.text.replace('Bedrooms: ', '')) if (soup.find(string='Bedrooms:', name='strong')) else None
                baths = float(soup.find(string='Bathrooms:', name='strong').parent.text.replace('Bathrooms: ', '')) if (soup.find(string='Bathrooms:', name='strong')) else None
                toilets = float(soup.find(string='Toilets:', name='strong').parent.text.replace('Toilets: ', '')) if (soup.find(string='Toilets:', name='strong')) else None
                parkingSpaces = float(soup.find(string='Parking Spaces:', name='strong').parent.text.replace('Parking Spaces: ', '')) if (soup.find(string='Parking Spaces:', name='strong')) else None
                description = soup.find("p", attrs={"itemprop": "description"}).text.strip() if (soup.find("p", attrs={"itemprop": "description"})) else None
                imgUrls = [img['src'] for img in soup.select('ul li img')] if (soup.select('ul li img')) else None
                agentNumber = soup.find('input', {'id': 'fullPhoneNumbers'})['value'] if (soup.find('input', {'id': 'fullPhoneNumbers'})) else None
                agent = soup.select_one('img.company-logo')['alt'] if (soup.select_one('img.company-logo')) else None
                size = soup.find(string='Total Area:', name='strong').parent.text.replace('Total Area: ', '') if (soup.find(string='Total Area:', name='strong')) else None
                totalArea = size if size else None
                coveredArea = soup.find(string='Covered Area:', name='strong').parent.text.replace('Covered Area: ', '') if (soup.find(string='Covered Area:', name='strong')) else None
                address = soup.find('address').text.strip() if (soup.find('address')) else None
                listingType = 'For Sale' if 'for-sale' in link[0] else ('For Rent' if 'for-rent' in link[0] else None)

                price = float(soup.select('span.pull-right.property-details-price span.price')[1].text.replace(',', ''))
                currency = soup.select('span.pull-right.property-details-price span.price')[0].text.strip()
                pricingCriteria = soup.select_one("span.period").text.strip()

                priceStatus, priceDiff, priceChange = None, None, None
                if price:
                    data = singleItem.find_one({"url": link[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                    priceChange = True if (priceDiff > 0) else False
                    if price != oldPrice:
                        priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                    else:
                        priceStatus = None

                #print(link[0], propertyTitle, propertyId, addedOn, lastUpdated, marketStatus, propertyType, beds, baths, toilets, parkingSpaces, description, imgUrls, agent, agentNumber, size, totalArea, coveredArea, address, price, currency, priceDiff, priceChange, priceStatus)
#                 print(link[0], propertyTitle, propertyId)
#                 print('')

            except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                retries -= 1
                time.sleep(delay)
            except Exception as e:
                retries -= 1
                print(f"Failed to scrape data for {link[0]}: {e}")

            finally:
                try:
                    all_data.append([link[0], propertyTitle, propertyId, datetime.datetime.strptime(addedOn, '%d %b %Y'), datetime.datetime.strptime(lastUpdated, '%d %b %Y'), marketStatus, propertyType, beds, baths, toilets, parkingSpaces, description, imgUrls, agent, agentNumber, size, totalArea, coveredArea, address, price, currency, pricingCriteria, priceDiff, priceChange, priceStatus, listingType])
                    return
                except Exception as e:
                    print(e)
                    continue
        print(f"Max retries reached. Could not scrape {link[0]}")


    def getData():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['nigeriaPropertyCentre']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['nigeriaPropertyCentre']
        return db['propertyURLs']

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

    columns=['url', 'propertyTitle', 'propertyId', 'addedOn', 'lastUpdated', 'marketStatus', 'propertyType', 'beds', 'baths', 'toilets', 'parkingSpaces', 'description', 'imgUrls', 'agent', 'agentNumber', 'size', 'totalArea', 'coveredArea', 'address', 'price', 'currency', 'pricingCriteria', 'priceDiff', 'priceChange', 'priceStatus', 'listingType']
    databaseName = 'nigeriaPropertyCentre'
    threads = 16
    opt = 2
    links, all_data = [], []
    try:
        if opt == 2:
            datas = getData()
            links = [list(data['url'].strip().split()) for data in datas]

            singleItem = continous_connection()
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                executor.map(scrape_data, links)

            sendData(all_data, columns, 'propertyDetails')
        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['nigeriapropertycentre.com'][2]=timeEnded
        db['nigeriapropertycentre.com'][3]=dateEnded
        db['nigeriapropertycentre.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['nigeriapropertycentre.com'][-1]=f'error occured-->{e}'
        write_db()

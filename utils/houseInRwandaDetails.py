import concurrent.futures
import requests
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
def houseInRwandaDetails():
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['houseinrwanda.com'][0]=date
    db['houseinrwanda.com'][1]=timeStarted
    db['houseinrwanda.com'][2]='-'
    db['houseinrwanda.com'][3]='-'
    db['houseinrwanda.com'][-1]='running'
    databaseName='HouseInRwanda'
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
            
    def getUrls():
        print('Gathering property links...')
        print('')
        links = []
        for i in range(3):
            response = requests.get(f'https://www.houseinrwanda.com/?page={i}', timeout=300)
            soup = BeautifulSoup(response.content, 'lxml')
#             print('\n'.join(['https://www.houseinrwanda.com' + link['href'].replace(':', '') for link in soup.select('h5 a[href]')]))

            links += [[link, ids, price] for link, ids, price in zip(
                ['https://www.houseinrwanda.com' + link['href'].replace(':', '') for link in soup.select('h5 a[href]')],
                [prop.text.replace('| Ref: ', '') for prop in soup.find_all(attrs={'class': 'text-muted'})],
                [(float(prop.text.split(' ')[0].replace(',', '')) if (prop.text.split(' ')[0] != 'Price' and prop.text.split(' ')[0] != 'Auction') else None) for prop in soup.find_all(class_='badge bg-light text-dark')]
            )]
        sendData(links, ['url', 'propertyId', 'price'], 'propertyURLs')
        links = [list(link[0].strip().split()) for link in links] 
        
    getUrls()
    def scrape_data(link):
        retries = 3
        delay = 5
        while retries > 0:
            try:
                response = requests.get(link[0], timeout=120)
                soup = BeautifulSoup(response.content, 'lxml')
                if soup.find("h1", text="Oops, 403!") or soup.find("h1", text="Oops, 404!"):
                    print(f'nothing found in {link[0]}')
                    break
                propertyTitle = soup.find(class_='field field--name-title field--type-string field--label-hidden').text if soup.find(class_='field field--name-title field--type-string field--label-hidden') else None
                propertyId = soup.find(class_='card-header bg-secondary text-white text-center').text.replace('Summary - Ref: ', '') if (soup.find(class_='card-header bg-secondary text-white text-center')) else None
                price = soup.find(name='strong', string='Price:').parent.text.replace('Price: ', '') if soup.find(name='strong', string='Price:') else None
                priceStatus, currency, priceDiff = None, None, None
                if price:
                    priceLst = price.split(' ')
                    if (priceLst[0] != 'Price' and priceLst[0] != 'Auction'):
                        price = float(priceLst[0].replace(',', ''))
                        currency = priceLst[1]

                        data = singleItem.find_one({"url": link[0]})
                        oldPrice = data['price'] if data else None
                        priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                        if price != oldPrice:
                            priceStatus = 'increased' if (price > oldPrice) else 'decreased'

                imgUrls = list(set([a['href'] for a in soup.select('#carousel a')]))
                description = soup.select_one('meta[property="og:description"]')['content'] if soup.select_one('meta[property="og:description"]') else None 
                amenities = []
                if description:
                    amenities = [item for item in description.split('.') if "AMENITIES" in item][0].strip().replace('(AMENITIES) ', '').split(' - ') if (len([item for item in description.split('.') if "AMENITIES" in item]) > 0) else []

                beds = soup.find(name='strong', string='Bedrooms:').parent.text.replace('Bedrooms: ', '') if soup.find(name='strong', string='Bedrooms:') else None
                baths = soup.find(name='strong', string='Bathrooms:').parent.text.replace('Bathrooms: ', '') if soup.find(name='strong', string='Bathrooms:') else None
                totalFloors = soup.find(name='strong', string='Total floors:').parent.text.replace('Total floors: ', '') if soup.find(name='strong', string='Total floors:') else None
                address = soup.find(name='strong', string='Address:').parent.text.replace('Address: ', '') if soup.find(name='strong', string='Address:') else  None
                advertType = soup.find(name='strong', string='Advert type:').parent.text.replace('Advert type: ', '') if soup.find(name='strong', string='Advert type:') else None
                plotSize = soup.find(name='strong', string='Plot size:').parent.text.replace('Plot size: ', '') if soup.find(name='strong', string='Plot size:') else None
                furnished = soup.find(name='strong', string='Furnished:').parent.text.replace('Furnished: ', '') if soup.find(name='strong', string='Furnished:') else None
                propertyType = soup.find(name='strong', string='Property type:').parent.text.replace('Property type: ', '') if soup.find(name='strong', string='Property type:') else None
                expiryDate = soup.find(name='strong', string='Expiry date:').parent.text.replace('Expiry date: ', '') if soup.find(name='strong', string='Expiry date:') else None
                agentName = soup.find(name='strong', string='Name:').parent.text.replace('Name: ', '') if soup.find(name='strong', string='Name:') else None
                agentCellPhone = soup.find(name='strong', string='Cell phone:').parent.text.replace('Cell phone: ', '') if soup.find(name='strong', string='Cell phone:') else None
                agentEmailAddress = soup.find(name='strong', string='Email address:').parent.text.replace('Email address: ', '') if soup.find(name='strong', string='Email address:') else None

                #print(link[0], propertyId, propertyTitle, price, beds, totalFloors, address, advertType, plotSize, furnished, propertyType, expiryDate, agentName, agentCellPhone, agentEmailAddress)
            except (requests.exceptions.Timeout, requests.exceptions.SSLError, requests.exceptions.ConnectionResetError):
                print(f"Timeout error occurred in {link[0]}. Retrying in {delay} seconds...")
                retries -= 1
                time.sleep(delay)
                continue

            except Exception as e:
                print(f"Failed to scrape data for {link[0]}: {e}")
                break

            try:
                all_data.append([propertyId, propertyTitle, link[0], price, currency, float(priceDiff) if (priceDiff is not None and priceDiff != '') else None, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, priceStatus, imgUrls, description, amenities, int(beds) if (beds != '') else None, int(baths) if (baths != '') else None, int(totalFloors) if (totalFloors != '') else None, address, advertType, plotSize if (plotSize != '') else None, furnished, propertyType, datetime.datetime.strptime(expiryDate, "%B %d, %Y"), agentName, agentCellPhone, agentEmailAddress])
                break
            except Exception as e:
                print(f"ERROR:{link[0]}")
                continue

    def getData():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['HouseInRwanda']
        collection = db['propertyURLs']
        data = collection.find()
        print("Scraping stored URLs ...")
        print('')
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['HouseInRwanda']
        return db['propertyURLs']


    databaseName='HouseInRwanda'
    columns=['propertyId', 'propertyTitle', 'url', 'price', 'currency', 'priceDiff', 'priceChange', 'priceStatus', 'imgUrls', 'description', 'amenities', 'beds', 'baths', 'totalFloors', 'address', 'advertType', 'plotSize', 'furnished', 'propertyType', 'expiryDate', 'agentName', 'agentCellPhone', 'agentEmailAddress']    
    opt = 2
    threads = 16
    all_data = []
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
        db['houseinrwanda.com'][2]=timeEnded
        db['houseinrwanda.com'][3]=dateEnded
        db['houseinrwanda.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['houseinrwanda.com'][-1]=f'error occured-->{e}'
        write_db()

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
def sesoDetails():
    databaseName = 'SeSo'
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['seso.global'][0]=date
    db['seso.global'][1]=timeStarted
    db['seso.global'][2]='-'
    db['seso.global'][3]='-'
    db['seso.global'][-1]='running'
    write_db()
    def getData():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['SeSo']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['SeSo']
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
            
    def getUrl():
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-GB,en-PK;q=0.9,en-US;q=0.8,en;q=0.7',
            'cache-control': 'no-cache',
            'pragma': 'no-cache',
            'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
            'sec-ch-ua-mobile': '?1',
            'sec-ch-ua-platform': '"Android"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-site'
        }

        url = 'https://back.seso.global/property/search?countryId=83&countryId=160&location=1&location=6&location=4&location=21&location=20&location=19&location=17&location=22&location=2&location=3&location=16&location=5&suburbId=903&suburbId=22504&suburbId=7609&suburbId=895&suburbId=1144&suburbId=999&suburbId=23961&suburbId=26209&suburbId=1189&suburbId=2&suburbId=29092&suburbId=7032&suburbId=26210&suburbId=34328&suburbId=20755&suburbId=884&suburbId=7677&suburbId=1219&suburbId=6276&suburbId=1018&suburbId=32546&suburbId=742&suburbId=962&suburbId=34327&suburbId=34207&suburbId=26208&suburbId=34330&suburbId=33687&suburbId=29296&suburbId=30070&suburbId=26203&suburbId=5&suburbId=22339&suburbId=1221&suburbId=1265&suburbId=7654&suburbId=1266&suburbId=6977&suburbId=9&suburbId=10&suburbId=795&suburbId=34931&suburbId=33686&suburbId=21879&suburbId=6277&suburbId=852&suburbId=24159&suburbId=34326&suburbId=7331&suburbId=1190&suburbId=6284&suburbId=34331&suburbId=22335&suburbId=25337&suburbId=6976&suburbId=20754&suburbId=21367&suburbId=25341&suburbId=7138&suburbId=33806&suburbId=13'
        opt = 1
        databaseName = 'SeSo'
        if opt == 1:
            print('Gathering property links !', '\n')

            response = requests.get(url, headers=headers)
            data = response.json()

            prop_ids = [prop['id'] for prop in data['data']['search']]
            prices = [prop['propertyPrice'] for prop in data['data']['search']]
            urls = [f"https://app.seso.global/search-result/(home//modal:details/{propId})" for propId in prop_ids]
            print('\n'.join(urls))
            links = [[link, ids, price] for link, ids, price in zip(urls, prop_ids, prices)]

            sendData(links, ['url', 'propertyId', 'price'], 'propertyURLs')    
    getUrl()
    headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-GB,en-PK;q=0.9,en-US;q=0.8,en;q=0.7',
        'cache-control': 'no-cache',
        'pragma': 'no-cache',
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?1',
        'sec-ch-ua-platform': '"Android"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-site'
    }
    url = 'https://back.seso.global/property/search?countryId=83&countryId=160&location=1&location=6&location=4&location=21&location=20&location=19&location=17&location=22&location=2&location=3&location=16&location=5&suburbId=903&suburbId=22504&suburbId=7609&suburbId=895&suburbId=1144&suburbId=999&suburbId=23961&suburbId=26209&suburbId=1189&suburbId=2&suburbId=29092&suburbId=7032&suburbId=26210&suburbId=34328&suburbId=20755&suburbId=884&suburbId=7677&suburbId=1219&suburbId=6276&suburbId=1018&suburbId=32546&suburbId=742&suburbId=962&suburbId=34327&suburbId=34207&suburbId=26208&suburbId=34330&suburbId=33687&suburbId=29296&suburbId=30070&suburbId=26203&suburbId=5&suburbId=22339&suburbId=1221&suburbId=1265&suburbId=7654&suburbId=1266&suburbId=6977&suburbId=9&suburbId=10&suburbId=795&suburbId=34931&suburbId=33686&suburbId=21879&suburbId=6277&suburbId=852&suburbId=24159&suburbId=34326&suburbId=7331&suburbId=1190&suburbId=6284&suburbId=34331&suburbId=22335&suburbId=25337&suburbId=6976&suburbId=20754&suburbId=21367&suburbId=25341&suburbId=7138&suburbId=33806&suburbId=13'
    columns = ['url', 'propertyName', 'propertyId', 'area', 'currency', 'address', 'beds', 'baths', 'listingType', 'features', 'imgUrls', 'unitsAvailable', 'description', 'propertyStatus', 'price', 'priceStatus', 'priceDiff', 'priceChange']
    databaseName = 'SeSo'
    opt = 2



    response = requests.get(url, headers=headers)
    data = response.json()
    try:
        links, all_data = [], []
        if opt == 2:
            datas = getData()
            links = [list(data['url'].strip().split()) for data in datas]

        singleItem = continous_connection()

        for prop in data['data']['search']:
            propertyId = prop['id']
            url = f"https://app.seso.global/search-result/(home//modal:details/{propertyId})"
            propertyName = prop['propertyName']
            area = prop['propertySize']+' sqm' if prop['propertySize'] else None

            price = prop['propertyPrice']
            priceStatus, priceDiff, priceChange = None, None, None
            data = singleItem.find_one({"propertyId": propertyId})
            if price and data:
                oldPrice = data['price'] if data else None
                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                priceChange = True if (priceDiff > 0) else False
                if price != oldPrice:
                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                else:
                    priceStatus = None

            currency = prop['currency']['currencyInitials']
            address = prop['address']
            beds = prop['propertiesFeatures']['numberOfBedrooms'] if prop['propertiesFeatures'] else None
            baths = prop['propertiesFeatures']['numberOfBathrooms'] if prop['propertiesFeatures'] else None
            listingType = prop['tag']['name']

            keyFeatures = prop['propertiesFeatures']['keyFeatures'] if prop['propertiesFeatures'] else None
            additionalFeatures = prop['propertiesFeatures']['additionalFeatures'] if prop['propertiesFeatures'] else None
            features = None
            if (keyFeatures and additionalFeatures):
                features = [i.text.strip() for i in BeautifulSoup(keyFeatures).find_all('li')]
                features += [i.text.strip() for i in BeautifulSoup(additionalFeatures).find_all('li')]

            imgUrls = [img['imgUrl'] for img in prop['propertyImages']]
            unitsAvailable = prop['unitsAvailable']
            description = prop['propertyDescription']
            propertyStatus = prop['sesoPropertyType']['propertyTypeName']
    #         print(url, propertyName, propertyId)
            all_data.append([url, propertyName, propertyId, area, currency, address, beds, baths, listingType, features, imgUrls, unitsAvailable, description, propertyStatus, price, priceStatus, priceDiff, priceChange])

        sendData(all_data, columns, 'propertyDetails')
        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['seso.global'][2]=timeEnded
        db['seso.global'][3]=dateEnded
        db['seso.global'][-1]='completed'
        write_db()
    except Exception as e:
        db['seso.global'][-1]=f'error occured-->{e}'
        write_db()

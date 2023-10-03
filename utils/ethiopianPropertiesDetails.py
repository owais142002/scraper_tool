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
def ethiopianPropertiesDetails():
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['ethiopianproperties.com'][0]=date
    db['ethiopianproperties.com'][1]=timeStarted
    db['ethiopianproperties.com'][2]='-'
    db['ethiopianproperties.com'][3]='-'
    db['ethiopianproperties.com'][-1]='running'    
    write_db()
    databaseName='EthiopianProperties'
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
        print('Updating urls for ethiopianProperties!')
        def get_links(page_num):
            url = f"https://www.ethiopianproperties.com/property-search/page/{page_num}/"
            response = requests.get(url, headers=headers, timeout=300)
            soup = BeautifulSoup(response.content, 'lxml')
#             print('\n'.join([link['href'] for link in soup.select('div.list-container.clearfix h4 a[href]')]))

            return [[link, price] for link, price in zip(
                    [link['href'] for link in soup.select('div.list-container.clearfix h4 a[href]')],
                    [float(price.text.strip().split('-')[0].split(' ')[0].replace('$', '').replace(',', '')) if price.text.strip().split('-')[0].split(' ')[0].replace('$', '').replace(',', '') != '' else None for price in soup.select('h5.price')]
                )]

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1'
        }
        databaseName='EthiopianProperties'
        opt = 1
        threads = 16
        links, all_data = [], []
        if opt == 1:
            print('Gathering property links !')
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                future_to_links = {executor.submit(get_links, i): i for i in range(1, 125)}
                for future in concurrent.futures.as_completed(future_to_links):
                    try:
                        links += future.result()
                    except Exception as e:
                        continue

            sendData(links, ['url', 'price'], 'propertyURLs')
            links = [list(link[0].strip().split()) for link in links]   
            
    getUrls()
    def scrape_data(link):
        retries = 3
        delay = 10
        while retries > 0:
            try:
                response = requests.get(link[0], headers=headers, timeout=180)
                soup = BeautifulSoup(response.content, 'lxml')
                if soup.find("span", text="404 - Page Not Found!"):
                    print('not found', link[0])
                    return

                propertyTitle = soup.select_one('h1.page-title').text if soup.select_one('h1.page-title') else None
                propertyId = soup.select_one('h4.title').text.split(':')[1].strip() if (len(soup.select_one('h4.title').text.split(':')) > 1) else None
                priceLst = soup.select_one('h5.price span:nth-child(2)').text.strip().split('-')[0].strip().replace('$', '').replace(',', '') if soup.select_one('h5.price span:nth-child(2)') else None
                price = priceLst.split(' ')[0]
                currency, priceStatus, priceDiff = None, None, None
                if (price is not None and price != ''):
                    price = float(price)  
                    currency = "USD"

                    data = singleItem.find_one({"url": link[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                    if price != oldPrice:
                        priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                    else:
                        priceStatus = None

                priceType = ' '.join(priceLst.split(' ')[1:])
                listingType = soup.select_one('h5.price span').text.strip()
                imgUrls = [a['href'] for a in soup.select('ul.slides li a')]
                features = [feature.text for feature in soup.select('ul.arrow-bullet-list.clearfix a')]
                city = soup.select_one('nav.property-breadcrumbs li:nth-of-type(2)').text if soup.select_one('nav.property-breadcrumbs li:nth-of-type(2)') else None
                neighbourhood = soup.select_one('nav.property-breadcrumbs li:nth-of-type(3)').text if soup.select_one('nav.property-breadcrumbs li:nth-of-type(3)') else None

                props = [prop.text.replace('\n', '').replace('\xa0', '') for prop in soup.select('div.property-meta.clearfix span')[:-3]]
                beds, baths, garage, size = None, None, None, None
                for i in props:
                    if 'Bedroom' in i:
                        beds = float(i.replace('Bedroom', '').replace('s', ''))
                    elif 'Bathroom' in i:
                        baths = float(i.replace('Bathroom', '').replace('s', ''))
                    elif 'Garage' in i:
                        garage = float(i.replace('Garage', '').replace('s', ''))
                    else:
                        size = i

                content = "\n".join([desc.text for desc in soup.select('div.content.clearfix')]).replace('\xa0', '')
                description = '\n'.join(content.split('Additional Amenities')[0].strip().split('\n'))
                amenities = content.split('Additional Amenities')[1].strip().split('\n') if (len(content.split('Additional Amenities')) > 1) else []
                agentNumber = soup.select('li.office')[0].text.replace('\n', '').replace('\t', '').split(':')[1].strip() if soup.select('li.office') else "+251-911-088-114"
                #print(link[0], propertyTitle, propertyId, price, priceType if (priceType != "") else None, listingType, imgUrls, features, beds, baths, garage, size, description, amenities)
#                 print(link[0], propertyTitle, propertyId)
#                 print('')

            except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                retries -= 1
                time.sleep(delay)

            except Exception as e:
                print(f"Failed to scrape data for {link[0]}: {e}")

            finally:
                try:
                    all_data.append([propertyTitle, propertyId, link[0], price if (price != '') else None, currency if (price != '') else None, float(priceDiff) if (priceDiff != '' and priceDiff is not None) else None, priceStatus, True if (priceDiff is not None and (priceDiff > 0 and priceDiff != '')) else False, priceType if (priceType != '') else None, listingType, imgUrls, features, beds, baths, garage, size, description, amenities, "Agent Admin", agentNumber, city, neighbourhood])
                    return
                except:
                    continue
        print(f"Max retries reached. Could not scrape {link[0]}")

    def getData():
        print("Fetching Stored URLs.")
        print('')
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['EthiopianProperties']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['EthiopianProperties']
        return db['propertyURLs']


    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    databaseName='EthiopianProperties'
    columns=['propertyTitle', 'propertyId', 'url', 'price', 'currency', 'priceDiff', 'priceStatus', 'priceChange', 'pricingCriteria', 'listingType', 'imgUrls', 'features', 'beds', 'baths', 'garage', 'size', 'description', 'amenities', 'agent', 'agentNumber', 'city', 'neighbourhood']
    opt = 2
    threads = 16

    try:
        links, all_data = [], []
        if opt == 2:
            datas = getData()
            links = [list(data['url'].strip().split()) for data in datas]

            singleItem = continous_connection()
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                executor.map(scrape_data, links)

            sendData(all_data, columns, 'propertyDetails')
        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['ethiopianproperties.com'][2]=timeEnded
        db['ethiopianproperties.com'][3]=dateEnded
        db['ethiopianproperties.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['ethiopianproperties.com'][-1]=f'error occured-->{e}'
        write_db()

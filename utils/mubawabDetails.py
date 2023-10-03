import requests, re 
import pandas as pd
from bs4 import BeautifulSoup
import concurrent.futures
from pymongo import MongoClient
import datetime
import concurrent.futures
import requests, math
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import re

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
def mubawabDetails():
    databaseName='mubawab'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'  
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['mubawab.ma'][0]=date
    db['mubawab.ma'][1]=timeStarted
    db['mubawab.ma'][2]='-'
    db['mubawab.ma'][3]='-'
    db['mubawab.ma'][-1]='running'
    write_db()
    try:
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

        def getExcel_links(url):
            response = requests.get(url, timeout=60)
            soup = BeautifulSoup(response.text, 'lxml')
            records = int(re.search(r"[\d]+", soup.select_one('span.resultNum.floatR').text).group())
            if records != 0:
                for page in range(1, (records//33)+1 ):
                    response = requests.get(url + f":p:{page}", timeout=60)
                    soup = BeautifulSoup(response.text, 'lxml')
#                     print(url + f":p:{page}")
                    if soup.select_one('h3.red'):
                        if "Sorry! No results found" in soup.select_one('h3.red').text.strip().split('\n')[0]:
                            print('No Results founbd !')
                            break
                    urls = [prop['linkref'] for prop in soup.select("li.listingBox")]
                    prop_ids = [propId.split('/')[-2] for propId in urls]
                    prices = [float(re.search(r"[\d,]+", price.text).group().replace(",", "")) if 'Price on request' not in price.text.strip() else None for price in soup.select("span.priceTag")]
                    currencies = [curr.text.strip().split(' ')[-1] if 'Price on request' not in curr.text.strip() else None for curr in soup.select("span.priceTag")]
                    pricingCriteria = [criteria.find("em").text if criteria.find("em") else None for criteria in soup.select("span.priceTag")]
                    cities = [url.split('/')[-1].capitalize() for i in range(len(urls))]
                    links.extend([[link, ids, price, currency, pricingCriteria, cities] for link, ids, price, currency, pricingCriteria, cities in zip(urls, prop_ids, prices, currencies, pricingCriteria, cities)])


        excelLinks = ['https://www.mubawab.ma/en/t/casablanca',
         'https://www.mubawab.ma/en/t/marrakech',
         'https://www.mubawab.ma/en/t/tanger',
         'https://www.mubawab.ma/en/t/rabat',
         'https://www.mubawab.ma/en/t/agadir',
         'https://www.mubawab.ma/en/t/bouskoura',
         'https://www.mubawab.ma/en/t/dar-bouazza',
         'https://www.mubawab.ma/en/t/f%C3%A8s',
         'https://www.mubawab.ma/en/t/k%C3%A9nitra',
         'https://www.mubawab.ma/en/t/sal%C3%A9',
         'https://www.mubawab.ma/en/t/temara',
         'https://www.mubawab.ma/en/t/mohamm%C3%A9dia',
         'https://www.mubawab.ma/en/t/meknes',
         'https://www.mubawab.ma/en/t/el-jadida',
         'https://www.mubawab.ma/en/t/martil',
         'https://www.mubawab.ma/en/t/bouznika',
         'https://www.mubawab.ma/en/t/essaouira',
         'https://www.mubawab.ma/en/t/oujda',
         'https://www.mubawab.ma/en/t/t%C3%A9touan',
         'https://www.mubawab.ma/en/t/berrechid',
         'https://www.mubawab.ma/en/t/asilah',
         'https://www.mubawab.ma/en/t/benslimane',
         'https://www.mubawab.ma/en/t/harhoura',
         'https://www.mubawab.ma/en/t/safi',
         'https://www.mubawab.ma/en/t/mehdia',
         'https://www.mubawab.ma/en/t/errahma',
         'https://www.mubawab.ma/en/t/b%C3%A9ni-mellal',
         'https://www.mubawab.ma/en/t/sa%C3%AFdia',
         'https://www.mubawab.ma/en/t/tamesna',
         "https://www.mubawab.ma/en/t/m'diq",
         'https://www.mubawab.ma/en/t/had-soualem',
         'https://www.mubawab.ma/en/t/skhirat',
         'https://www.mubawab.ma/en/t/el-mansouria',
         'https://www.mubawab.ma/en/t/nouaceur',
         'https://www.mubawab.ma/en/t/sidi-rahal',
         'https://www.mubawab.ma/en/t/sidi-rahal-chatai',
         'https://www.mubawab.ma/en/t/settat',
         'https://www.mubawab.ma/en/t/deroua',
         'https://www.mubawab.ma/en/t/nador',
         'https://www.mubawab.ma/en/t/sidi-allal-el-bahraoui',
         'https://www.mubawab.ma/en/t/azrou',
         'https://www.mubawab.ma/en/t/ain-aouda',
         'https://www.mubawab.ma/en/t/cabo-negro',
         'https://www.mubawab.ma/en/t/taroudant',
         'https://www.mubawab.ma/en/t/tiznit',
         'https://www.mubawab.ma/en/t/el-menzeh',
         'https://www.mubawab.ma/en/t/al-hoceima',
         'https://www.mubawab.ma/en/t/tit-mellil',
         'https://www.mubawab.ma/en/t/ain-attig',
         'https://www.mubawab.ma/en/t/larache',
         'https://www.mubawab.ma/en/t/chefchaouen',
         'https://www.mubawab.ma/en/t/tifelt',
         'https://www.mubawab.ma/en/t/khouribga',
         'https://www.mubawab.ma/en/t/ourika',
         'https://www.mubawab.ma/en/t/zenata',
         'https://www.mubawab.ma/en/t/m%C3%A9diouna',
         'https://www.mubawab.ma/en/t/azemmour',
         'https://www.mubawab.ma/en/t/ouarzazate',
         'https://www.mubawab.ma/en/t/taghazout',
         'https://www.mubawab.ma/en/t/b%C3%A9ni-yakhlef',
         'https://www.mubawab.ma/en/t/ait-melloul',
         'https://www.mubawab.ma/en/t/berkane',
         'https://www.mubawab.ma/en/t/sidi-bouknadel',
         'https://www.mubawab.ma/en/t/kh%C3%A9misset',
         'https://www.mubawab.ma/en/t/fnideq',
         'https://www.mubawab.ma/en/t/gueznaia',
         'https://www.mubawab.ma/en/t/ait-ourir',
         'https://www.mubawab.ma/en/t/ben-guerir',
         'https://www.mubawab.ma/en/t/bir-jdid',
         'https://www.mubawab.ma/en/t/ifrane',
         'https://www.mubawab.ma/en/t/ksar-sghir',
         'https://www.mubawab.ma/en/t/la%C3%A4youne',
         'https://www.mubawab.ma/en/t/dakhla',
         'https://www.mubawab.ma/en/t/sefrou',
         'https://www.mubawab.ma/en/t/taza',
         'https://www.mubawab.ma/en/t/sidi-bouzid',
         'https://www.mubawab.ma/en/t/tahannaout',
         'https://www.mubawab.ma/en/t/oualidia',
         'https://www.mubawab.ma/en/t/sidi-yahya-zaer',
         'https://www.mubawab.ma/en/t/oued-laou',
         'https://www.mubawab.ma/en/t/ouled-te%C3%AFma',
         'https://www.mubawab.ma/en/t/sidi-abdallah-ghiat',
         'https://www.mubawab.ma/en/t/oulad-salah',
         'https://www.mubawab.ma/en/t/sidi-kacem',
         'https://www.mubawab.ma/en/t/a%C3%AFn-harrouda',
         'https://www.mubawab.ma/en/t/el-hajeb',
         'https://www.mubawab.ma/en/t/sidi-bibi',
         'https://www.mubawab.ma/en/t/ait-faska']
        databaseName = 'mubawab'
        threads = 16

        links = []
        print('Gathering property links !')
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures, links = [], []
            for excelLink in excelLinks:
                futures.append(executor.submit(getExcel_links, excelLink))
            concurrent.futures.wait(futures)



        sendData(links, ['url', 'propertyId', 'price', 'currency', 'pricingCriteria', 'city'], 'propertyURLs')
        def scrape_data(link):
            retries = 3
            delay = 10
            while retries > 0:
                try:
                    response = requests.get(link[0], timeout=120)
                    soup = BeautifulSoup(response.text, 'lxml')

                    title = soup.select_one("h1.searchTitle").text.strip() if soup.select_one("h1.searchTitle") else None
                    propertyId = soup.select_one("input#adId")['value'] if soup.select_one("input#adId") else None
                    description = soup.select_one("i.icon-doc-text").parent.parent.find('p').text.strip() if soup.select_one("i.icon-doc-text") else None
                    imgUrls = [imgs['src'].replace('/s/', '/h/') for imgs in soup.select("img.imgThumb")]
                    amenities = [amenity.text.strip().replace('\n', '').replace('\t', '') for amenity in soup.select("span.characIconText") + soup.select("span.tagProp.tagPromo")]
                    listingType = 'rent' if '-for-rent-' in link[0] else 'sale'
                    address = soup.select_one("i.icon-location").parent.text.strip() if soup.select_one("i.icon-location") else None
                    attrs = [attr.text.strip().replace('\n', ' ').replace('\t', '') for attr in soup.select("span.tagProp")]
                    baths = next((i for i in attrs if 'Bathroom' in i), None)
                    beds = next((i for i in attrs if 'Room' in i), None)
                    size = next((i for i in attrs if 'm²' in i), None)
                    district = (soup.select_one('h3.greyTit').text.strip().replace('\n', ' ').replace('\t', '').split(' in ')[0] if 'in' in soup.select_one('h3.greyTit').text else None) if soup.select_one('h3.greyTit') else None

                    price = float(re.search(r"[\d,]+", soup.select_one("h3.orangeTit").text).group().replace(",", "")) if soup.select_one("h3.orangeTit") else None
                    currency = soup.select_one("h3.orangeTit").text.replace('\n', ' ').split(' ')[1] if price else None
                    pricingCriteria = (soup.select_one("h3.orangeTit").find('em').text if soup.select_one("h3.orangeTit").find('em') else None) if price else None

                    priceStatus, priceDiff, priceChange, city = None, None, None, None
                    for prop in datas:
                        if prop['propertyId'] == propertyId:
                            city = prop['city']

                            if price:
                                oldPrice = prop['price']
                                priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                                priceChange = True if (priceDiff > 0) else False
                                if price != oldPrice:
                                    priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                                else:
                                    priceStatus = None

                    agent = soup.select_one("p.link a").text.strip() if soup.select_one("p.link") else None
                    if agent:
                        res = requests.get(soup.select_one("p.link a")['href'], timeout=60)
                        agentSoup = BeautifulSoup(res.text, 'lxml')
                        agentNumber = agentSoup.select_one("a.agencyLink p").text if agentSoup.select_one("a.agencyLink p") else None
                    else:
                        agentNumber = None if soup.select_one("div.refBox") else '+212 6 61 32 55 35'

#                     print(link[0], title, propertyId)
#                     print('')

                except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                    print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                    retries -= 1
                    time.sleep(delay)
                except Exception as e:
                    retries -= 1
                    #print(f"Failed to scrape data for {link[0]}: {e}")

                finally:
                    try:
                        all_data.append([link[0], title, propertyId, description, imgUrls, amenities, listingType, address, city, beds, baths, size, price, currency, pricingCriteria, priceDiff, priceChange, priceStatus, district, agent, agentNumber])
                        return
                    except Exception as e:
                        #print(e)
                        continue
            print(f"Max retries reached. Could not scrape {link[0]}")

        def getData():
            CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
            client = MongoClient(CONNECTION_STRING)
            print("Fetching URLs from database !")
            db = client['mubawab']
            collection = db['propertyURLs']
            data = collection.find()
            return list(data)

        def continous_connection():
            CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
            clientC = MongoClient(CONNECTION_STRING)
            db = clientC['mubawab']
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

        columns=['url', 'title', 'propertyId', 'description', 'imgUrls', 'amenities', 'listingType', 'address', 'city', 'beds', 'baths', 'size', 'price', 'currency', 'pricingCriteria', 'priceDiff', 'priceChange', 'priceStatus', 'district', 'agent', 'agentNumber']
        databaseName = 'mubawab'
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
        db['mubawab.ma'][2]=timeEnded
        db['mubawab.ma'][3]=dateEnded
        db['mubawab.ma'][-1]='completed'
        write_db()
    except Exception as e:
        db['mubawab.ma'][-1]=f'error occured-->{e}'
        write_db()

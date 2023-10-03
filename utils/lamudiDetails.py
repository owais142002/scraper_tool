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
def lamudiDetails():
    print('started lamudi.co.ug scraper.')
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['lamudi.co.ug'][0]=date
    db['lamudi.co.ug'][1]=timeStarted
    db['lamudi.co.ug'][2]='-'
    db['lamudi.co.ug'][3]='-'
    db['lamudi.co.ug'][-1]='running'    
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
        print('Updating lamudi URLs.')
        def get_links(page_num):
            url = f'https://lamudi.co.ug/Lamudi/Houselist.aspx?HouseCategory={page_num}'
            while True:
                response = requests.get(url, timeout=300)
                soup = BeautifulSoup(response.content, 'lxml')

                urls = ['https://lamudi.co.ug/Lamudi/'+url['href'] for url in soup.select("span.FeaturedDataListItemStyle tr:nth-of-type(2) > td > a:first-of-type")]
                prop_ids = [prop.split('=')[-1].split('#')[0] for prop in urls]
                prices = [float(price.text.split(' ')[1].replace(',', '')) for price in soup.select("span.FeaturedDataListItemStyle tr:nth-of-type(2) > td > div:first-of-type > span > span")]

                links.extend([[link, ids, price] for link, ids, price in zip(urls, prop_ids, prices)])

                if (soup.find("a", {"id": "ContentPlaceHolder1_MoreHyperlink"})):
                    url = soup.find("a", {"id": "ContentPlaceHolder1_MoreHyperlink"}).get("href")
                    if 'https:' not in url:
                        url = 'https://lamudi.co.ug/Lamudi/' + url
                else:
                    break
        databaseName = 'lamudi'
        threads = 16
        opt = 1
        links, all_data = [], []
        if opt == 1:
            print('Gathering property links !')
            with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
                futures, links = [], []
                for page_num in range(1, 50):
                    futures.append(executor.submit(get_links, page_num))
                concurrent.futures.wait(futures)

            sendData(links, ['url', 'propertyId', 'price'], 'propertyURLs')   
    getUrls()
    def scrape_data(link):
        retries = 3
        delay = 10
        while retries > 0:
            try:
                response = requests.get(link[0], timeout=180)
                soup = BeautifulSoup(response.text, 'lxml')

                propertyTitle = soup.find('meta', {'name': 'keywords'})['content'] if (soup.find('meta', {'name': 'keywords'})) else None
                propertyId = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CodeLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CodeLabel'})) else None
                category = soup.find('span', {'id': "ContentPlaceHolder1_DetailsFormView_CategoryLabel"}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_CategoryLabel'})) else None
                listingType = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_StatusLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_StatusLabel'})) else None
                beds = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BedsInWordsLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BedsInWordsLabel'})) else None
                baths = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BathsInWordsLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_BathsInWordsLabel'})) else None
                size = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_SizeLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_SizeLabel'})) else None
                tenure = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_TenureLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_TenureLabel'})) else None
                district = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_DistrictLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_DistrictLabel'})) else None
                agent = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_AgentLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_AgentLabel'})) else None
                agentNumber = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_MobileLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_MobileLabel'})) else None
                agentEmail = soup.find('span', {'id': 'ContentPlaceHolder1_FormView1_ContactEmailLabel'}).text if (soup.find('span', {'id': 'ContentPlaceHolder1_FormView1_ContactEmailLabel'})) else None
                description = soup.find('meta', {'name': 'description'})['content'] if (soup.find('meta', {'name': 'description'})) else None
                amenities = [amenity.text for amenity in soup.find_all('span', {'class': 'FourTables'})]
                location = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_LocationLabel'}).parent.text.strip() if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_LocationLabel'})) else None

                priceStr = soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Shillings'}).parent.text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Shillings'})) else (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Dollars'}).parent.text if (soup.find('span', {'id': 'ContentPlaceHolder1_DetailsFormView_Dollars'})) else None)
                price, currency, priceStatus, priceDiff, priceChange = None, None, None, None, None
                if (priceStr is not None and priceStr != ''):
                    price = float(re.findall(r"([\d,]+)", priceStr)[0].replace(",", ""))
                    currency = 'Ugx' if '\xa0' in priceStr else '$'

                    data = singleItem.find_one({"url": link[0]})
                    oldPrice = data['price'] if data else None
                    priceDiff = max(oldPrice, price) - min(oldPrice, price) if oldPrice else 0
                    priceChange = True if (priceDiff > 0) else False
                    if price != oldPrice:
                        priceStatus = 'increased' if (price > oldPrice) else 'decreased'
                    else:
                        priceStatus = None

                imgUrls = [img["src"] for img in soup.select("#wowslider-container2 a img")]

                #print(link[0], propertyTitle, propertyId, category, listingType, beds, baths, size, tenure, district, agent, agentNumber, description, amenities, location, price, currency, priceDiff, priceChange, priceStatus, imgUrls)

            except (requests.exceptions.Timeout, requests.exceptions.SSLError):
                print("Timeout error occurred. Retrying in {} seconds...".format(delay))
                retries -= 1
                time.sleep(delay)
            except Exception as e:
                retries -= 1
                print(f"Failed to scrape data for {link[0]}: {e}")

            finally:
                try:
                    all_data.append([link[0], propertyTitle, propertyId, category, listingType, beds, baths, size, tenure, district, agent, agentNumber, agentEmail, description, amenities, location, price, currency, priceDiff, priceChange, priceStatus, imgUrls])
#                     print(link[0], propertyTitle, propertyId)
                    return
                except Exception as e:
                    print(e)
                    continue
        print(f"Max retries reached. Could not scrape {link[0]}")


    def getData():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        client = MongoClient(CONNECTION_STRING)

        db = client['lamudi']
        collection = db['propertyURLs']
        data = collection.find()
        return list(data)

    def continous_connection():
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"
        clientC = MongoClient(CONNECTION_STRING)
        db = clientC['lamudi']
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


    columns=['url', 'propertyTitle', 'propertyId', 'category', 'listingType', 'beds', 'baths', 'size', 'tenure', 'district', 'agent', 'agentNumber', 'agentEmail', 'description', 'amenities', 'location', 'price', 'currency', 'priceDiff', 'priceChange', 'priceStatus', 'imgUrls']
    databaseName = 'lamudi'
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
        db['lamudi.co.ug'][2]=timeEnded
        db['lamudi.co.ug'][3]=dateEnded
        db['lamudi.co.ug'][-1]='completed'
        write_db()
    except Exception as e:
        db['lamudi.co.ug'][-1]=f'error occured-->{e}'
        write_db()

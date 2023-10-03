import pandas as pd
from pymongo import MongoClient
import PySimpleGUI as sg
import threading
from selenium.webdriver.common.keys import Keys
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
import undetected_chromedriver as uc
from selenium.webdriver.support.ui import Select
import time
import datetime
import random
import math
import concurrent.futures
import requests, math
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient

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
def prophuntDetails(threads):
    databaseName='prophunt'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'    
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['prophuntgh.com'][0]=date
    db['prophuntgh.com'][1]=timeStarted
    db['prophuntgh.com'][2]='-'
    db['prophuntgh.com'][3]='-'
    db['prophuntgh.com'][-1]='running'
    write_db()
    
    def getDatabase(databaseName):
        # Provide the mongodb atlas url to connect python to mongodb using pymongo
        CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"

        # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
        client = MongoClient(CONNECTION_STRING)

        # Create the database for our example (we will use the same database throughout the tutorial
        return client[databaseName]

    def sendData(data,columns,databaseName,collectionName):
        try:
            print(f'Collected {len(data)} records!')
            df=pd.DataFrame(data,columns=columns)
            mongo_insert_data=df.to_dict('records')
            print('Sending Data to MongoDB!')
            def get_database():

               # Provide the mongodb atlas url to connect python to mongodb using pymongo
                CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"

               # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
                client = MongoClient(CONNECTION_STRING)

               # Create the database for our example (we will use the same database throughout the tutorial
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
        response = requests.get(url, timeout=300)
        soup = BeautifulSoup(response.text, 'lxml')
        records = int(soup.select_one("#searchPropertyCount").text)
        if records != 0:
            for page in range(1, (records//9)+1 ):
                response = requests.get(url + f"/page/{page}/", timeout=300)
                soup = BeautifulSoup(response.text, 'lxml')
                print(url + f"/page/{page}/")
                urls = ['https://www.prophuntgh.com/'+url['href'] for url in soup.select("div#listingTab a.text-dark")]
                prop_ids = [prop.split('/')[-2] for prop in urls]
                prices = [float(price.text.strip().split(' ')[1].replace(',', '')) for price in soup.select('span[id^="originalPriceSpan"]')]

                links.extend([[link, ids, price] for link, ids, price in zip(urls, prop_ids, prices)])
    def getUrls():
        print('Updating URLs for prophuntgh.')
        excelLinks=['https://www.prophuntgh.com/properties-for-sale-in-accra/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-accra/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-accra/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-accra/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-cape-coast-metropolitan/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-cape-coast-metropolitan/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-cape-coast-metropolitan/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-cape-coast-metropolitan/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-danfa/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-danfa/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-danfa/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-danfa/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-dangme-west/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-dangme-west/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-dangme-west/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-dangme-west/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-dodowa/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-dodowa/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-dodowa/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-dodowa/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-ga-east/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-ga-east/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-ga-east/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-ga-east/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-ga-west/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-ga-west/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-ga-west/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-ga-west/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-haatso/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-haatso/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-haatso/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-haatso/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-kumasi/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-kumasi/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-kumasi/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-kumasi/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-sunyani/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-sunyani/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-sunyani/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-sunyani/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-takoradi/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-takoradi/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-takoradi/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-takoradi/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-tamale/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-tamale/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-tamale/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-tamale/srp',
         'https://www.prophuntgh.com/properties-for-sale-in-tema/srp',
         'https://www.prophuntgh.com/properties-for-rent-in-tema/srp',
         'https://www.prophuntgh.com/projects-for-sale-in-tema/srp',
         'https://www.prophuntgh.com/projects-for-rent-in-tema/srp']
        threads = 16

        links = []
        print('Gathering property links !')
        with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as executor:
            futures, links = [], []
            for excelLink in excelLinks:
                futures.append(executor.submit(getExcel_links, excelLink))
            concurrent.futures.wait(futures)

        sendData(links, ['url', 'propertyId', 'price'],'prophunt', 'propertyURLs')
    getUrls()
    def get_thread_list(current_session_data,threads,itemsPerThread):
        current_session_data_thread=[]
        for thread_no in range(1,threads+1):
            initial_index=(thread_no-1)*itemsPerThread
            final_index=thread_no*itemsPerThread
            current_session_data_thread.append(current_session_data[initial_index:final_index])
        return current_session_data_thread


    def start_driver():
        chromeOptions = webdriver.ChromeOptions()
        chromeOptions.add_argument("--disable-popup-blocking")
        driver = uc.Chrome(executable_path=ChromeDriverManager().install(),use_subprocess=True,options=chromeOptions)
        try:
            driver.minimize_window()
        except:
            pass
        try:
            driver.maximize_window()
        except:
            pass
        return driver

    def maximize(driver):
        try:
            driver.minimize_window()
        except:
            pass
        try:
            driver.maximize_window()
        except:
            pass


    def start_thread_process(driver_instance,chrome_instance,*working_list):
        collectionNameDetails='propertyDetails'
        databaseName='prophunt'
        currency='gh₵'
        columnsDetails=[                    'url','listingType','propertyId','title','location','dateListed','agent','agentNumber','price','currency','beds','baths'
                        ,'parking','parkingVehicles','amenities','imgUrls','description',
                        'priceChange','priceStatus','priceDiff','pricingCriteria']
    #     global chrome_status
        working_list=working_list[0]
        all_data=[]
        for index,i in enumerate(working_list):
            if len(all_data)%1000==0 and len(all_data)!=0:
                # Sending data to MongoDb
                sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
                all_data=[]
            driver_instance.get(working_list[index][0])
            driver_instance.implicitly_wait(30)
            url=working_list[index][0]
            propertyId=working_list[index][1]
            prevPrice=working_list[index][2]

            location=driver_instance.execute_script('''
                try{
                    return document.evaluate('//div[@class="title-block"]', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()

                }
                catch{
                    return null
                }
            ''')
            if location!=None:
                location=location.split('\n')[-1].strip()
            title=driver_instance.execute_script('''
                try{
                    return document.evaluate('//div[@class="title-block"]/h1', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()

                }
                catch{
                    return null
                }
            ''')
            agent=driver_instance.execute_script('''
                try{
                    return document.evaluate('//div[@id="right-contact"]//figcaption', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()

                }
                catch{
                    return null
                }
                ''')

            agentNumber=driver_instance.execute_script('''
                try{
                    return document.evaluate('//span[contains(@class,"icon-phone-call")]/following-sibling::span', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()

                }
                catch{
                    return null
                }
                ''')
            dateListed=driver_instance.execute_script('''
                try{
                    return document.evaluate('//div[contains(text(),"Posted on")]', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim().split(':')[1].trim()

                }
                catch{
                    return null
                }
                ''')
            price=driver_instance.execute_script('''
                try{
                    return parseFloat(document.evaluate('//span[contains(@class,"neg_price")]', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim().replace(/^\D+/g, '').replaceAll(',',''))

                }
                catch{
                    return null
                }
                ''')

            priceDiff=max(prevPrice,price)-min(prevPrice,price)
            if prevPrice<price:
                priceStatus='increased'
            elif prevPrice>price:
                priceStatus='decreased'
            else:
                priceStatus=None
            if price==prevPrice:
                priceChange=False
            else:
                priceChange=True
            beds=driver_instance.execute_script('''
                try{
                    return parseInt(document.evaluate('//div[contains(text(),"Bedrooms")]/following-sibling::div', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim())

                }
                catch{
                    return null
                }
                ''')
            baths=driver_instance.execute_script('''
                try{
                    return parseFloat(document.evaluate('//div[contains(text(),"Bathrooms")]/following-sibling::div', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim())

                }
                catch{
                    return null
                }
                ''')

            parkingVehicles=driver_instance.execute_script('''
                try{
                    return parseFloat(document.evaluate('//div[contains(text(),"Parking")]/following-sibling::div', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim())

                }
                catch{
                    return null
                }
                ''')
            if parkingVehicles:
                parking=True
            else:
                parking=False


            description=driver_instance.execute_script('''
                try{
                    return document.evaluate('//h2[contains(text(),"About The Property")]/following-sibling::div', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()

                }
                catch{
                    return null
                }
                ''')
            if 'sale' in url:            
                listingType='sale'
            elif 'rent' in url:
                listingType='rent'
            else:
                listingType='development'

            if listingType=='rent':
                pricingCriteria=driver_instance.execute_script('''
                try{
                    return document.evaluate('//span[contains(@class,"neg_price")]', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim().split('/').slice(-1)[0] 

                }
                catch{
                    return null
                }
                ''')
            else:
                pricingCriteria=None



            amenities=driver_instance.execute_script('''
                try{
                    var lst=[]
        for (let i = 0; i < document.evaluate('//section[@id="amenities"]/ul/li', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                            lst.push(document.evaluate('//section[@id="amenities"]/ul/li', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).textContent)
                        }
                        return lst
                }
                catch{
                return []
                }
            ''')

            imgUrls=driver_instance.execute_script('''
                try{
                var lst=[]
    var url;
        for (let i = 0; i < document.evaluate('//div[@class="slider-outer"]//img', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                            url=document.evaluate('//div[@class="slider-outer"]//img', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).src
                            if(url.includes('medium'))  {lst.push(url)}  
                        }
                        return lst
                }
                catch{
                return []
                }
            ''')


            all_data.append([url,listingType,propertyId,title,location,dateListed,agent,agentNumber,price,currency,beds,baths,
                        parking,parkingVehicles,amenities,imgUrls,description,
                        priceChange,priceStatus,priceDiff,pricingCriteria
                    ])

        if len(all_data) <999:
            sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
        # Sending data to MongoDB
        driver_instance.quit()
        print(f"Chrome--->{chrome_instance}: Extraction Completed and Data Send successfully.")
        
    
    print('Fetching URLs from prophuntgh.com database')
    dbname_1=getDatabase(databaseName)
    links=[]
    collection_name_1 = dbname_1[collectionNameURLs]
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    for i in data_mongo:
        links.append([i['url'],i['propertyId'],i['price']])
    print('Total items to be entertained:',len(links))
    try:
        itemsPerThread=math.ceil(len(links)/threads)
        current_session_data_thread=get_thread_list(links,threads,itemsPerThread) #3d list

        driver_opened=[]
        for idx,thread_list in enumerate(current_session_data_thread):
            driver_opened.append(start_driver())

        threads_item=[]
        for idx,thread_list in enumerate(current_session_data_thread):
            t1 = threading.Thread(target=start_thread_process,args=(driver_opened[idx],idx,current_session_data_thread[idx]),daemon=True)
            threads_item.append(t1)
            threads_item[-1].start()

        for thread in threads_item:
            thread.join()        

        timeEnded=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
        dateEnded=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
        db['prophuntgh.com'][2]=timeEnded
        db['prophuntgh.com'][3]=dateEnded
        db['prophuntgh.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['prophuntgh.com'][-1]=f'error occured-->{e}'
        write_db()

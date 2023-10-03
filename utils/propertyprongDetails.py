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
import re
import datetime
import random
import math
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
def propertyprongDetails(threads):
    databaseName='propertypro_ng'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'    
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['propertypro.ng'][0]=date
    db['propertypro.ng'][1]=timeStarted
    db['propertypro.ng'][2]='-'
    db['propertypro.ng'][3]='-'
    db['propertypro.ng'][-1]='running'
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

    def getUrls():
        print('Updating URLs for propertypro.ng')
        allURL=[ 'https://www.propertypro.ng/property-for-sale',
                'https://www.propertypro.ng/property-for-rent',
                'https://www.propertypro.ng/property-for-short-let',
                'https://www.propertypro.ng/properties/land'
              ]

        links=[]
        columns=['url','propertyId','listingType','price','currency']
        databaseName='propertypro_ng'
        collectionName='propertyURLs'

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


        for url in allURL:
            driver.get(url)
            driver.implicitly_wait(30)
            totalPages=driver.execute_script('''
            try{
            return parseInt(document.evaluate("//div[@class='property-number-left']/h3/strong", 
                            document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent)
                }
            catch{
            return 1
            }
            ''')
            totalPages=math.ceil(totalPages/20)
            for page in range(totalPages):
                driver.get(url+f'?page={page}')
                driver.implicitly_wait(30)
                resultsPerPage=driver.execute_script('''
                    var lst=[]
                    for (let i = 0; i < document.evaluate("//div[contains(@class,'listings-property') and not(contains(@class,'sponsored-listing'))]", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                        lst.push( [document.evaluate("//div[contains(@class,'listings-property') and not(contains(@class,'sponsored-listing'))]/div[contains(@class,'single-room-img')]/a", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).href,
                                   document.evaluate("//div[contains(@class,'listings-property') and not(contains(@class,'sponsored-listing'))]//*[contains(text(),'PID')]", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).textContent,
                                   document.evaluate("//div[contains(@class,'listings-property') and not(contains(@class,'sponsored-listing'))]//h3[@class='listings-price']/span[2]", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).textContent,
                                   document.evaluate("//div[contains(@class,'listings-property') and not(contains(@class,'sponsored-listing'))]//h3[@class='listings-price']/span[1]", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).getAttribute('content')

                                  ])
                    }

                    return lst
                ''')

                for instance in resultsPerPage:
                    if 'sale' in instance[0]:
                        listingType='sale'
                    elif 'rent' in instance[0]:
                        listingType='rent'
                    elif 'shortlet' in instance[0]:
                        listingType='shortlet'
                    links.append([instance[0],instance[1].replace('PID:','').strip(),listingType,float(''.join(re.findall('[0-9]+', instance[2].strip()))),instance[3]])

            sendData(links,columns,databaseName,collectionName) 
            links=[]
        driver.quit()    
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
        databaseName='propertypro_ng'
        columnsDetails=['url','listingType','propertyId','title','location','agent','agentNumber','price',
                        'currency','beds','baths','toilets','amenities','marketPriceLower',
                        'marketPriceHigher','imgUrls','description','priceChange','priceStatus',
                        'priceDiff']
    #     global chrome_status
        working_list=working_list[0]
        all_data=[]
        for index,i in enumerate(working_list):
            if len(all_data)%1000==0 and len(all_data)!=0:
                # Sending data to MongoDb
                sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
                all_data=[]
            counter=0
            while True:
                try:
                    driver_instance.get(working_list[index][0])
                    driver_instance.implicitly_wait(30)
                    url=working_list[index][0]
                    propertyId=working_list[index][1]
                    listingType=working_list[index][2]
                    prevPrice=working_list[index][3]
                    currency=working_list[index][4]
                    #here
                    title=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//div[@class='duplex-text']/h1", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                    ''')        
                    #here
                    location=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//div[@class='duplex-text']/h6", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')
                    agent=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//div[@class='consulting-top text-center']", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                    ''')
                    agentNumber=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//p[@class='call-hide']/following-sibling::a", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                    ''')
                    #here
                    price=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//div[@class='duplex-view-text']//strong[2]", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                    ''')
                    if price!=None:
                        price=float(''.join(re.findall('[0-9]+', price.strip())))


                    beds=driver_instance.execute_script('''
                        try{
                            return parseFloat(document.evaluate("//img[@alt='bed-icon']/parent::span", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/)[0])
                        }
                        catch{
                            return null
                        }
                    ''')
                    baths=driver_instance.execute_script('''
                        try{
                            return parseFloat(document.evaluate("//img[@alt='bath-icon']/parent::span", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/)[0])
                        }
                        catch{
                            return null
                        }
                    ''')
                    toilets=driver_instance.execute_script('''
                        try{
                            return parseFloat(document.evaluate("//img[@alt='toilet-icon']/parent::span", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/)[0])
                        }
                        catch{
                            return null
                        }
                    ''')

            #                 numberOfUnits=
            #                 size=
            #                 parking=
                    amenities=driver_instance.execute_script('''
                        try{
                            var lst=[]
                            for (let i = 0; i < document.evaluate("//div[@class='key-features-list']//li", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                                lst.push(document.evaluate("//div[@class='key-features-list']//li", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).textContent)
                            }
                            return lst
                        }
                        catch{
                            return []
                        }
                    ''')
                    marketPriceLower=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//h5[text()='ENTRY PRICE']/following-sibling::h3", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')
                    if marketPriceLower!=None:
                        marketPriceLower=marketPriceLower.replace('KSh','').strip()
                    marketPriceHigher=driver_instance.execute_script('''
                        try{
                            return document.evaluate("//h5[text()='HIGH PRICE']/following-sibling::h3", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')
                    if marketPriceHigher!=None:
                        marketPriceHigher=marketPriceHigher.replace('KSh','').strip()                


            #                 floors=
                    description=  driver_instance.execute_script('''
                        try{
                            return document.evaluate("//div[@class='description-text']", 
                        document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                    ''')          
                    if price!=None:
                        if price==prevPrice:
                            priceChange=False
                        else:
                            priceChange=True       
                        priceDiff=max(prevPrice,price)-min(prevPrice,price)
                        if prevPrice<price:
                            priceStatus='increased'
                        elif prevPrice>price:
                            priceStatus='decreased'
                        else:
                            priceStatus=None
                    else:
                        priceChange=False
                        priceDiff=None
                        priceStatus=None

                    imgUrls=driver_instance.execute_script('''
                            await new Promise(r => setTimeout(r, 1000));
                            var lst=[]
                            for (let i = 0; i < document.evaluate("//img[@class='slider-nav-img']", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                                lst.push(document.evaluate("//img[@class='slider-nav-img']", document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).src)
                            }
                            return lst

                    ''')
                    all_data.append([
                        url,listingType,propertyId,title,location,agent,agentNumber,price,currency,beds,baths,toilets,
                        amenities,marketPriceLower,marketPriceHigher,imgUrls,description,
                        priceChange,priceStatus,priceDiff

                    ])
                    break
                except Exception as e:
                    if counter==4:
                        print(e)
                        print(working_list[index][0])
                        break
                    driver_instance.execute_script('''
                        window.location.reload(true);
                    ''')
                    time.sleep(30)
                    counter=counter+1
        if len(all_data) <1000:
            sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
        # Sending data to MongoDB
        driver_instance.quit()
        print(f"Chrome--->{chrome_instance}: Extraction Completed and Data Send successfully.")
        
    
    print('Fetching URLs from propertypro.ng database')
    dbname_1=getDatabase(databaseName)
    links=[]
    collection_name_1 = dbname_1[collectionNameURLs]
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    for i in data_mongo:
        links.append([i['url'],i['propertyId'],i['listingType'],i['price'],i['currency']])
        
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
        db['propertypro.ng'][2]=timeEnded
        db['propertypro.ng'][3]=dateEnded
        db['propertypro.ng'][-1]='completed'
        write_db()
    except Exception as e:
        db['propertypro.ng'][-1]=f'error occured-->{e}'
        write_db()

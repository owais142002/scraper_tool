import pandas as pd
from pymongo import MongoClient
import PySimpleGUI as sg
import threading
from selenium.common.exceptions import TimeoutException
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
import re
from bs4 import BeautifulSoup
import requests
from lxml import etree
import json
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
def buyRentKenyaDetails(threads):
    databaseName='buyrentkenya'
    collectionNameURLs='propertyURLs'
    collectionNameDetails='propertyDetails'    
    read_db()
    timeStarted=f'{datetime.datetime.today().hour};{datetime.datetime.today().minute};{datetime.datetime.today().second}'
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    db['buyrentkenya.com'][0]=date
    db['buyrentkenya.com'][1]=timeStarted
    db['buyrentkenya.com'][2]='-'
    db['buyrentkenya.com'][3]='-'
    db['buyrentkenya.com'][-1]='running'
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
        print('Updating buyrentkenya URLs.')
        allURL=[ 
            'https://www.buyrentkenya.com/property-for-sale',
            'https://www.buyrentkenya.com/property-for-rent',
            'https://www.buyrentkenya.com/projects'
              ]

        links=[]
        columns=['url','propertyId','listingType','price']
        databaseName='buyrentkenya'
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
            return (document.evaluate('//ul[contains(@class,"pagination-page-nav")]/li[last()]',document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent)
                }
            catch{
            return 1
            }
            ''')
            if totalPages!=1:
                totalPages=int(totalPages.replace('\n',''))+100
                resultsCurrentPage=driver.execute_script('''
                var lst=[]
                for (let i = 0; i < document.evaluate('//h3[contains(@class,"hide-title")]/a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                    lst.push([document.evaluate('//h3[contains(@class,"hide-title")]/a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).href,parseFloat(document.evaluate('//div[@data-cy="listing-section"]/span/div[contains(@data-cy,"listing-")]/div', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).getAttribute('data-bi-listing-price'))])
                }
                return lst
                ''')
            else:
                resultsCurrentPage=driver.execute_script('''
                var lst=[]
                for (let i = 0; i < document.evaluate('//div[@id="mainContent"]/div[1]/div[1]/div/div[1]/div[2]/div/div[2]/div/div[1]/div/div[contains(@class,"bg-white")]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                    lst.push(document.evaluate('//div[@id="mainContent"]/div[1]/div[1]/div/div[1]/div[2]/div/div[2]/div/div[1]/div/div[contains(@class,"bg-white")]//a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).href)
                }
                return lst
                ''')
            for instance in resultsCurrentPage:
                if 'sale' in instance[0]:
                    listingType='sale'
                    links.append([instance[0],instance[0].split('-')[-1],listingType,instance[1]])
                elif 'rent' in instance[0]:
                    listingType='rent'
                    links.append([instance[0],instance[0].split('-')[-1],listingType,instance[1]])
                elif 'project' in instance:
                    listingType='project'
                    links.append([instance,instance.split('/')[-1],listingType,None])

            if totalPages!=1:
                for idx in range(2,totalPages+2):
                    driver.get(f"{url}?page={idx}")
                    driver.implicitly_wait(30)
                    breakCondition=driver.execute_script('''
                        return document.evaluate('//h3[contains(@class,"hide-title")]/a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength
                    ''')
                    if breakCondition==0:
                        break
                    resultsCurrentPage=driver.execute_script('''
                    var lst=[]
                    for (let i = 0; i < document.evaluate('//h3[contains(@class,"hide-title")]/a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                        lst.push([document.evaluate('//h3[contains(@class,"hide-title")]/a[@data-cy="listing-title-link"]', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).href,parseFloat(document.evaluate('//div[@data-cy="listing-section"]/span/div[contains(@data-cy,"listing-")]/div', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).getAttribute('data-bi-listing-price'))])
                    }
                    return lst
                    ''')
                    for instance in resultsCurrentPage:
                        if 'sale' in instance[0]:
                            listingType='sale'
                            links.append([instance[0],instance[0].split('-')[-1],listingType,instance[1]])
                        elif 'rent' in instance[0]:
                            listingType='rent'
                            links.append([instance[0],instance[0].split('-')[-1],listingType,instance[1]])
                        elif 'project' in instance:
                            listingType='project'
                            links.append([instance,instance.split('/')[-1],listingType,None])

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
        databaseName='buyrentkenya'
        columnsDetails=['url','listingType','propertyId','title','location','dateListed','agent','agentNumber','price',
                        'currency','beds','baths','numberOfUnits','size','parking','amenities','marketPriceLower',
                        'marketPriceHigher','similarPropertiesInfo','imgUrls','floors','description','priceChange','priceStatus',
                        'priceDiff','projectPriceLower','projectPriceHigher','housingType','city','suburb']
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
                    housingType=driver_instance.execute_script('''                
                            try{
                    return document.evaluate("//ul/li[contains(@class,'whitespace-nowrap')][2]", 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                                }
                            catch{
                            return null
                            }
                    ''')
                    city=driver_instance.execute_script('''                
                        try{
                    return document.evaluate("//ul/li[contains(@class,'whitespace-nowrap')][3]", 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                                }
                        catch{
                        return null
                        }
                    ''')
                    suburb=driver_instance.execute_script('''  
                    try{
                    return document.evaluate("//ul/li[contains(@class,'whitespace-nowrap')][4]", 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                                }
                        catch{
                        return null
                        }
                    ''')
                    jsonData=driver_instance.execute_script('''
                        return window.dataLayer
                    ''')

                    title=jsonData[0]['search_title']
                    if listingType!='project':
                        location=", ".join(list(set(jsonData[0]['listing_location_slug'].split('-')))).title()
                    else:
                        location=driver_instance.execute_script('''
                            return document.evaluate('//div[contains(@class,"project-details-info")]/div', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        ''')
                        location=location.split(' in ')[-1].replace('\n','')

                    dateListed= driver_instance.execute_script('''
                        try{
                            return document.evaluate('//span[contains(@date-cy,"date-created")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')
                    if dateListed!=None:
                        dateListed=datetime.datetime.strptime(dateListed,"%d/%m/%Y")

                    agent=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//button[contains(@class,"detail-listing-open-phone-modal")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.getAttribute('data-bi-listing-agency')
                        }
                        catch{
                            return null
                        }
                    ''')

                    agentNumber=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//button[contains(@class,"detail-listing-open-phone-modal")]/following-sibling::a', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.href.split('?')[0].split('+')[1]
                        }
                        catch{
                            return null
                        }
                    ''')

                    if listingType!='project':
                        price=float(jsonData[0]['listing_price'])
                        currency="KES"
                    else:
                        price=driver_instance.execute_script('''
                            return document.evaluate('//span[@id="topbar-listing-price"]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        ''')
                        price=price.replace('KSh','').replace('\n','').replace(',','')
                        currency="KES"

                    beds=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//span[contains(@aria-label,"bedrooms")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')

                    if beds!=None:
                        beds=int(beds.replace('\n',''))
                    baths=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//span[contains(@aria-label,"bathrooms")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')

                    if baths!=None:
                        baths=int(baths.replace('\n',''))
                    if listingType=='project':
                        numberOfUnits=driver_instance.execute_script('''
                        try{
                            return parseInt(document.evaluate('//div[contains(text(),"No. of Unit")]/following-sibling::div', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/)[0])
                                }
                        catch{
                            return null
                        }
                        ''')
                    else:
                        numberOfUnits=None


                    size=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//span[contains(@aria-label,"area")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d.+/)[0];
                        }
                        catch{
                            return null
                        }
                    ''')

                    parking=driver_instance.execute_script('''
                        try{
                        document.evaluate('//ul/li/div/div[contains(text(),"Parking")]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue
                        return true
                            }
                        catch{
                        return false
                        }
                    ''')

                    amenities=driver_instance.execute_script('''
                        var lst=[]
                        for (let i = 0; i < document.evaluate('(//div/p[contains(text(),"Amenities")]/parent::div/following-sibling::div)[1]//ul/li/div', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                            lst.push(document.evaluate('(//div/p[contains(text(),"Amenities")]/parent::div/following-sibling::div)[1]//ul/li/div', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).textContent.trim())

                        }
                        return lst
                    ''')
                    marketPriceLower=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//h2[contains(text(),"Market Price")]/following-sibling::div/div/div/div[2]/p[1]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/g).join('')
                        }
                        catch{
                            return null
                        }
                    ''')

                    marketPriceHigher=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//h2[contains(text(),"Market Price")]/following-sibling::div/div/div/div[2]/p[2]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/g).join('')
                        }
                        catch{
                            return null
                        }
                    ''')
                    similarPropertiesInfo=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//h2[contains(text(),"Market Price")]/following-sibling::div/div[2]//a', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent
                        }
                        catch{
                            return null
                        }
                    ''')

                    if marketPriceHigher!=None:
                        marketPriceHigher=float(marketPriceHigher)
                    if marketPriceLower!=None:
                        marketPriceLower=float(marketPriceLower)
                    if similarPropertiesInfo!=None:
                        similarPropertiesInfo=similarPropertiesInfo.replace('\n','').strip()

                    imgUrls=driver_instance.execute_script('''
                        var lst=[]
                        for (let i = 0; i < document.evaluate('(//div[@x-ref="gallery"])[1]//img', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotLength; i++) {
                            lst.push(document.evaluate('(//div[@x-ref="gallery"])[1]//img', document, null, XPathResult.ORDERED_NODE_SNAPSHOT_TYPE, null).snapshotItem(i).src)
                        }
                        return lst
                    ''')
                    if listingType=='project':
                        floors=driver_instance.execute_script('''
                            try{
                                return parseInt(document.evaluate('//div[contains(text(),"No. of Floor")]/following-sibling::div', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.match(/\d+/)[0])
                            }
                            catch{
                                return null
                            }
                        ''')
                    else:
                        floors=None
                    if listingType=='project':
                        description=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//div[@id="overview"]', 
                                    document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                                    }
                        catch{
                            return null
                        }
                        ''')
                    else:
                        description=driver_instance.execute_script('''
                        try{
                            return document.evaluate('//div[@x-html="description"]', 
                                document, null, XPathResult.FIRST_ORDERED_NODE_TYPE, null).singleNodeValue.textContent.trim()
                        }
                        catch{
                            return null
                        }
                        ''')
                    if listingType=='project':            
                        priceChange=None
                    else:
                        if price==prevPrice:
                            priceChange=False
                        else:
                            priceChange=True
                    if listingType=='project':        
                        priceDiff=None
                        priceStatus=None
                    else:
                        priceDiff=max(prevPrice,price)-min(prevPrice,price)
                        if prevPrice<price:
                            priceStatus='increased'
                        elif prevPrice>price:
                            priceStatus='decreased'
                        else:
                            priceStatus=None
                    if listingType=='project':
                        tempPriceList=price.split('-')
                        if len(tempPriceList)==1:
                            projectPriceLower=0
                            projectPriceHigher=tempPriceList[0]
                        else:
                            projectPriceLower=tempPriceList[0]
                            projectPriceHigher=tempPriceList[1]
                    else:
                        projectPriceLower=None
                        projectPriceHigher=None
                    all_data.append([
                        url,listingType,propertyId,title,location,dateListed,agent,agentNumber,price,currency,beds,baths,numberOfUnits,
                        size,parking,amenities,marketPriceLower,marketPriceHigher,similarPropertiesInfo,imgUrls,floors,description,
                        priceChange,priceStatus,priceDiff,projectPriceLower,projectPriceHigher,housingType,city,suburb
                    ])
                    break
                except Exception as e:
                    if 'Too Many Requests' not in driver_instance.find_element(By.TAG_NAME,'body').get_attribute('textContent'):
                        print(working_list[index][0])
                        break
                    if counter==4:
                        print(e)
                        print(working_list[index][0])
                        break
                    driver_instance.execute_script('''
                        window.location.reload(true);
                    ''')
                    time.sleep(30)
                    counter=counter+1
        if len(all_data) <999:
            sendData(all_data,columnsDetails,databaseName,collectionNameDetails)
        # Sending data to MongoDB
        driver_instance.quit()
        print(f"Chrome--->{chrome_instance}: Extraction Completed and Data Send successfully.")
        
    print('Fetching URLs from buyrentkenya.com database')
    dbname_1=getDatabase(databaseName)
    links=[]
    collection_name_1 = dbname_1[collectionNameURLs]
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    for i in data_mongo:
        links.append([i['url'],i['propertyId'],i['listingType'],i['price']])
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
        db['buyrentkenya.com'][2]=timeEnded
        db['buyrentkenya.com'][3]=dateEnded
        db['buyrentkenya.com'][-1]='completed'
        write_db()
    except Exception as e:
        db['buyrentkenya.com'][-1]=f'error occured-->{e}'
        write_db()

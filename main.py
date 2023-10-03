import pandas as pd
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
import concurrent.futures
import requests
from bs4 import BeautifulSoup
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

from utils.ethiopianPropertiesDetails import ethiopianPropertiesDetails
from utils.houseInRwandaDetails import houseInRwandaDetails
from utils.sesoDetails import sesoDetails
from utils.airbnbDetails import airbnbDetails
from utils.buyRentKenya import buyRentKenyaDetails
from utils.ghanaPropertyCentreDetails import ghanaPropertyCentreDetails
from utils.kenyaPropertyCentreDetails import kenyaPropertyCentreDetails
from utils.lamudiDetails import lamudiDetails
from utils.mubawabDetails import mubawabDetails
from utils.nigeriaPropertyCentreDetails import nigeriaPropertyCentreDetails
from utils.property24Details import property24Details
from utils.property24cokeDetails import property24cokeDetails
from utils.propertyprocokeDetails import propertyprocokeDetails
from utils.propertyprocougDetails import propertyprocougDetails
from utils.propertyprocozwDetails import propertyprocozwDetails
from utils.propertyprongDetails import propertyprongDetails
from utils.prophunt import prophuntDetails
from utils.realestatetanzaniaDetails import realestatetanzaniaDetails
from utils.bookingDetails import bookingDetails

from pymongo import MongoClient
import pandas as pd
import numpy as np
import re


import warnings
warnings.filterwarnings("ignore")

color='#283b5b'
highlight='#283b5c'
disabled='#FFFFFF'
buttonColor='#64778d'

def getDatabase(databaseName):
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = "mongodb+srv://david:0pFvuYveY8EIwWDs@cluster0.gfzw4mh.mongodb.net/?retryWrites=true&w=majority"

    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)

    # Create the database for our example (we will use the same database throughout the tutorial
    return client[databaseName]

def get_key(my_dict,val):
    keys=[]
    for key, value in my_dict.items():
        if val in value:
             keys.append(key)
    return keys

def hexToRgb(hex):
    """
    hex: hex string
    """
    hex = hex.replace("#", "")
    r, g, b = [int(hex[i: i + 2], 16) for i in range(0, len(hex), 2)]

    return r, g, b

def replaceColor(image, colorToReplace, replaceColor):
    """
    Replace a present color with another, applied on all image
    This method is extremely precise, only corrisponding pixels
    will be replaced with no threshold
    """
    import numpy as np

    if "#" in colorToReplace:
        colorToReplace, replaceColor = [[*hexToRgb(each), 255]
            for each in [colorToReplace, replaceColor]]

    def parser(color):
        color = str(color)
        rgbNumber = ""
        rgb = []
        for char in color:
            if char.isdigit():
                rgbNumber += char
            if char == "," or char == ")":
                rgb.append(int(rgbNumber))
                rgbNumber = ""
        r, g, b = rgb[0], rgb[1], rgb[2]
        return r, g, b

    data = np.array(image)
    r, g, b, a = data.T
    if colorToReplace is None:  # replace all pixels
        data[..., :-1] = parser(replaceColor)
        image = Image.fromarray(data)
    else:  # replace single color
        rr, gg, bb = parser(colorToReplace)
        colorToReplace = (r == rr) & (g == gg) & (b == bb)
        data[..., :-1][colorToReplace.T] = parser(replaceColor)
        image = Image.fromarray(data)

    return image

def roundCorners(im, rad):
    """
    Rounds the corners of an image to given radius
    """
    mask = Image.new("L", im.size)
    if rad > min(*im.size) // 2:
        rad = min(*im.size) // 2
    draw = ImageDraw.Draw(mask)

    draw.ellipse((0, 0, rad * 2, rad * 2), fill=255)
    draw.ellipse((0, im.height - rad * 2 -2, rad * 2, im.height-1) , fill=255)
    draw.ellipse((im.width - rad * 2, 1, im.width, rad * 2), fill=255)
    draw.ellipse(
        (im.width - rad * 2, im.height - rad * 2, im.width-1, im.height-1), fill=255
    )
    draw.rectangle([rad, 0, im.width - rad, im.height], fill=255)
    draw.rectangle([0, rad, im.width, im.height - rad], fill=255)

    mask,_ = superSample(mask, 8)
    im.putalpha(mask)


    return im

def superSample(image, sample):
    """
    Supersample an image for better edges
    image: image object
    sample: sampling multiplicator int(suggested: 2, 4, 8)
    """
    w, h = image.size

    image = image.resize((int(w * sample), int(h * sample)), resample=Image.LANCZOS)
    image = image.resize((image.width // sample, image.height // sample), resample=Image.ANTIALIAS)


    return image, ImageDraw.Draw(image)

def image_to_data(im):
    """
    This is for Pysimplegui library
    Converts image into data to be used inside GUIs
    """
    from io import BytesIO

    with BytesIO() as output:
        im.save(output, format="PNG")
        data = output.getvalue()
    return data

def getSize(text_string, font):
    ascent, descent = font.getmetrics()
    text_width, text_height = font.getsize(text_string)

    return (text_width, text_height-descent)

def getOut(w,h):
    OUT = [Image.new("RGBA", (w*5, h*5), color)]
    OUT.append(ImageDraw.Draw(OUT[0]))

    OUT[0] = roundCorners(OUT[0], 30)
    IN = replaceColor(OUT[0], color, highlight).resize((w,h), resample=Image.ANTIALIAS)
    DISABLED = replaceColor(OUT[0], color, disabled).resize((w,h), resample=Image.ANTIALIAS)
    OUT = OUT[0].resize((w,h), resample=Image.ANTIALIAS)
    OUT, IN, DISABLED = [image_to_data(each) for each in [OUT, IN, DISABLED]]
    return OUT

# button = sg.Button(text, font=('Poppins',12),border_width=0, button_color=sg.theme_background_color(),
#                    disabled_button_color=sg.theme_background_color(), 
#                    image_data=OUT)

databaseKeys={
    "ethiopianproperties.com":"EthiopianProperties",
    "houseinrwanda.com":"HouseInRwanda",
    "seso.global":"SeSo",
    "airbnb.com":"airbnb",
    "buyrentkenya.com":"buyrentkenya",
    "ghanapropertycentre.com":"ghanaPropertyCentre",
    "kenyapropertycentre.com":"kenyaPropertyCentre",
    "lamudi.co.ug":"lamudi",
    "mubawab.ma":"mubawab",
    "nigeriapropertycentre.com":"nigeriaPropertyCentre",
    "property24.com":"property24",
    "property24.co.ke":"property24_co_ke",
    "propertypro.co.ke":"propertypro_co_ke",
    "propertypro.co.ug":"propertypro_co_ug",
    "propertypro.co.zw":"propertypro_co_zw",
    "propertypro.ng":"propertypro_ng",
    "prophuntgh.com":"prophunt",
    "real-estate-tanzania.beforward.jp":"realEstateTanzania",
    "booking.com":"booking"
}

options=['ethiopianproperties.com','houseinrwanda.com','seso.global','airbnb.com','buyrentkenya.com','ghanapropertycentre.com','kenyapropertycentre.com','lamudi.co.ug','mubawab.ma','nigeriapropertycentre.com','property24.com','property24.co.ke','propertypro.co.ke','propertypro.co.ug','propertypro.co.zw','propertypro.ng','prophuntgh.com','real-estate-tanzania.beforward.jp','booking.com']

detailExtraction=None

button1Text='Download Data From MongoDB'
button2Text='Run the Scraper'
button3Text='Scrapers Status'

db= {}
def read_db():
    with open('./db.txt') as f:
        for line in f:
            key, value = line.strip().split(': ')
            db[key] = value.split(',')
        
def write_db():
    with open('./db.txt', 'w') as f:
        for key, value in db.items():
            f.write(f"{key}: {','.join(value)}\n")

def makeCsv(databaseName):    
    dbname=getDatabase(databaseName)
    links=[]
    collectionName = dbname['propertyDetails']
    data=list(collectionName.find({},{'_id':False}))
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    pd.DataFrame(data).to_csv(f'{databaseName}[{date}].csv')
    print(f'{databaseName} csv file created!')

def downloadCombinedData(databaseName):    
    dbname_1=getDatabase(databaseName)
    data=[]
    collection_name_1 = dbname_1['data']
    data_mongo=list(collection_name_1.find({},{'_id':False}))
    date=f'{datetime.datetime.today().day}-{datetime.datetime.today().month}-{datetime.datetime.today().year}'
    pd.DataFrame(data_mongo,columns=data_mongo[0].keys()).to_csv(f'CombinedData[{date}].csv')
    print('Combined csv created!')
    
def getStatus():    
    radio_choices = ['Revisit every property urls from database and update their contents only.',
                     'Rescrape the property urls and ids again (the data collected will get pushed to urls collection too).']
    layout = [
                [sg.Text('airbnb.com scraper')],
                [sg.Radio(text, 1) for text in radio_choices],
                [sg.Button("SUBMIT",font=('Poppins',12))]
             ]

    window = sg.Window('Select Criteria of Scraping', layout)

    while True:             # Event Loop
        event, values = window.read()
        if event == sg.WIN_CLOSED or event=="Exit":
            window.close()
            break

        elif event == "SUBMIT":
            status=(values.values())
            window.close()
            break
    status=list(status)
    return status

while True:    #Main Window
    databaseKeys={
        "ethiopianproperties.com":"EthiopianProperties",
        "houseinrwanda.com":"HouseInRwanda",
        "seso.global":"SeSo",
        "airbnb.com":"airbnb",
        "buyrentkenya.com":"buyrentkenya",
        "ghanapropertycentre.com":"ghanaPropertyCentre",
        "kenyapropertycentre.com":"kenyaPropertyCentre",
        "lamudi.co.ug":"lamudi",
        "mubawab.ma":"mubawab",
        "nigeriapropertycentre.com":"nigeriaPropertyCentre",
        "property24.com":"property24",
        "property24.co.ke":"property24_co_ke",
        "propertypro.co.ke":"propertypro_co_ke",
        "propertypro.co.ug":"propertypro_co_ug",
        "propertypro.co.zw":"propertypro_co_zw",
        "propertypro.ng":"propertypro_ng",
        "prophuntgh.com":"prophunt",
        "real-estate-tanzania.beforward.jp":"realEstateTanzania",
        "booking.com":"booking"
    }

    options=['ethiopianproperties.com','houseinrwanda.com','seso.global','airbnb.com','buyrentkenya.com','ghanapropertycentre.com','kenyapropertycentre.com','lamudi.co.ug','mubawab.ma','nigeriapropertycentre.com','property24.com','property24.co.ke','propertypro.co.ke','propertypro.co.ug','propertypro.co.zw','propertypro.ng','prophuntgh.com','real-estate-tanzania.beforward.jp','booking.com']    
    read_db()
    print('Main Window')
    layout = [
        [sg.Text('Rehani LLC Scraper',font=('Poppins',14))],
        [sg.Button(button1Text,font=('Poppins',12),image_data=getOut(260,40),button_color=buttonColor,border_width=0)],
        [sg.Button(button2Text,font=('Poppins',12),image_data=getOut(150,40),button_color=buttonColor,border_width=0)],
        [sg.Button(button3Text,font=('Poppins',12),image_data=getOut(150,40),button_color=buttonColor,border_width=0)],
        [sg.Button('Close',font=('Poppins',12),pad=((0, 0), (100, 0)),image_data=getOut(100,40),button_color=buttonColor,border_width=0)],
    ]
    startScraper=False
    downloadData=False
    scraperStatus=False

    window = sg.Window('Rehani LLC Scraper', layout,size=(300, 350),element_justification="center",text_justification="center",resizable=True,font=('Poppins',12),icon='./logo.ico')

    while True:
        event, values = window.read()
        if event in (sg.WIN_CLOSED, 'Exit'):
            break
        if event == button1Text:
            downloadData=True
            window.close()
            break
        elif event == button2Text:
            startScraper=True
            window.close()
            break
        elif event == button3Text:
            scraperStatus=True
            window.close()        
            break
        elif event == 'Close':
            window.close()        
            break            

    if True not in [downloadData,startScraper,scraperStatus]:
        print('Closing Rehani LLC Scraper!')
        break


    if startScraper:
        runningScrapers=get_key(db,'running')
        if runningScrapers!=[]:
            runningScrapersText='\n'.join(runningScrapers)
            for item in runningScrapers:
                options.remove(item)
        else:
            runningScrapersText='None'
        
        backStatus=False
        #Start scraper Window
        print('Starting Scraper')
        layout = [
            [sg.Text('Select Website and Extraction website',font=('Poppins',14))],
            [sg.Combo(options, default_value=options[0],font=('Poppins',14))],
            [sg.Text('Scrapers Running:',font=('Poppins',12))],
            [sg.Multiline(runningScrapersText,size=(100,3),font=('Poppins',12))],
            [sg.Text('Threads'), sg.InputText()],
            [sg.Button('Start',font=('Poppins',12),image_data=getOut(100,40),button_color=buttonColor,border_width=0)],
            [sg.Button('Back',font=('Poppins',12),pad=((0, 0), (40, 0)),image_data=getOut(100,40),button_color=buttonColor,border_width=0)],
        ]

        window = sg.Window('Rehani LLC Scraper', layout,size=(400, 400),element_justification="center",text_justification="center",resizable=True,font=('Poppins',12),icon='./logo.ico')

        while True:
            event, values = window.read()
            if event=='Back':
                window.close()
                backStatus=True
                websiteName=None
                break
            elif event == 'Start':
#                 detailExtraction=values[0]
                websiteName=values[0]
                try:
                    threads=int(values[2])
                except:
                    threads=2
                window.close()
                break
            elif event in (sg.WIN_CLOSED, 'Exit'):
                detailExtraction=None
                websiteName=None
                break 
#         db[websiteName]='started'
#         write_db()
#         if detailExtraction:
        if websiteName=='ethiopianproperties.com':                
            t = threading.Thread(target=ethiopianPropertiesDetails, args=([]))
            t.start()
        elif websiteName=='houseinrwanda.com':
            t = threading.Thread(target=houseInRwandaDetails, args=([]))
            t.start()   
        elif websiteName=='seso.global':
            t = threading.Thread(target=sesoDetails, args=([]))
            t.start()  
        elif websiteName=='airbnb.com':
            t = threading.Thread(target=airbnbDetails, args=([threads]))
            t.start()
        elif websiteName=='buyrentkenya.com':
            t = threading.Thread(target=buyRentKenyaDetails, args=([threads]))
            t.start()
        elif websiteName=='ghanapropertycentre.com':
            t = threading.Thread(target=ghanaPropertyCentreDetails, args=([]))
            t.start()
        elif websiteName=='kenyapropertycentre.com':
            t = threading.Thread(target=kenyaPropertyCentreDetails, args=([]))
            t.start()
        elif websiteName=='lamudi.co.ug':
            t = threading.Thread(target=lamudiDetails, args=([]))
            t.start()                
        elif websiteName=='mubawab.ma':
            print('here')
            t = threading.Thread(target=mubawabDetails, args=([]))
            t.start()
        elif websiteName=='nigeriapropertycentre.com':
            t = threading.Thread(target=nigeriaPropertyCentreDetails, args=([]))
            t.start()
        elif websiteName=='property24.com':
            t = threading.Thread(target=property24Details, args=([]))
            t.start()
        elif websiteName=='property24.co.ke':
            t = threading.Thread(target=property24cokeDetails, args=([]))
            t.start()
        elif websiteName=='propertypro.co.ke':
            t = threading.Thread(target=propertyprocokeDetails, args=([threads]))
            t.start()
        elif websiteName=='propertypro.co.ug':
            t = threading.Thread(target=propertyprocougDetails, args=([threads]))
            t.start()
        elif websiteName=='propertypro.co.zw':
            t = threading.Thread(target=propertyprocozwDetails, args=([threads]))
            t.start()
        elif websiteName=='propertypro.ng':                
            t = threading.Thread(target=propertyprongDetails, args=([threads]))
            t.start()
        elif websiteName=='prophuntgh.com':
            t = threading.Thread(target=prophuntDetails, args=([threads]))
            t.start()
        elif websiteName=='real-estate-tanzania.beforward.jp':                
            t = threading.Thread(target=realestatetanzaniaDetails, args=([]))
            t.start()
        elif websiteName=='booking.com':
            t = threading.Thread(target=bookingDetails, args=([threads]))
            t.start()            
        if backStatus:
            continue
        

    elif downloadData:
        backStatus=False
        print('Download data')
        layout = [
            [sg.Text('Select Database',font=('Poppins',14))],
            [sg.Combo(options, default_value=options[0])],
            [sg.Text('The csv file will be generated where the exe file is located!',font=('Poppins',10),key='-TEXT1-')],
            [sg.Button('Get CSV',key='-TEXT2-',image_data=getOut(100,40),button_color=buttonColor,border_width=0)],
            [sg.Button('Download combined data',key='-TEXT3-',image_data=getOut(250,40),button_color=buttonColor,border_width=0)],
            [sg.Button('Back',font=('Poppins',12),image_data=getOut(100,40),button_color=buttonColor,border_width=0,pad=((0, 0), (30, 0)))],
        ]

        window = sg.Window('Rehani LLC Scraper', layout,size=(400, 300),element_justification="center",text_justification="center",resizable=True,font=('Poppins',12),icon='./logo.ico')

        while True:
            event, values = window.read()
            if event=='Back':
                window.close()
                backStatus=True
                break
            elif event == '-TEXT2-':
                database=values[0]
                databaseName=databaseKeys[database]          
                t = threading.Thread(target=makeCsv, args=([databaseName]))
                t.start()
                window.close()
                break
            elif event == '-TEXT3-':
                databaseName='rehaniAIData'
                database='rehaniAIData'
                t = threading.Thread(target=downloadCombinedData, args=(['rehaniAIData']))
                t.start()
                window.close()
                break
            elif event in (sg.WIN_CLOSED, 'Exit'):
                database=None
                break  
        if backStatus:
            continue

        if database!=None:        
            sg.PopupTimed(f'The data for {databaseName} is being collecting!', title='Rehani LLC Scraper',auto_close_duration=5,button_type=5,font=('Poppins', 14))

            
    elif scraperStatus:
        print('Check status of scrapers')
        read_db()
        headings = ['Website', 'Last Ran Date','Last Ran Time','Successful Completion Date','Successful Completion Time','Status']
        statusDisplayList=[]
        for idx,instance in enumerate(list(db.values())):
            instance.insert(0,list(db.keys())[idx])
            instance[2]=instance[2].replace(';',':')
            instance[4]=instance[4].replace(';',':')
            statusDisplayList.append(instance)

        layout = [
            [sg.Table(statusDisplayList, headings=headings, justification='center', key='-TABLE-',font=('Poppins'))],
            [sg.Button('Back',font=('Poppins',12),image_data=getOut(100,40),button_color=buttonColor,border_width=0,pad=((0, 0), (50, 0)))],
        ]

        window = sg.Window('Rehani LLC Scraper', layout,element_justification="center",text_justification="center",resizable=True,font=('Poppins',12),icon='./logo.ico')

        while True:
            event, values = window.read()
            if event=='Back':
                window.close()
                backStatus=True
                break
            elif event in (sg.WIN_CLOSED, 'Exit'):
                break        
        if backStatus:
            continue         
    else:
        pass
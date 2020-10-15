#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Oct  8 20:49:19 2020

@author: suhasmahesh
"""

import requests
from bs4 import BeautifulSoup
import time
import csv

varnas = 'अआइईउऊऋऌऍऎएऐऑऒओऔकखगघङचछजझञटठडढणतथदधनऩपफबभमयरऱलळऴवशषसह'
test_varnas ='ಙ'

for varna in varnas:
    print ('************************************************************')
    print ('Now we start with ' + varna)
    test_URL = 'https://bharatavani.in/dictionary-surf/?did=249&letter='+varna+'&language=Hindi&page=1'
    page = requests.get(test_URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    result = soup.find("div", {"class":"dnavigation dddddd"})
    children = result.findChildren()
    if not children:
        print ('The last page for '+varna+' is 0')
        number_of_pages = 1
        
    else:
        print ('The last page for '+varna+' is:',children[-2].text) 
        number_of_pages =  int(children[-2].text)
    
    for page_number in range(1,number_of_pages+3): #3 is a safety factor since the website is not well constructed.
        
        with open(varna+r'_awadhikosha.csv', 'a') as f:
            writer = csv.writer(f)
            writer.writerow(['page',str(page_number)])
            
        time.sleep(3.0) #Delay to avoid being blocked. Not important; website doesn't seem smart.
        print ('***We are on page'+str(page_number)+'***')
        URL = 'https://bharatavani.in/dictionary-surf/?did=249&letter='+varna+'&language=Hindi&page='+str(page_number)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        
        event_containers = soup.find_all('div', class_ = "oneword")
        for tag in event_containers: 
            print(tag.h4.text + '\t' + tag.h5.text)
        
            with open(varna+r'_awadhikosha.csv', 'a') as f:
                writer = csv.writer(f)            
                writer.writerow([tag.h4.text,tag.h5.text])
            #word_list.append([tag.h4.text,tag.h5.text])
            
    

# for url_no in range(1,2):
    

#     URL = 'https://bharatavani.in/dictionary-surf/?did=64&letter=ಅ&language=Kannada&page='+str(url_no)
#     page = requests.get(URL)
    
#     soup = BeautifulSoup(page.text, 'html.parser')
    
#     event_containers = soup.find_all('div', class_ = "oneword")
#     result = soup.find("div", {"class":"dnavigation dddddd"})
#     children = result.findChildren()
#     print (children[-2].text)

    # for tag in event_containers:
    #     print(tag.h4.text + '\t' + tag.h5.text)    
    

# results = soup.find(id='entry-content')
# print (results)

#wriitern by sim 

import requests
from bs4 import BeautifulSoup
from datetime import date
import pandas as pd
import numpy as np

Listofcountry = ['uk','hk','ca','au']
ListofJobs=['Data%20Analyst','Data%20Engineer','Data%20Scientist','Business%20Intelligence','Data%20Analytics',"Data%20Consultant"]

listofdata=[]

#scraping the total no.of jobs opening by different countries and jobs

for i in Listofcountry:
    for j in ListofJobs:
        URL = f'https://{i}.indeed.com/jobs?q={j}&start=0'
        page = requests.get(URL)
        soup = BeautifulSoup(page.content,'html.parser')
        numberofjobs=soup.find('div',attrs={'id':'searchCountPages'}).text.strip()
        data={
            'Country': i,
            'NoOfJobs': numberofjobs,
            'JobTitle': j,
            'Date': date.today().strftime('%d/%m/%Y')
        }
        listofdata.append(data)  

print(listofdata)

#Transform to dataframe for easy processing
overview=pd.DataFrame(listofdata)

#Data cleansing 
overview["NoOfJobs"] = overview["NoOfJobs"].str.slice(10,-5)
overview["JobTitle"]=overview["JobTitle"].str.replace("%20"," ")
overview["Country"]=overview["Country"].replace({"uk":"United Kingdom","hk":"Hong Kong","ca":"Canada","au":"Australia"})

print(overview)

#export as csv
overview.to_csv('overview.csv')

#Scraping the details job list for deeper analysis

jobdetails=[]

#def extract(page):
for i in Listofcountry:
    for j in ListofJobs:
      for k in range(0,200,10):
          URL = f'https://{i}.indeed.com/jobs?q={j}&start={k}'
          page = requests.get(URL)
          soup = BeautifulSoup(page.content,'html.parser')
          divs=soup.find_all('div',class_ = 'job_seen_beacon')
          for item in divs:
            company = item.find('span',class_='companyName').text.strip()
            title = item.find('h2',class_='jobTitle').text.strip()
            companylocation=item.find('div',class_='companyLocation').text.strip()
            salarydivs=item.find('div',class_='salary-snippet')
            try:
              salary=salarydivs.find('span').text.strip()
            except:
              salary=''
            
            detail={'Country':i,
                    'Keyword':j,
                    'Company':company,
                    'Title':title,
                    'Location':companylocation,
                    'Salary':salary
            }
            
            jobdetails.append(detail)
          

#input the total page of jobs u want to extract
#for i in range(0,20,10):
 # extract(i)

#Transform to dataframe
details=pd.DataFrame(jobdetails)

#remove deuplicates
details=details.drop_duplicates()

#Data cleansing 
details["Keyword"]=details["Keyword"].str.replace("%20"," ")
details["Country"]=details["Country"].replace({"uk":"United Kingdom","hk":"Hong Kong","ca":"Canada","au":"Australia"})

details["strindex"]=details["Salary"].str.find("-")
details["measureindex"]=details["Salary"].str.find("a")

#Salary component
details["SalaryFrom1"]=details.apply(lambda x : x['Salary'][1:x["strindex"]-1],1)
details["SalaryFrom2"]=details.apply(lambda x : x['Salary'][1:x["measureindex"]],1)
details["SalaryTo1"]=details.apply(lambda x: x['Salary'][x["strindex"]+3:x["measureindex"]],1)
details["SalaryMeasure"]=details.apply(lambda x: x["Salary"][x["measureindex"]:],1)

details["SalaryFrom"]=np.where(details["strindex"] == -1 , details["SalaryFrom2"],details["SalaryFrom1"])
details["SalaryTo"]=np.where(details["strindex"] == -1, details["SalaryFrom2"],details["SalaryTo1"])

def currency_lab (details):
    if details["Country"] == "United Kingdom" : 
        return 'Pounds'
    if details["Country"] == "Australia":
        return 'Austalian Dollars'
    if details["Country"] == "Hong Kong":
        return "Hong Kong Dollars"
    if details["Country"] == 'Canada':
        return "Canadian Dollars"

details["Currency"]= details.apply(lambda details: currency_lab(details),axis=1 )

#Location component 
details["Remote?"]=details["Location"].str.find("Remote")
details["HybridRemote?"]=details["Location"].str.find("Hybrid remote")
details["PositionofIN"]=details["Location"].str.find(" in ")
details["Location2"]=details.apply(lambda x : x['Location'][x["PositionofIN"]+4:],1)
details["FinalLocation"]=np.where(details["PositionofIN"] == -1 , details["Location"],details["Location2"])

details["Remote?"]=np.where(details["Remote?"] == -1 , False , True)
details["HybridRemote?"]=np.where(details["HybridRemote?"] == -1 ,False, True)

final_details = details[["Keyword","Country","Company","Title","Remote?","HybridRemote?","Location","FinalLocation","SalaryFrom","SalaryTo","Currency","SalaryMeasure"]]

#export as csv
final_details.to_csv('details.csv')



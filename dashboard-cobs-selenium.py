#!/usr/local/bin/python3.9
##home/acelle/cobs_python/cobs-env/bin/python3

# DOWNLOADS ONE file with all beneficiaries from the platform using selenium and clicks to a specific path
# Next, it uploads the newest file within that path to UPLOAD_SHEET in origin_sheet

import gspread
from oauth2client.service_account import ServiceAccountCredentials
#from apiclient.discovery import build
#import mysql.connector
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd
import requests

#from webdriver_manager.chrome import ChromeDriverManager
#from selenium.webdriver.chrome.service import Service
#from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from chromedriver_py import binary_path
import time
import os
import shutil
import config.cobsDB as cobsDB

def import_xls2gs(filename,origin_sheet,ws,destination_sheet):
	df = pd.read_excel(filename,sheet_name=origin_sheet) #read input file
	sh=ws.worksheet(destination_sheet) #select worksheet
	sh.clear() #delete previous data
	set_with_dataframe(sh,df) #write new data


# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('../client_secret_interpetrol.json', scope)
client = gspread.authorize(creds)




print(datetime.now(),': COBS DASHBOARD')
ws = client.open_by_key(cobsDB.file_id)
#df = pd.read_excel('/Users/acelle/Downloads/M12.xlsx',sheet_name='Sheet1') #read input file
#sh=ws.worksheet('TEST') #select worksheet
#sh.clear() #delete previous data
#set_with_dataframe(sh,df) #write new data
options = Options()

options.add_experimental_option("prefs", {
  "download.default_directory": cobsDB.path
  })
options.add_argument("--start-maximized")
options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36")
options.add_argument("--headless")
svc = webdriver.ChromeService(executable_path=binary_path)
driver = webdriver.Chrome(service=svc,options=options)

driver.get("https://www.cobsandcogs.cl/admin/login")
driver.find_element("name","username").send_keys(cobsDB.username)
driver.find_element("name","password").send_keys(cobsDB.password)
driver.find_element("class name", "btn-lg").click()


# wait the ready state to be complete
WebDriverWait(driver=driver, timeout=10).until(
    lambda x: x.execute_script("return document.readyState === 'complete'")
)
error_message = "Incorrect username or password."
# get the errors (if there are)
errors = driver.find_elements("css selector", ".flash-error")
# print the errors optionally
# for e in errors:
#     print(e.text)
# if we find that error message within errors, then login is failed
if any(error_message in e.text for e in errors):
    print("[!] Login failed")
else:
    print("[+] Login successful")


driver.get("https://www.cobsandcogs.cl/admin/reports/activities/report/14")
driver.find_element("name","filters[55]").send_keys('01/01/2024')
driver.find_element("name","filters[56]").send_keys('31/12/2024')
driver.maximize_window()
driver.implicitly_wait(10)
#driver.find_element("class name", "fa-search").send_keys(Keys.RETURN)
driver.find_element(By.PARTIAL_LINK_TEXT,"Buscar").send_keys(Keys.RETURN)
driver.implicitly_wait(10)
time.sleep(5)
driver.find_element(By.PARTIAL_LINK_TEXT,"Exportar").click()
driver.find_element(By.PARTIAL_LINK_TEXT,".xlsx").click()
#send_keys(Keys.RETURN)
time.sleep(15)
#driver.implicitly_wait(10)


filename = max([cobsDB.path + "/" + f for f in os.listdir(cobsDB.path)],key=os.path.getctime)
#shutil.move(filename,os.path.join(path,r"newfilename.ext")) #CHANGE NAME
print("New File to be uploaded to sheet:",cobsDB.upload_sheet)
print(filename)

import_xls2gs(filename,'Sheet1',ws,cobsDB.upload_sheet)
exit()

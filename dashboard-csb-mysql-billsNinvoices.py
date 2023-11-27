#!/usr/local/bin/python3.9
###home/acelle/cobs_python/cobs-env/bin/python3
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
import mysql.connector
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.csbDB as csbDB

def log_begin(wbook,rownum):
	begincolumn=5
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,begincolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def log_end(wbook,rownum):
	endcolumn=7
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,endcolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def import_mysql2gas(db,sqlstmt,ws,sheetname,rowlog):
	#start logging
	log_begin(ws,rowlog)
	#getdata and write to GAS
	cursor = db.cursor()
	cursor.execute(sqlstmt)
#	results=cursor.execute(sqlstmt,multi=True)
	sql_data=DataFrame(cursor.fetchall())
	sql_data.columns=cursor.column_names
	sh=ws.worksheet(sheetname)
	sh.clear()
	set_with_dataframe(sh,sql_data)
	cursor.close()
	#end logging
	log_end(ws,rowlog)

try:
	mydb = mysql.connector.connect(host=csbDB.dbhost,user=csbDB.dbuser,password=csbDB.dbpassword,database=csbDB.dbname)
except mysql.connector.Error as err:
	print("Error code:", err.errno)
	print("SQLSTATE value:", err.sqlstate)
	print("Error message:", err.msg)
	print("Error:", err)
	print("Error:", str(err))


# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
client = gspread.authorize(creds)
file_id = '1VVfSDKIRGJwaMGMU3DCJWkcy41vVJlL_meddISUnTLQ'

print(datetime.now(),': CSB Bills N Invoices DASHBOARD')
# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)

sql_invoices = "SELECT rp.name AS client,    rc.name AS vessel,ai.name AS codigo,ai.* FROM account_invoice ai JOIN res_partner rp  ON ai.partner_id = rp.id JOIN res_company rc  ON ai.company_id = rc.id WHERE ai.type ='out_invoice' AND ai.name is NOT null ORDER BY ai.date desc,ai.name desc"
sql_bills_residual = "select ai.*,rp.name as supplier, rc.name as company from account_invoice ai join res_partner rp on ai.partner_id = rp.id join res_company rc on ai.company_id = rc.id where ai.type ='in_invoice'  and residual_company_signed <>0 order by ai.date desc,ai.partner_id desc,ai.name desc"
sql_bills = "select ai.*,rp.name as supplier, rc.name as company from account_invoice ai join res_partner rp on ai.partner_id = rp.id join res_company rc on ai.company_id = rc.id where ai.type ='in_invoice' order by ai.date desc,ai.partner_id desc,ai.name desc"
sql_bills_ip_administracion = "SELECT ai.*,rp.name AS supplier,    rc.name AS company FROM account_invoice ai JOIN res_partner rp  ON ai.partner_id = rp.id JOIN res_company rc  ON ai.company_id = rc.id WHERE ai.type ='in_invoice'  AND ai.partner_id = 154 and ai.state <> 'cancel' ORDER BY ai.date desc,ai.partner_id desc,ai.name desc"
sql_bills_csb_administracion = "SELECT ai.*,rp.name AS supplier,    rc.name AS company FROM account_invoice ai JOIN res_partner rp  ON ai.partner_id = rp.id JOIN res_company rc  ON ai.company_id = rc.id WHERE ai.type ='in_invoice'  AND ai.partner_id = 245 and ai.state <> 'cancel' ORDER BY ai.date desc,ai.partner_id desc,ai.name desc"

import_mysql2gas(mydb,sql_invoices,ws,"qINVOICES",11)
import_mysql2gas(mydb,sql_bills_residual,ws,"qBILLSRESIDUAL",12)
import_mysql2gas(mydb,sql_bills,ws,"qBILLS",13)
import_mysql2gas(mydb,sql_bills_ip_administracion,ws,"qBILLS_IP_ADMINISTRACION",14)

mydb.close()


exit()


# PDF export commented out as it prints the entire file and not single sheets
#for single sheest I need to copy the sheet to a temp file and export that file
# EASIER to command this from google sheet itself (using copyTO method)
# The mailing lists should be managed in mailgun directly

# setup authorization
DRIVE = build('drive', 'v4', credentials=creds)

data = DRIVE.files().export(fileId=file_id, mimeType="application/pdf").execute()
if data:
    filename = 'your-file-name.pdf'
    with open(filename, 'wb') as pdf_file:
        pdf_file.write(data)

#request = drive_service.files().export_media(fileId=file_id,mimeType='application/pdf')
#fh = io.BytesIO()
#downloader = MediaIoBaseDownload(fh, request)
#done = False
#while done is False:
 #   status, done = downloader.next_chunk()

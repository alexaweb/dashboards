#!/usr/local/bin/python3.9
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
#import mysql.connector
#import pyodbc
import pymssql
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.conacsaDB as DB
import config.common as interpetrol

def log_begin(wbook,rownum):
	begincolumn=5
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,begincolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def log_end(wbook,rownum):
	endcolumn=7
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,endcolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def import_sqlserver2gas(db,sqlstmt,ws,sheetname,rowlog):
	#start logging
	log_begin(ws,rowlog)
	#getdata and write to GAS
	cursor = db.cursor()
	cursor.execute(sqlstmt)
	sql_data=DataFrame.from_records(cursor.fetchall(),columns=[desc[0] for desc in cursor.description])
	sh=ws.worksheet(sheetname)
	sh.clear()
	set_with_dataframe(sh,sql_data)
	cursor.close()
	#end logging
	log_end(ws,rowlog)

def import_sp2gas(db,sp,ws,sheetname,rowlog):
	#start logging
	log_begin(ws,rowlog)
	#getdata and write to GAS
	cursor = db.cursor()

	#RUN FOR CURRENT YEAR
	ano = datetime.now().strftime("%Y")
	cursor.callproc(sp,[ano])

	for i in cursor.stored_results(): results = i.fetchall()
	df=pd.DataFrame(results,columns=['fechadb','purchased_entity','ano','type','monto_pagado','monto_inscrito','inscritos','miembros','medio_de_pago'])
#	sql_data.columns=(c for c in cursor.description)
	sh=ws.worksheet(sheetname)
	sh.clear()
	set_with_dataframe(sh,df)
	cursor.close()
	#end logging
	log_end(ws,rowlog)

#driver = '{ODBC Driver 17 for SQL Server}'
#sql_connection_string = 'DRIVER='+driver+';SERVER='+interpetrolDB.dbhost+';DATABASE='+interpetrolDB.dbname+';UID='+interpetrolDB.dbuser+';PWD='+interpetrolDB.dbpassword
print(datetime.now(),': Interpetrol ctas ctes DASHBOARD')
#mydb = pyodbc.connect(sql_connection_string)
mydb = pymssql.connect(DB.dbhost,DB.dbuser,DB.dbpassword,DB.dbname)
creds = ServiceAccountCredentials.from_json_keyfile_name(interpetrol.jsoninterpetrol, interpetrol.scope)
client = gspread.authorize(creds)

# use creds to create a client to interact with the Google Drive API
#scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
#creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
#client = gspread.authorize(creds)
file_id = '1ViDL3B0HiWfBy9KuUdZyyd097W6IRrGbz2HEl9nq1ks'

sql_conacsa_cetra = "SELECT cwcpbte.CpbAno, cwmovim.CodAux, cwmovim.PctCod, cwmovim.TtdCod, cwttdoc.DesDoc, cwmovim.NumDoc, cwmovim.MovFe, cwmovim.MovFv, cwmovim.MovTipDocRef, cwmovim.MovNumDocRef, cwcpbte.CpbNum, cwcpbte.CpbFec, cwmovim.MovDebe, cwmovim.MovHaber, cwmovim.MovGlosa, cwcpbte.CpbEst FROM softland.cwcpbte cwcpbte, softland.cwmovim cwmovim, softland.cwttdoc cwttdoc WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.TtdCod = cwttdoc.CodDoc AND ((cwcpbte.CpbNum<>'00000000') AND (cwmovim.CodAux='77214142') AND (cwmovim.PctCod='1-1-10-006') AND (cwcpbte.CpbEst='V'))"



# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)

import_sqlserver2gas(mydb,sql_conacsa_cetra,ws,"qConacsa-Cetra",12)
#import_sqlserver2gas(mydb,sql_ip_csb,ws,"qInterpetrol-CSB",9)
#import_sqlserver2gas(mydb,sql_gastosdetalle,ws,"qGASTOSDETALLE",11)


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

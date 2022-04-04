#!/usr/local/bin/python3.9
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.discovery import build
#import mysql.connector
import pyodbc
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.carsiDB as carsiDB

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

driver = '{ODBC Driver 17 for SQL Server}'
sql_connection_string = 'DRIVER='+driver+';SERVER='+carsiDB.dbhost+';DATABASE='+carsiDB.dbname+';UID='+carsiDB.dbuser+';PWD='+carsiDB.dbpassword
print(datetime.now(),': Carsi DASHBOARD')
mydb = pyodbc.connect(sql_connection_string)

# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
client = gspread.authorize(creds)
file_id = '1eGrfT8VyxJ7LXZWZmbHyK6VvF3aT-qWac7aph3FJD7Q'

sql_saldos = "SELECT cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', Sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'SALDO', cwmovim.MovGlosa FROM softland.cwcpbte, softland.cwmovim, softland.cwpctas WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.PctCod = cwpctas.PCCODI AND cwcpbte.CpbEst='V' and (cwcpbte.CpbMes<>'00') GROUP BY cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes, cwmovim.CpbAno, cwmovim.MovGlosa HAVING cwpctas.pccodi like '1-1-10-%' or cwpctas.pccodi like '2-1-06-%' or cwpctas.pccodi = '1-1-04-001' or cwpctas.pccodi = '1-1-04-000'  or cwpctas.pccodi = '1-1-04-002' or cwpctas.pccodi = '1-1-05-000' or cwpctas.pccodi = '1-1-04-006'"

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)

#import_sqlserver2gas(mydb,sql_saldos,ws,"qTEST",10)
import_sqlserver2gas(mydb,sql_saldos,ws,"qSALDOS",10)


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

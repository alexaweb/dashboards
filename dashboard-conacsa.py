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

import config.conacsaDB as conacsaDB

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
sql_connection_string = 'DRIVER='+driver+';SERVER='+conacsaDB.dbhost+';DATABASE='+conacsaDB.dbname+';UID='+conacsaDB.dbuser+';PWD='+conacsaDB.dbpassword
#print(datetime.now(),': ',sql_connection_string)
print(datetime.now(),': CONACSA DASHBOARD')
mydb = pyodbc.connect(sql_connection_string)


# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
client = gspread.authorize(creds)
file_id = '1XvaocgAR3rlnOSo9yCd2UBVV2f05Ac_M4juVCSc6fcA'

sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.DgaCod like '03%'--cwmovim.PctCod ='5-2-01-002' AND (cwcpbte.CpbEst='V')"
sql_gastosdetalle = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod, cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod"
sql_ingresos = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', -cwmovim.MovDebe+cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descipcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.PctCod like '4%' AND (cwcpbte.CpbEst='V')"
sql_ventas = "SELECT iw_gsaen.Tipo, iw_gsaen.Folio, (iw_gsaen.NetoAfecto + iw_gsaen.NetoExento) as Netos, month(iw_gsaen.Fecha) as Mes, year(iw_gsaen.Fecha) as Year, cwtauxi.CodAux, cwtauxi.NomAux FROM softland.iw_gsaen, softland.cwtauxi WHERE cwtauxi.CodAux = iw_gsaen.CodAux AND ((iw_gsaen.Estado='V') AND (iw_gsaen.Tipo='F' Or iw_gsaen.Tipo='N' Or iw_gsaen.Tipo='D'))"
sql_saldos = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', cwmovim.pctcod as 'codigo' FROM softland.cwcpbte, softland.cwmovim where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and cwmovim.CpbMes <> '00' AND cwcpbte.CpbEst='V' group by cwmovim.pctcod, cwmovim.CpbMes, cwmovim.CpbAno"
sql_enel = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod, cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwmovim.cccod = '03-32'"
sql_enap = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod, cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwmovim.cccod = '03-50'"
sql_conacsa = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod, cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwmovim.cccod = '03-45'"

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)

import_sqlserver2gas(mydb,sql_enap,ws,"qENAP",10)
import_sqlserver2gas(mydb,sql_rem,ws,"qREM",11)
import_sqlserver2gas(mydb,sql_gastosdetalle,ws,"qGASTOSDETALLE",12)
import_sqlserver2gas(mydb,sql_ingresos,ws,"qINGRESOS",13)
import_sqlserver2gas(mydb,sql_ventas,ws,"qVENTAS",14)
import_sqlserver2gas(mydb,sql_saldos,ws,"qSALDOS",15)
import_sqlserver2gas(mydb,sql_enel,ws,"qENEL",16)
import_sqlserver2gas(mydb,sql_conacsa,ws,"qCONACSA",17)


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

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
import pymssql

import config.indeproipDB as indeproipDB
import config.interpetrolDB as interpetrolDB
import config.conacsaDB as conacsaDB
import config.ipmtDB as ipmtDB
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



driver = '{ODBC Driver 17 for SQL Server}'
#sql_connection_string = 'DRIVER='+driver+';SERVER='+indeproipDB.dbhost+';DATABASE='+indeproipDB.dbname+';UID='+indeproipDB.dbuser+';PWD='+indeproipDB.dbpassword
print(datetime.now(),': ENAP DASHBOARD')
#mydb = pyodbc.connect(sql_connection_string)

# use creds to create a client to interact with the Google Drive API
#scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
#creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
#client = gspread.authorize(creds)
#file_id_enap = '16-4A0al78YgloNqVxki8s1aqefv5u6RMtYBwbDb_9tw'
creds = ServiceAccountCredentials.from_json_keyfile_name(interpetrol.jsoninterpetrol, interpetrol.scope)
client = gspread.authorize(creds)
ws = client.open_by_key(ipmtDB.file_id_enap)

mydb = pymssql.connect(indeproipDB.dbhost,indeproipDB.dbuser,indeproipDB.dbpassword,indeproipDB.dbname)
sql_ingresos = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', -cwmovim.MovDebe+cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descipcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.PctCod like '4%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.DgaCod like '03%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
sql_ventas = "SELECT iw_gsaen.Tipo, iw_gsaen.Folio, (iw_gsaen.NetoAfecto + iw_gsaen.NetoExento) as Netos, month(iw_gsaen.Fecha) as Mes, year(iw_gsaen.Fecha) as Year, cwtauxi.CodAux, cwtauxi.NomAux FROM softland.iw_gsaen, softland.cwtauxi WHERE cwtauxi.CodAux = iw_gsaen.CodAux AND ((iw_gsaen.Estado='V') AND (iw_gsaen.Tipo='F' Or iw_gsaen.Tipo='N' Or iw_gsaen.Tipo='D'))"
sql_gastosdetallecodigo = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod,cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod and cwmovim.cpbano > 2019 and cwmovim.CcCod = '03-50' and cwmovim.DgaCod not like '03%'"

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.

import_sqlserver2gas(mydb,sql_ingresos,ws,"qINGRESOS",10)
import_sqlserver2gas(mydb,sql_rem,ws,"qREM",11)
import_sqlserver2gas(mydb,sql_gastosdetallecodigo,ws,"qGASTOSdetalleCODIGO",12)
#import_sqlserver2gas(mydb,sql_ventas,ws,"qVENTAS",12)
#import_sp2gas(mydb,'sp_cobs_resumen_evolucion',ws,"qEVOLUCION",12)
#import_mysql2gas(mydb,"select * from view_cobs_detalle_membresias",ws,"qMEMBRESIAS",14)
mydb.close()

## INFO INTERPETROL
#sql_connection_string = 'DRIVER='+driver+';SERVER='+interpetrolDB.dbhost+';DATABASE='+interpetrolDB.dbname+';UID='+interpetrolDB.dbuser+';PWD='+interpetrolDB.dbpassword
#mydb = pyodbc.connect(sql_connection_string)
mydb = pymssql.connect(interpetrolDB.dbhost,interpetrolDB.dbuser,interpetrolDB.dbpassword,interpetrolDB.dbname)
sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.dgacod like '03%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
sql_gastosdetallecodigo = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod,cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod and cwmovim.cpbano > 2019 and cwmovim.CcCod = '03-50' and cwmovim.DgaCod not like '03%'"
import_sqlserver2gas(mydb,sql_rem,ws,"qREM-IP",14)
import_sqlserver2gas(mydb,sql_gastosdetallecodigo,ws,"qGASTOSdetalleCODIGO-IP",15)
mydb.close()

# INFO IPMT
#sql_connection_string = 'DRIVER='+driver+';SERVER='+ipmtDB.dbhost+';DATABASE='+ipmtDB.dbname+';UID='+ipmtDB.dbuser+';PWD='+ipmtDB.dbpassword
#mydb = pyodbc.connect(sql_connection_string)
mydb = pymssql.connect(ipmtDB.dbhost,ipmtDB.dbuser,ipmtDB.dbpassword,ipmtDB.dbname)
import_sqlserver2gas(mydb,sql_ingresos,ws,"qINGRESOS-IPMT",18)
import_sqlserver2gas(mydb,sql_rem,ws,"qREM-IPMT",19)
import_sqlserver2gas(mydb,sql_gastosdetallecodigo,ws,"qGASTOSdetalleCODIGO-IPMT",20)
mydb.close()


#  INFO CONACSA
#sql_connection_string = 'DRIVER='+driver+';SERVER='+conacsaDB.dbhost+';DATABASE='+conacsaDB.dbname+';UID='+conacsaDB.dbuser+';PWD='+conacsaDB.dbpassword
#mydb = pyodbc.connect(sql_connection_string)
mydb = pymssql.connect(conacsaDB.dbhost,conacsaDB.dbuser,conacsaDB.dbpassword,conacsaDB.dbname)
sql_gastosdetallecodigo = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod,cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod and cwmovim.cpbano > 2019 and cwmovim.CcCod = '03-50'"
import_sqlserver2gas(mydb,sql_gastosdetallecodigo,ws,"qGASTOSdetalleCODIGO-CONACSA",16)
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

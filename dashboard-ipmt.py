#!/usr/local/bin/python3.9
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pymssql
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.ipmtDB as DB
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


#BEGIN
print(datetime.now(),': IPMT DASHBOARD')
mydb = pymssql.connect(DB.dbhost,DB.dbuser,DB.dbpassword,DB.dbname)


# use creds to create a client to interact with the Google Drive API
creds = ServiceAccountCredentials.from_json_keyfile_name(interpetrol.jsoninterpetrol, interpetrol.scope)
client = gspread.authorize(creds)

#SQL SERVER QUERIES
sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.DgaCod like '03%'--cwmovim.PctCod ='5-2-01-002' AND (cwcpbte.CpbEst='V')"
sql_gastosdetalle = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod, cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod"
sql_ingresos = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', -cwmovim.MovDebe+cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descipcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.PctCod like '4%' AND (cwcpbte.CpbEst='V')"
sql_ventas = "SELECT iw_gsaen.Tipo, iw_gsaen.Folio, (iw_gsaen.NetoAfecto + iw_gsaen.NetoExento) as Netos, month(iw_gsaen.Fecha) as Mes, year(iw_gsaen.Fecha) as Year, cwtauxi.CodAux, cwtauxi.NomAux FROM softland.iw_gsaen, softland.cwtauxi WHERE cwtauxi.CodAux = iw_gsaen.CodAux AND ((iw_gsaen.Estado='V') AND (iw_gsaen.Tipo='F' Or iw_gsaen.Tipo='N' Or iw_gsaen.Tipo='D'))"
sql_saldos = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', cwmovim.pctcod as 'codigo' FROM softland.cwcpbte, softland.cwmovim where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and cwmovim.CpbMes <> '00' AND cwcpbte.CpbEst='V' group by cwmovim.pctcod, cwmovim.CpbMes, cwmovim.CpbAno"



# HASTA ACA 20:27jul21 - debo incorporar y revisar estos cambios para luego hacer las páginas en google sheets y probar
# para luego hacer el modelo
sql_ingresos = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', -cwmovim.MovDebe+cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descipcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.PctCod like '4%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.DgaCod like '03%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
sql_gastosdetallecodigo = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod,cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod and cwmovim.cpbano > 2019 and cwmovim.CcCod = '03-50' and cwmovim.DgaCod not like '03%'"
# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(DB.file_id)
#ws_enap = client.open_by_key('1Dy2sSifECZjxlK-R393GtfVR7jTkn-XZ2V8RYoLmEK0')

#EXECUTE SQL SERVER QUERIES

import_sqlserver2gas(mydb,sql_rem,ws,"qREM",11)
print(datetime.now(),': IPMT DASHBOARD: REM')
import_sqlserver2gas(mydb,sql_gastosdetalle,ws,"qGASTOSDETALLE",12)
print(datetime.now(),': IPMT DASHBOARD: GASTOS DETALLE')
import_sqlserver2gas(mydb,sql_ingresos,ws,"qINGRESOS",13)
print(datetime.now(),': IPMT DASHBOARD: INGRESOS')
import_sqlserver2gas(mydb,sql_ventas,ws,"qVENTAS",14)
print(datetime.now(),': IPMT DASHBOARD: VENTAS')
import_sqlserver2gas(mydb,sql_saldos,ws,"qSALDOS",15)
print(datetime.now(),': IPMT DASHBOARD: SALDOS')


## ESTO LO TRAJE DE DASHBOARD-ENAP
#sql_ingresos = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', -cwmovim.MovDebe+cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descipcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.PctCod like '4%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
#sql_rem = "SELECT cwmovim.DgaCod as 'dgacod', cwcpbte.CpbMes as 'mes', cwcpbte.CpbAno as 'ano', cwcpbte.CpbNum as 'cpbnum', cwcpbte.CpbTip as 'cpbtip', cwcpbte.CpbNui as 'cpbnui', cwmovim.PctCod as 'pctcod', cwmovim.MovGlosa as 'glosa', cwmovim.MovDebe-cwmovim.MovHaber AS 'SALDO', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.DgaCod like '03%' AND (cwcpbte.CpbEst='V') and cwmovim.CcCod = '03-50'"
#sql_ventas = "SELECT iw_gsaen.Tipo, iw_gsaen.Folio, (iw_gsaen.NetoAfecto + iw_gsaen.NetoExento) as Netos, month(iw_gsaen.Fecha) as Mes, year(iw_gsaen.Fecha) as Year, cwtauxi.CodAux, cwtauxi.NomAux FROM softland.iw_gsaen, softland.cwtauxi WHERE cwtauxi.CodAux = iw_gsaen.CodAux AND ((iw_gsaen.Estado='V') AND (iw_gsaen.Tipo='F' Or iw_gsaen.Tipo='N' Or iw_gsaen.Tipo='D'))"
#sql_gastosdetallecodigo = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.dgacod,cwtdetg.DesDet as 'descripciondetalle', cwmovim.MovGlosa as 'glosa', cwmovim.CcCod as 'cccod', cwtccos.DescCC as 'descripcioncc', cwpctas.pccodi as 'codigo', cwpctas.pcdesc as 'descripcion' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg, softland.cwpctas where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC and cwpctas.pccodi= cwmovim.pctcod and cwmovim.cpbano > 2019 and cwmovim.CcCod = '03-50' and cwmovim.DgaCod not like '03%'"

#import_sqlserver2gas(mydb,sql_ingresos,ws_enap,"qINGRESOS-IPMT",18)
#import_sqlserver2gas(mydb,sql_rem,ws_enap,"qREM-IPMT",19)
#import_sqlserver2gas(mydb,sql_gastosdetallecodigo,ws_enap,"qGASTOSdetalleCODIGO-IPMT",20)
#mydb.close()

mydb.close()


exit()

#!/usr/local/bin/python3.9
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pymssql
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.thunderboltDB as DB
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
print(datetime.now(),': Thunderbolt Chile DASHBOARD')
mydb = pymssql.connect(DB.dbhost,DB.dbuser,DB.dbpassword,DB.dbname)


# use creds to create a client to interact with the Google Drive API
creds = ServiceAccountCredentials.from_json_keyfile_name(interpetrol.jsoninterpetrol, interpetrol.scope)
client = gspread.authorize(creds)

#SQL SERVER QUERIES
sql_docsemitidos = "select year(cwmovim.movfe) as 'ano', month(cwmovim.movfe) as 'mes', 'ALLENDALE' as 'vessel', cwttdoc.CodDoc, cwmovim.numdoc, cwmovim.codaux, cwtauxi.nomaux, cwmovim.movfe, cwmovim.cpbmes, cwmovim.cpbano, cwmovim.movdebe- cwmovim.movhaber as MONTO_FACTURADO, cwmovim.movglosa, cwttdoc.desdoc from softland.cwcpbte, softland.cwmovim, softland.cwtauxi, softland.cwttdoc where cwmovim.ttdcod = cwttdoc.coddoc and cwtauxi.codaux=cwmovim.codaux and cwmovim.cpbmes<>'00' AND cwcpbte.CpbEst='V' and cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and cwmovim.codaux = '59180540'  order by movfe desc"
sql_cxcextranjeros = "SELECT cwmovim.CcCod, cwmovim.DgaCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovFe, cwmovim.CpbNum, cwmovim.PctCod, cwmovim.codaux, cwmovim.MovGlosa, cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS' FROM softland.cwmovim, softland.cwcpbte where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwcpbte.CpbEst='V' and cwmovim.PctCod='1-1-04-002' and cwmovim.codaux in ('59180540') and cwmovim.cpbmes <>0 order by cwmovim.MovFe desc"
sql_rem = "SELECT g.mes as 'mes', g.ano as 'ano', isnull(sum(g.GASTOS),0) as 'gastos_excentos', isnull(sum(f.NETO_CC_Barco),0) as 'facturado_neto', isnull(sum(g.GASTOS),0) + isnull(sum(f.NETO_CC_Barco),0) as 'gastos_por_facturar', g.vessel as 'vessel', g.cc as 'cc' from (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='5-3-01-005' AND cwcpbte.CpbEst='V' and cwmovim.CcCod='03-40' group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC, cwmovim.CcCod) as g full outer JOIN (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'NETO_CC_Barco', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='4-1-03-007' and cwmovim.CcCod='03-40'  group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC,cwmovim.CcCod) as f on f.ano=g.ano and f.mes=g.mes and f.vessel = g.vessel and f.cc = g.cc group by g.ano, g.mes, g.vessel, g.cc order by g.ano, g.mes"
sql_excentosresumen = "SELECT g.mes as 'mes', g.ano as 'ano', isnull(sum(g.GASTOS),0) as 'gastos_excentos', isnull(sum(f.NETO_CC_Barco),0) as 'facturado_neto', isnull(sum(g.GASTOS),0) + isnull(sum(f.NETO_CC_Barco),0) as 'gastos_por_facturar', g.vessel as 'vessel', g.cc as 'cc' from (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='5-3-01-012' AND cwcpbte.CpbEst='V' and cwmovim.CcCod='03-40' group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC, cwmovim.CcCod) as g full outer JOIN (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'NETO_CC_Barco', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='4-1-03-007' and cwmovim.CcCod='03-40'  group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC,cwmovim.CcCod) as f on f.ano=g.ano and f.mes=g.mes and f.vessel = g.vessel and f.cc = g.cc group by g.ano, g.mes, g.vessel, g.cc order by g.ano, g.mes"
sql_afectosresumen = "SELECT g.mes as 'mes', g.ano as 'ano', isnull(sum(g.GASTOS),0) as 'gastos_afectos', isnull(sum(f.NETO_CC_Barco),0) as 'facturado_neto', isnull(sum(g.GASTOS),0) + isnull(sum(f.NETO_CC_Barco),0) as 'gastos_por_facturar', g.vessel as 'vessel', g.cc as 'cc' from (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='5-3-01-013' and cwmovim.CcCod='03-40'  group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC, cwmovim.CcCod) as g full outer JOIN (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'NETO_CC_Barco', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='4-1-03-003' and (cwmovim.CcCod='03-40')  group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC,cwmovim.CcCod) as f on f.ano=g.ano and f.mes=g.mes and f.vessel = g.vessel and f.cc = g.cc group by g.ano, g.mes, g.vessel, g.cc order by g.ano, g.mes"

sql_ctasctes = "SELECT cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', Sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'SALDO', cwmovim.MovGlosa FROM softland.cwcpbte, softland.cwmovim, softland.cwpctas WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.PctCod = cwpctas.PCCODI AND cwcpbte.CpbEst='V' and cwcpbte.CpbMes<>'00' GROUP BY cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes, cwmovim.CpbAno, cwmovim.MovGlosa HAVING (cwpctas.PCCODI='1-1-05-010')  AND (cwmovim.CodAux='59180540')"

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(DB.file_id)

#EXECUTE SQL SERVER QUERIES
import_sqlserver2gas(mydb,sql_docsemitidos,ws,"qDOCS_EMITIDOS",7)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: DOCS EMITIDOS')
import_sqlserver2gas(mydb,sql_cxcextranjeros,ws,"qCXC_EXTRANJEROS",8)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: CxC EXTRANJEROS')
import_sqlserver2gas(mydb,sql_rem,ws,"qREM",9)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: REMUNERACIONES RESUMEN')
import_sqlserver2gas(mydb,sql_excentosresumen,ws,"qEXCENTOS_RESUMEN",10)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: EXCENTOS RESUMEN')
import_sqlserver2gas(mydb,sql_afectosresumen,ws,"qAFECTOS_RESUMEN",11)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: AFECTOS RESUMEN')
import_sqlserver2gas(mydb,sql_ctasctes,ws,"qCTASCTES",12)
print(datetime.now(),': Thunderbolt Chile DASHBOARD: CTAS CTES')

mydb.close()


exit()

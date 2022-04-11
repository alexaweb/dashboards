#!/usr/local/bin/python3.9
import gspread
from oauth2client.service_account import ServiceAccountCredentials

import pymssql
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from pandas import DataFrame
from datetime import datetime
import pandas as pd

import config.interpetrolcsbDB as DB
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
print(datetime.now(),': INTERPETROL BARCOS Chile DASHBOARD')
mydb = pymssql.connect(DB.dbhost,DB.dbuser,DB.dbpassword,DB.dbname)


# use creds to create a client to interact with the Google Drive API
creds = ServiceAccountCredentials.from_json_keyfile_name(interpetrol.jsoninterpetrol, interpetrol.scope)
client = gspread.authorize(creds)

#SQL SERVER QUERIES
sql_afectosresumen = "SELECT g.mes as 'mes', g.ano as 'ano', isnull(sum(g.GASTOS),0) as 'gastos_afectos', isnull(sum(f.NETO_CC_Barco),0) as 'facturado_neto', isnull(sum(g.GASTOS),0) + isnull(sum(f.NETO_CC_Barco),0) as 'gastos_por_facturar', g.vessel as 'vessel', g.cc as 'cc' from (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND (cwmovim.PctCod='5-3-01-002' or cwmovim.PctCod='5-3-01-010') and ((cwmovim.CcCod='01-11') or (cwmovim.CcCod='01-12') or (cwmovim.CcCod='01-36') or (cwmovim.CcCod='01-37')) group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC, cwmovim.CcCod) as g full outer JOIN (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'NETO_CC_Barco', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='4-1-03-003' and ((cwmovim.CcCod='01-11') or (cwmovim.CcCod='01-12') or (cwmovim.CcCod='01-36') or (cwmovim.CcCod='01-37')) group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC,cwmovim.CcCod) as f on f.ano=g.ano and f.mes=g.mes and f.vessel = g.vessel and f.cc = g.cc group by g.ano, g.mes, g.vessel, g.cc order by g.ano, g.mes"
sql_excentos = "SELECT cwmovim.DgaCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwcpbte.CpbNum, cwcpbte.CpbNui, cwtdetg.DesDet, cwmovim.PctCod, cwmovim.MovGlosa, cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.CcCod, ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.DgaCod = cwtdetg.CodDet AND ((cwmovim.PctCod='5-3-01-005') AND (cwcpbte.CpbEst='V') and cwmovim.cpbmes<>'00') UNION SELECT cwmovim.DgaCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.CpbNum, cwcpbte.CpbNui, cwtdetg.DesDet, cwmovim.PctCod, cwmovim.MovGlosa, cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS', cwmovim.CcCod, ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg WHERE cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC AND (cwmovim.PctCod='5-3-01-009') AND (cwcpbte.CpbEst='V') and cwmovim.cpbmes<>'00'"
sql_imports = "select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc', cwmovim.dgacod FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' and cwcpbte.CpbMes<>'00' AND cwmovim.CcCod = cwtccos.CodiCC and (cwmovim.PctCod='5-3-01-002' or cwmovim.PctCod='5-3-01-010') and ((cwmovim.CcCod='01-11') or (cwmovim.CcCod='01-12') or (cwmovim.CcCod='01-36') or (cwmovim.CcCod='01-37')) and ((cwmovim.dgacod = '05-010') or (cwmovim.dgacod = '05-011')) group by  cwmovim.CpbMes , cwmovim.CpbAno ,  cwtccos.DescCC,cwmovim.CcCod, cwmovim.dgacod order by  ano desc, mes desc"

sql_ctasctes = "SELECT cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', Sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'SALDO', cwmovim.MovGlosa FROM softland.cwcpbte, softland.cwmovim, softland.cwpctas WHERE cwmovim.CpbAno = cwcpbte.CpbAno AND cwmovim.CpbNum = cwcpbte.CpbNum AND cwmovim.AreaCod = cwcpbte.AreaCod AND cwmovim.PctCod = cwpctas.PCCODI AND cwcpbte.CpbEst='V' and cwcpbte.CpbMes<>'00' GROUP BY cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.CpbMes, cwmovim.CpbAno, cwmovim.MovGlosa HAVING ((cwpctas.PCCODI='1-1-10-002') or (cwpctas.PCCODI='1-1-10-005') or (cwpctas.PCCODI='1-1-10-006') or (cwpctas.PCCODI='1-1-10-007') or (cwpctas.PCCODI='1-1-10-008') or (cwpctas.PCCODI='1-1-10-009') or (cwpctas.PCCODI='1-1-10-010') or (cwpctas.PCCODI='1-1-10-011')) AND ((cwmovim.CodAux='103') or (cwmovim.CodAux='101') or (cwmovim.CodAux='11-2') or (cwmovim.CodAux='2013') or (cwmovim.CodAux='2014') or (cwmovim.CodAux='2015') or (cwmovim.CodAux='15')) ";
sql_excentos_resumen = "SELECT g.mes as 'mes', g.ano as 'ano', isnull(sum(g.GASTOS),0) as 'gastos_excentos', isnull(sum(f.NETO_CC_Barco),0) as 'facturado_neto', isnull(sum(g.GASTOS),0) + isnull(sum(f.NETO_CC_Barco),0) as 'gastos_por_facturar', g.vessel as 'vessel', g.cc as 'cc' from (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'GASTOS', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwmovim.CcCod = cwtccos.CodiCC AND (cwmovim.PctCod='5-3-01-009' or cwmovim.PctCod ='5-3-01-005') AND cwcpbte.CpbEst='V' and ((cwmovim.CcCod='01-11') or (cwmovim.CcCod='01-12') or (cwmovim.CcCod='01-36') or (cwmovim.CcCod='01-37')) group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC, cwmovim.CcCod) as g full outer JOIN (select cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', ltrim(right(rtrim(cwtccos.DescCC),7)) as 'vessel', sum(cwmovim.MovDebe-cwmovim.MovHaber) AS 'NETO_CC_Barco', cwmovim.CcCod as 'cc' FROM softland.cwcpbte, softland.cwmovim, softland.cwtccos, softland.cwtdetg where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwmovim.DgaCod = cwtdetg.CodDet AND cwcpbte.CpbEst='V' AND cwmovim.CcCod = cwtccos.CodiCC AND cwmovim.PctCod='4-1-03-007' and ((cwmovim.CcCod='01-11') or (cwmovim.CcCod='01-12') or (cwmovim.CcCod='01-36') or (cwmovim.CcCod='01-37')) group by  cwmovim.CpbMes, cwmovim.CpbAno,  cwtccos.DescCC,cwmovim.CcCod) as f on f.ano=g.ano and f.mes=g.mes and f.vessel = g.vessel and f.cc = g.cc group by g.ano, g.mes, g.vessel, g.cc order by g.ano, g.mes"
sql_documentos_emitidos = "select year(cwmovim.movfe) as 'ano', month(cwmovim.movfe) as 'mes', 'KOREIZ' as 'vessel', cwttdoc.CodDoc, cwmovim.numdoc, cwmovim.codaux, cwtauxi.nomaux, cwmovim.movfe, cwmovim.cpbmes, cwmovim.cpbano, cwmovim.movdebe- cwmovim.movhaber as MONTO_FACTURADO, (cwmovim.movdebe- cwmovim.movhaber)/1.19 as MONTO_NETO,cwmovim.movglosa, cwttdoc.desdoc from softland.cwcpbte, softland.cwmovim, softland.cwtauxi, softland.cwttdoc where cwmovim.ttdcod = cwttdoc.coddoc and cwtauxi.codaux=cwmovim.codaux and cwmovim.cpbmes<>'00' AND cwcpbte.CpbEst='V' and cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and ((cwmovim.codaux = '101') or (cwmovim.codaux = '2014') or (cwtauxi.nomaux like '%KOREIZ%')) UNION select year(cwmovim.movfe) as 'ano', month(cwmovim.movfe) as 'mes', 'SIMEIZ' as 'vessel', cwttdoc.CodDoc, cwmovim.numdoc, cwmovim.codaux, cwtauxi.nomaux, cwmovim.movfe, cwmovim.cpbmes, cwmovim.cpbano, cwmovim.movdebe- cwmovim.movhaber as MONTO_FACTURADO, (cwmovim.movdebe- cwmovim.movhaber)/1.19 as MONTO_NETO,cwmovim.movglosa, cwttdoc.desdoc from softland.cwcpbte, softland.cwmovim, softland.cwtauxi, softland.cwttdoc where cwmovim.ttdcod = cwttdoc.coddoc and cwtauxi.codaux=cwmovim.codaux and cwmovim.cpbmes<>'00' AND cwcpbte.CpbEst='V' and cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and ((cwmovim.codaux = '11-2') or (cwmovim.codaux = '2015') or (cwtauxi.nomaux like '%SIMEIZ%')) UNION select year(cwmovim.movfe) as 'ano', month(cwmovim.movfe) as 'mes', 'CALIPSO' as 'vessel', cwttdoc.CodDoc, cwmovim.numdoc, cwmovim.codaux, cwtauxi.nomaux, cwmovim.movfe, cwmovim.cpbmes, cwmovim.cpbano, cwmovim.movdebe- cwmovim.movhaber as MONTO_FACTURADO,(cwmovim.movdebe- cwmovim.movhaber)/1.19 as MONTO_NETO, cwmovim.movglosa, cwttdoc.desdoc from softland.cwcpbte, softland.cwmovim, softland.cwtauxi, softland.cwttdoc where cwmovim.ttdcod = cwttdoc.coddoc and cwtauxi.codaux=cwmovim.codaux and cwmovim.cpbmes<>'00' AND cwcpbte.CpbEst='V' and cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum and ((cwmovim.codaux = '103') or (cwtauxi.nomaux like '%CALIPSO%')) order by movfe desc"

sql_cxc_extranjeros = "SELECT cwmovim.CcCod, cwmovim.DgaCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovFe, cwmovim.CpbNum, cwmovim.PctCod, cwmovim.codaux, cwmovim.MovGlosa, cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS' FROM softland.cwmovim, softland.cwcpbte where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwcpbte.CpbEst='V' and cwmovim.PctCod='1-1-04-002' and cwmovim.codaux in ('105','103','101','11-2','96566940','2014','2015','2013') and cwmovim.cpbmes <>0 order by cwmovim.MovFe desc"
sql_cxc_nacionales = "SELECT cwmovim.CcCod, cwmovim.DgaCod, cwmovim.CpbMes as 'mes', cwmovim.CpbAno as 'ano', cwmovim.MovFe, cwmovim.CpbNum, cwmovim.PctCod, cwmovim.codaux, cwmovim.MovGlosa, cwmovim.MovDebe-cwmovim.MovHaber AS 'GASTOS' FROM softland.cwmovim, softland.cwcpbte where cwcpbte.AreaCod = cwmovim.AreaCod AND cwcpbte.CpbAno = cwmovim.CpbAno AND cwcpbte.CpbNum = cwmovim.CpbNum AND cwcpbte.CpbEst='V' and cwmovim.PctCod='1-1-04-001' and cwmovim.codaux in ('105','103','101','11-2','96566940','2014','2015','2013','96566940k','96566940-k') and cwmovim.cpbmes <>0 order by cwmovim.MovFe desc"
# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(DB.file_id)

#EXECUTE SQL SERVER QUERIES
import_sqlserver2gas(mydb,sql_afectosresumen,ws,"qAFECTOS_RESUMEN",10)
print(datetime.now(),': CSB Chile DASHBOARD: AFECTOS RESUMEN')
import_sqlserver2gas(mydb,sql_excentos,ws,"qEXCENTOS",11)
print(datetime.now(),': CSB Chile DASHBOARD: GASTOS EXCENTOS')
import_sqlserver2gas(mydb,sql_excentos_resumen,ws,"qEXCENTOS_RESUMEN",12)
print(datetime.now(),': CSB Chile DASHBOARD: GASTOS EXCENTOS RESUMEN')

import_sqlserver2gas(mydb,sql_imports,ws,"qIMPORTS",13)
print(datetime.now(),': CSB Chile DASHBOARD: IMPORTS')
import_sqlserver2gas(mydb,sql_ctasctes,ws,"qCTASCTES",14)
print(datetime.now(),': CSB Chile DASHBOARD: CTAS CTES')

import_sqlserver2gas(mydb,sql_documentos_emitidos,ws,"qDOCUMENTOS_EMITIDOS",16)
print(datetime.now(),': CSB Chile DASHBOARD: DOCUMENTOS EMITIDOS')
import_sqlserver2gas(mydb,sql_cxc_nacionales,ws,"qCxC_NACIONALES",17)
print(datetime.now(),': CSB Chile DASHBOARD: CxC NACIONALES')
import_sqlserver2gas(mydb,sql_cxc_extranjeros,ws,"qCxC_EXTRANJEROS",18)
print(datetime.now(),': CSB Chile DASHBOARD: CxC EXTRANJEROS')

mydb.close()


exit()

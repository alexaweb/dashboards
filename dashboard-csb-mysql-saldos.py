#!/usr/local/bin/python3.9
##home/acelle/cobs_python/cobs-env/bin/python3
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

print(datetime.now(),': CSB Saldos DASHBOARD')
ano = datetime.now().year
proxano = ano+1
# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)
sql_saldos = "SELECT rc.name AS vessel,AA.NAME AS CUENTA,SUM(aml.balance) AS SALDO ,    AA.CODE AS CODIGO,    rc.id AS company_id FROM account_move_line AS aml JOIN account_account AS aa  ON aml.account_id = aa.id JOIN res_company AS rc  ON aml.company_id = rc.id GROUP BY AA.CODE, AA.NAME, rc.id, rc.name"
sql_saldos_partners = "SELECT SUM(amount_residual) AS saldo,    aml.partner_id,    rp.name partner,    aml.company_id,    rc.name AS vessel FROM account_move_line AS aml JOIN res_partner AS rp  ON aml.partner_id = rp.id JOIN res_company AS rc  ON aml.company_id = rc.id GROUP BY aml.partner_id, rp.name, aml.company_id, rc.name HAVING SUM(amount_residual) <>0"
sql_saldos_ano_mes = "select rc.name as vessel, AA.NAME AS CUENTA, sum(balance) AS SALDO , AA.CODE AS CODIGO,  rc.id as company_id, extract(year from date) as ano, extract(month from date) as mes from account_move_line as aml join account_account as aa on aml.account_id = aa.id join res_company as rc on aml.company_id = rc.id  group by AA.CODE, AA.NAME, rc.id, rc.name, extract(year from date), extract(month from date)"
sql_eerr_current = "select rc.name as vessel, AA.NAME AS CUENTA, sum(balance) AS SALDO , AA.CODE AS CODIGO,  rc.id as company_id from account_move_line as aml join account_account as aa on aml.account_id = aa.id join res_company as rc on aml.company_id = rc.id  where extract(year from aml.date) = '"+str(ano)+"' group by AA.CODE, AA.NAME, rc.id, rc.name"
sql_saldos_interpetrol = "SELECT SUM(balance_cash_basis) AS saldo,       aml.company_id,    rc.name AS vessel, aml.journal_id, aml.account_id FROM account_move_line AS aml JOIN res_partner AS rp  ON aml.partner_id = rp.id JOIN res_company AS rc  ON aml.company_id = rc.id where aml.account_id in (16,50,109,166) GROUP BY aml.company_id, rc.name, aml.journal_id, aml.account_id"

#sql_balance_current = "select rc.name as vessel, AA.NAME AS CUENTA, sum(balance) AS SALDO , AA.CODE AS CODIGO,  rc.id as company_id from account_move_line as aml join account_account as aa on aml.account_id = aa.id join res_company as rc on aml.company_id = rc.id where aml.date < '"+str(proxano)+"-01-01' group by AA.CODE, AA.NAME, rc.id, rc.name"
#import_mysql2gas(mydb,sql_balance_current,ws,"qTEST",6)
#proxano = proxano -1
#sql_balance_current = "select rc.name as vessel, AA.NAME AS CUENTA, sum(balance) AS SALDO , AA.CODE AS CODIGO,  rc.id as company_id from account_move_line as aml join account_account as aa on aml.account_id = aa.id join res_company as rc on aml.company_id = rc.id where aml.date < '"+str(proxano)+"-01-01' group by AA.CODE, AA.NAME, rc.id, rc.name"

#import_mysql2gas(mydb,sql_balance_current,ws,"qTEST",6)
import_mysql2gas(mydb,sql_eerr_current,ws,"qEERRCURRENT",20)
import_mysql2gas(mydb,sql_eerr_current,ws,"qEERR"+str(ano),21)
ano = ano -1
sql_eerr_current = "select rc.name as vessel, AA.NAME AS CUENTA, sum(balance) AS SALDO , AA.CODE AS CODIGO,  rc.id as company_id from account_move_line as aml join account_account as aa on aml.account_id = aa.id join res_company as rc on aml.company_id = rc.id  where extract(year from aml.date) = '"+str(ano)+"' group by AA.CODE, AA.NAME, rc.id, rc.name"
import_mysql2gas(mydb,sql_eerr_current,ws,"qEERR"+str(ano),22)
import_mysql2gas(mydb,sql_saldos,ws,"qSALDOS",15)
import_mysql2gas(mydb,sql_saldos_ano_mes,ws,"qSALDOS ANO MES",16)
import_mysql2gas(mydb,sql_saldos_partners,ws,"qSALDOSPARTNERS",17)
import_mysql2gas(mydb,sql_saldos_interpetrol,ws,"qSALDOSINTERPETROL",18)

mydb.close()
#mydb.shutdown()

exit()



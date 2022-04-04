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
import time

import config.csbDB as csbDB

#while_rows = 9000

def log_begin(wbook,rownum):
	begincolumn=5
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,begincolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def log_end(wbook,rownum):
	endcolumn=7
	wbook.worksheet("CONTROLPANEL").update_cell(rownum,endcolumn,datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def import_mysql2gas(db,sqlstmt,ws,sheetname,rowlog):
	try:
		num_rows = 0
		#start logging
		log_begin(ws,rowlog)
		#while num_rows < while_rows:
		#time.sleep(5)
		print(datetime.now(),': CSB Movimientos DASHBOARD: start')
		#getdata and write to GAS
		cursor = db.cursor()
		cursor.execute(sqlstmt)
		#results=cursor.execute(sqlstmt,multi=True)
		sql_data=DataFrame(cursor.fetchall())
		sql_data.columns=cursor.column_names
		num_rows=sql_data.shape[0]
		print(datetime.now(),': CSB Movimientos DASHBOARD: ',num_rows,' rows')
#		if num_rows > while_rows:
		sh=ws.worksheet(sheetname)
		sh.clear()
		set_with_dataframe(sh,sql_data)
		print(datetime.now(),': CSB Movimientos DASHBOARD: SUCCESS!! ',num_rows,' rows')
		cursor.close()	
		#end logging
		log_end(ws,rowlog)
	except Exception as e: 
		print(e)


try:
	mydb = mysql.connector.connect(host=csbDB.dbhost,user=csbDB.dbuser,password=csbDB.dbpassword,database=csbDB.dbname)
except mysql.connector.Error as err:
	print("Error code:", err.errno)
	print("SQLSTATE value:", err.sqlstate)
	print("Error message:", err.msg)
	print("Error:", err)
	print("Error:", str(err))
	exit()


# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('/home/acelle/apps/cobs/client_secret_interpetrol.json', scope)
client = gspread.authorize(creds)
file_id = '1VVfSDKIRGJwaMGMU3DCJWkcy41vVJlL_meddISUnTLQ'

print(datetime.now(),': CSB Movimientos DASHBOARD: BEGIN')
# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
ws = client.open_by_key(file_id)

sql_movimientos = "SELECT invline.partner_id AS id,    rp.name AS partner_name,    COUNT(invline.id) AS orders,    pt.name AS producto,    pt.id AS p_id,SUM(invline.price_subtotal_SIGNED) AS monto,SUM(invline.quantity) AS cantidad,    max(invline.write_date) AS last_order,    invline.company_id,    rc.name AS company_name,    extract(year FROM ai.date_invoice) AS ano, invline.name AS producto_descripcion FROM account_invoice_line AS invline LEFT JOIN product_product AS pr  ON invline.product_id = pr.id LEFT JOIN product_template AS pt  ON pr.product_tmpl_id = pt.id JOIN res_partner rp  ON invline.partner_id = rp.id JOIN res_company rc  ON rc.id = invline.company_id JOIN account_invoice AS ai  ON ai.id = invline.invoice_id GROUP BY invline.partner_id, rp.name, invline.product_id, pt.name, invline.company_id, rc.name, ai.date_invoice, pt.id, invline.name order by last_order"

import_mysql2gas(mydb,sql_movimientos,ws,"qMOVIMIENTOS",10)
print(datetime.now(),': CSB Movimientos DASHBOARD: END')


mydb.close()


exit()



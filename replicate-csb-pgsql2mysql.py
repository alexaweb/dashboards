#!/home/acelle/cobs_python/cobs-env/bin/python
# LO SIGUIENTE NO ARROJA NINGUN TIPO DE WARNING PERO Python 'timestamp' cannot be converted to a MySQL type
#!/usr/local/bin/python3.9
# LO SIGUIENTE ARROJA EL WARNING DE SSL NO SOPORTADO POR FUTURA VERSION
#!/home/acelle/cobs_python/cobs-env/bin/python
#from oauth2client.service_account import ServiceAccountCredentials
#from apiclient.discovery import build
import mysql.connector
import psycopg2
#from gspread_dataframe import get_as_dataframe, set_with_dataframe
#from pandas import DataFrame
from datetime import datetime
import pandas as pd
import numpy as np
import time
import re
import warnings


# Ignores only DeprecationWarningS (not recommended)
warnings.filterwarnings(action='ignore',module='.*paramiko.*')

import psycopg2
from sshtunnel import SSHTunnelForwarder #Run pip install sshtunnel



import config.sshconfig as sshkeys
import config.csbDB as csbDB

f= '%Y-%m-%d %H:%M:%S'

def fetch_last_write_date(db):
	cursor = db.cursor()
	try:
		cursor.execute("select write_date from csb_write_date where id='0'")
		sql_data=cursor.fetchall()
		#print(sql_data)
		cursor.close()
		return sql_data[0]
	except mysql.connector.Error as err:
		print("No pude obtener fecha de corte")
		print("Error code:", err.errno)
		print("SQLSTATE value:", err.sqlstate)
		print("Error message:", err.msg)
		print("Error:", err)
		print("Error:", str(err))
		exit()

def update_last_write_date(db,fecha):
	cursor = db.cursor()
	try:
		cursor.execute("update csb_write_date set write_date='"+fecha+"' where id='0'")
		db.commit()
		return 0
	except mysql.connector.Error as err:
		print("No pude insertar nueva fecha de corte: ",err)
		exit()


try:
	mydb = mysql.connector.connect(host=csbDB.dbhost,user=csbDB.dbrwuser,password=csbDB.dbrwpassword,database=csbDB.dbname)
except mysql.connector.Error as err:
	print("No me pude connectar a MySQL")
	print("Error code:", err.errno)
	print("SQLSTATE value:", err.sqlstate)
	print("Error message:", err.msg)
	print("Error:", err)
	print("Error:", str(err))
	exit()

nueva_fecha_corte = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
fecha_corte = fetch_last_write_date(mydb)[0]
print("Se inicia replica pgSQL a MySQL con fecha: ", nueva_fecha_corte)
print("Extran registros a contar de: ",fecha_corte)


with SSHTunnelForwarder(
	(csbDB.sshhost, 22), #Remote server IP and SSH port
	ssh_username = csbDB.sshuser,
	print('aw:',sshkeys.AWS)
	#ssh_password = "<password>",
	ssh_private_key = sshkeys.AWS,
	remote_bind_address=(sshkeys.AWS_local_ip, sshkeys.AWS_local_port)) as server: #PostgreSQL server IP and sever port on remote machine

	try:
		server.start() #start ssh sever
	except Error as e:
		print(datetime.now(),' No pude conectar a AWS: '',e)
		exit()
	print(datetime.now(),' Replicate CSB: Server connected via SSH')

	params = {
		'database': csbDB.pgdbname,
		'user': csbDB.pguser,
		'password': csbDB.pgpassword,
		'host': csbDB.pghost,
		'port': server.local_bind_port
		}

	try:
		conn = psycopg2.connect(**params)
	except Error as e:
		print(datetime.now(),' No pude conectar a pgSQL: '',e)
		exit()
	curs = conn.cursor()
	print(datetime.now(),' pgSQL database connected\n')


	sql_statement1 = "select * from account_account where write_date > '"+str(fecha_corte)+"';"
	sql_statement2 = "select * from account_invoice where write_date > '"+str(fecha_corte)+"';"
	sql_statement3 = "select * from account_invoice_line where write_date > '"+str(fecha_corte)+"';"
	sql_statement4 = "select * from account_move_line where write_date > '"+str(fecha_corte)+"';"
	sql_statement5 = "select * from product_product where write_date > '"+str(fecha_corte)+"';"
	sql_statement6 = "select * from product_template where write_date > '"+str(fecha_corte)+"';"
	sql_statement7 = "select * from res_company where write_date > '"+str(fecha_corte)+"';"
	sql_statement8 = "select * from res_partner where write_date > '"+str(fecha_corte)+"';"

#	curs.execute(sql_statement)  	#obtain results in set format
#	records = curs.fetchall()	#obtain results in set format
	try:
		data1 = pd.read_sql_query(sql_statement1,conn)	#obtain results in dataframe format
		data2 = pd.read_sql_query(sql_statement2,conn)	#obtain results in dataframe format
		data3 = pd.read_sql_query(sql_statement3,conn)	#obtain results in dataframe format
		data4 = pd.read_sql_query(sql_statement4,conn)	#obtain results in dataframe format
		data5 = pd.read_sql_query(sql_statement5,conn)	#obtain results in dataframe format
		data6 = pd.read_sql_query(sql_statement6,conn)	#obtain results in dataframe format
		data7 = pd.read_sql_query(sql_statement7,conn)	#obtain results in dataframe format
		data8 = pd.read_sql_query(sql_statement8,conn)	#obtain results in dataframe format
	except Error as e:
		print("Error obteniendo pgSQL: ",e)
		exit()


#	print(data7)
	data1 = data1.replace({np.NaN: None})
	data2 = data2.replace({np.NaN: None})
	data3 = data3.replace({np.NaN: None})
	data4 = data4.replace({np.NaN: None})
	data5 = data5.replace({np.NaN: None})
	data6 = data6.replace({np.NaN: None})
#	data7 = data7.replace({np.NaN: None})
	data8 = data8.replace({np.NaN: None})
#	print(data7)

	cols1 = "`,`".join([str(i) for i in data1.columns.tolist()])
	cols2 = "`,`".join([str(i) for i in data2.columns.tolist()])
	cols3 = "`,`".join([str(i) for i in data3.columns.tolist()])
	cols4 = "`,`".join([str(i) for i in data4.columns.tolist()])
	cols5 = "`,`".join([str(i) for i in data5.columns.tolist()])
	cols6 = "`,`".join([str(i) for i in data6.columns.tolist()])
	cols7 = "`,`".join([str(i) for i in data7.columns.tolist()])
	cols8 = "`,`".join([str(i) for i in data8.columns.tolist()])



	#newc = mydb.cursor()
	try:
		rstatement = "replace into account_move_line_copy(`id`,`create_date`,`statement_id`,`invoice_id`,`journal_id`,`currency_id`,`date_maturity`,`user_type_id`,`partner_id`,`blocked`,`analytic_account_id`,`create_uid`,`full_reconcile_id`,`amount_residual`,`company_id`,`amount_residual_currency`,`debit`,`ref`,`account_id`,`debit_cash_basis`,`reconciled`,`credit`,`balance_cash_basis`,`write_date`,`date`,`write_uid`,`move_id`,`product_id`,`payment_id`,`company_currency_id`,`name`,`product_uom_id`,`tax_line_id`,`credit_cash_basis`,`tax_exigible`,`amount_currency`,`balance`,`quantity`) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
		#vs = "(48459, Timestamp('2021-08-04 20:46:51'), None, None, 25, None, datetime.date(2021, 7, 21), 2, 90, False, None, 9, None, 100.0, 5, 0.0, 100.0, '', 164, 100.0, False, 0.0, 100.0, Timestamp('2021-08-04 20:46:51'), datetime.date(2021, 7, 21), 9, 18878, None, 3086, 3, 'Vendor Payment', None, None, 0.0, True, 0.0, 100.0, None)"
		#print(vs)
		#newc.execute(rstatement,vs)
	except mysql.connector.Error as err:
		print("No pude obtener fecha de corte")
		print("Error code:", err.errno)
		print("SQLSTATE value:", err.sqlstate)
		print("Error message:", err.msg)
		print("Error:", err)
		print("Error:", str(err))
		exit()
	#mydb.commit()

	i=0
	replacecursor = mydb.cursor()
	for i,row in data1.iterrows():
		replace_statement = "replace into account_account(`" +cols1+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		print(replace_statement)
#		print(tuple(row))
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 1: ",i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data2.iterrows():
		replace_statement = "replace into account_invoice(`" +cols2+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		print(replace_statement)
#		print(tuple(row))
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 2: ",i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data3.iterrows():
		replace_statement = "replace into account_invoice_line(`" +cols3+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		print(replace_statement)
#		print(tuple(row))
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 3: ",i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data4.iterrows():
		replace_statement = "replace into account_move_line(`" +cols4+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
		try:
			replacecursor.execute(replace_statement,tuple(row))
		except mysql.connector.Error as err:
			print(err)
			print("Error code:", err.errno)
			print("SQLSTATE value:", err.sqlstate)
			print("Error message:", err.msg)
			print("Error:", err)
			print("Error:", str(err))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 4: ", i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data5.iterrows():
		replace_statement = "replace into product_product(`" +cols5+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		print(replace_statement)
#		print(tuple(row))
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 5: ",i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data6.iterrows():
		replace_statement = "replace into product_template(`" +cols6+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		print(replace_statement)
#		print(tuple(row))
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 6: ", i)

#	i=0
#	replacecursor = mydb.cursor()
#	for i,row in data7.iterrows():
#		replace_statement = "replace into res_company(`" +cols7+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
#		replacecursor.execute(replace_statement,tuple(row))
#		mydb.commit()
#		i+=1
#	replacecursor.close()
#	print("TABLE 7: ", i)
	i=0
	replacecursor = mydb.cursor()
	for i,row in data8.iterrows():
		replace_statement = "replace into res_partner(`" +cols8+ "`) values (" + "%s,"*(len(row)-1) + "%s)"
		replacecursor.execute(replace_statement,tuple(row))
		mydb.commit()
		i+=1
	replacecursor.close()
	print("TABLE 8: ",i)

	update_last_write_date(mydb,nueva_fecha_corte)

	mydb.close()


	exit()

print("... ending ...")

exit()

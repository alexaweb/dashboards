#!/home/acelle/cobs_python/cobs-env/bin/python3
from pgsql.query import *
import getpass

p_host = '127.0.0.1'
p_port = 5432
db = 'CSB'
ssh = True
ssh_user = 'ec2-user'
ssh_host = '3.130.76.145'
ssh_pkey = '/home/acelle/aws-act.pem'
psql_user= 'odoo'
psql_pass = 'odoo'

pgres = Postgresql_connect(pgres_host=p_host, pgres_port=p_port, db=db, ssh=ssh, ssh_user=ssh_user, ssh_host=ssh_host, ssh_pkey=ssh_pkey)
#initiates a connection to the PostgreSQL database. In this instance we use ssh and must specify our ssh credentials.

#You'll need to define psql_user and psql_pass using input() and getpass() to temporarily store your credentials.
#Alternatively, best practice you may be to store your credentials as environment variables.
#psql_user = input("Please enter your database username:")
#psql_pass = getpass.getpass("Welcome {psql_user}! || Password:") 

#sql_statement = """SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'ey_test_table';"""
#query_df = pgres.query(db='CSB', query=sql_statement)
#query_df
#returns the results of an sql statement as a pandas dataframe. 
#This example returns the column names and data types of table 'ey_test_table'.
#exit()
sql_statement = "select * from account_account;"
query_df = pgres.query(db='CSB',query=sql_statement)
print(query_df)



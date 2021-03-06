from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import inspect
import pandas as pd

class Postgresql_connect(object):
    def __init__(self, pgres_host, pgres_port, db, ssh, ssh_user, ssh_host, ssh_pkey):
        # SSH Tunnel Variables
        self.pgres_host = pgres_host
        self.pgres_port = pgres_port
        
        if ssh == True:
            self.server = SSHTunnelForwarder(
                (ssh_host, 22),
                ssh_username=ssh_user,
                ssh_private_key=ssh_pkey,
                remote_bind_address=(pgres_host, pgres_port),
            )
            server = self.server
            server.start() #start ssh server
            self.local_port = server.local_bind_port
            print('Server connected via SSH || Local Port: {self.local_port}...')
        elif ssh == False:
            pass

    def schemas(self, db):
        engine = create_engine('postgresql://{psql_user}:{psql_pass}@{self.pgres_host}:{self.local_port}/{db}')
        inspector = inspect(engine)
        print ('Postgres database engine inspector created...')
        schemas = inspector.get_schema_names()
        self.schemas_df = pd.DataFrame(schemas,columns=['schema name'])
        print("Number of schemas: {len(self.schemas_df)}")
        engine.dispose()
        return self.schemas_df
    
    def tables(self, db, schema):
        engine = create_engine('postgresql://{psql_user}:{psql_pass}@{self.pgres_host}:{self.local_port}/{db}')
        inspector = inspect(engine)
        print ('Postgres database engine inspector created...')
        tables = inspector.get_table_names(schema=schema)
        self.tables_df = pd.DataFrame(tables,columns=['table name'])
        print("Number of tables: {len(self.tables_df)}")
        engine.dispose()
        return self.tables_df

    def query(self, db, query):
        print('postgresql://{psql_user}:{psql_pass}@{self.pgres_host}:{self.local_port}/{db}')
#        engine = create_engine('postgresql://odoo:odoo@3.130.76.145:5432/{db}')
        engine = create_engine('postgresql://odoo:odoo@3.130.76.145:5432/CSB')
        print ('Database [{db}] session created...')
        self.query_df = pd.read_sql(query,engine)
        print ('<> Query Sucessful <>')
        engine.dispose()
        return self.query_df

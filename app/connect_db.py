from sqlalchemy import create_engine
import config_utils
from logger import timed, logging
from sqlalchemy.orm import sessionmaker

class connect_db:
    def __init__(self, DB):
        self.database = DB
        config = config_utils.DBConnection()

        user = config['psql_user']
        password = config['psql_password']
        port = config['psql_port']
        host = config['psql_host']
        
        # sql_url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{self.database}'
        sql_url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{self.database}'
        # print(sql_url)
        self.engine = create_engine(sql_url)

    def session(self):
        try:
            Session = sessionmaker(self.engine)
            return Session
        except:
            print(f'couldnt connect to {self.database}')
            return None
        
    def connect(self):
        try:
            conn = self.sql.engine.connect()
            return conn
        except:
            print(f'couldnt connect to {self.database}')
            return None
        
    def disconnect(self):
        self.engine.dispose()

if __name__=='__main__':
    connect_db('Akshay')

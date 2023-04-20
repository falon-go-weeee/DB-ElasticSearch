from multiprocessing import Pool
from sqlalchemy import create_engine, select, update, MetaData, Table, Column, Integer, String, CHAR, VARCHAR

user = 'root'
password = 'newpassword'
port = 3306
database = 'Akshay'
host = 'localhost'
# create an engine to connect to the database
sql_url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}'
# print(sql_url)
engine = create_engine(sql_url)
print(engine)

# create a metadata object to hold information about the table
metadata = MetaData()

# define the table structure
products_table = Table('fashion_stores', metadata,
    Column('id', Integer, primary_key=True),
    Column('gender', String(10)),
    Column('age_group', String(10)),
    Column('material', String(50)),
    Column('season', String(10)),
    Column('rating', Integer)
)

# create a connection object
conn = engine.connect()

update_statement = update(products_table).where(products_table.c.id == 101).values(rating=5)

conn.execute(update_statement)

conn.commit()
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select

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

metadata = MetaData()

products_table = Table('fashion_stores', metadata,
              Column('id', Integer, primary_key=True, autoincrement='auto'),
              Column('gender', String(10)),
              Column('age_group', String(10)),
              Column('material', String(50)),
              Column('season', String(10)),
              Column('rating', Integer)
              )

query = select(products_table).where(products_table.c.gender.like('%male%'.upper()) &
                                        products_table.c.rating.between(7, 9))

conn = engine.connect()

result = conn.execute(query).fetchall()
for row in result:
    print(row)
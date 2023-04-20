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

metadata.create_all(engine)

query = select(products_table.c.id, products_table.c.gender, products_table.c.age_group, products_table.c.material, products_table.c.season, products_table.c.rating).where(products_table.c.rating.between(5,9))

conn = engine.connect()

result = conn.execute(query).fetchall()
for row in result:
    print(row)
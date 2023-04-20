from sqlalchemy import Table, Column, Integer, String, MetaData, select, join, CheckConstraint, ExceptionContext
from sqlalchemy.exc import DatabaseError
from connect_db import connect_db, disconnect_db
from logger import timed, logging
from tables import stores_table, addresses_table, metadata

# CREATE TABLE address LIKE raw_addresses;
# INSERT INTO address SELECT * FROM raw_addresses

@timed
def insert_data():
    engine, Session = connect_db('Akshay')
    
    metadata.create_all(engine)

    insert_in_stores = stores_table.insert().values(gender='female', age_group='adult', material='cotton', season='summer', rating=1)
    insert_in_address = addresses_table.insert().values(address='950 MASON ST, SAN FRANCISCO, CA, 94108')
    logging.info(f'data inserted in table')
    try:
        with Session() as session:
            session.execute(insert_in_stores)
            session.execute(insert_in_address)
            session.commit()
    except DatabaseError as de:
        print('invalid_rating data', de._message())
    finally:
        disconnect_db(engine)

if __name__=='__main__':
    insert_data()

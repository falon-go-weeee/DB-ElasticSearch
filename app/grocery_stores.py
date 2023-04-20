import pandas as pd
from sqlalchemy import Table, Column, Integer, String, MetaData, select, join, CheckConstraint, ForeignKey, ForeignKeyConstraint
from sqlalchemy.exc import DatabaseError
from sqlalchemy.sql import func
from multiprocessing import Pool, cpu_count
from connect_db import connect_db
from logger import timed, logging
from db_df import table_to_dict, to_df
from store_addresses import store_addresses
from date_time import get_date_time

class grocery_stores(store_addresses):
    metadata = MetaData()
    
    def __init__(self):
        self.sql = connect_db('grocery')
        sa = store_addresses()
        store_addresses_table = Table("store_addresses", sa.metadata, autoload_with=self.sql.engine, echo=True)
        self.grocery_stores_table = Table('grocery_stores', self.metadata,
                            Column('store_id', Integer, primary_key=True, autoincrement='auto'),
                            Column('BUSINESS_NAME', String(100)),
                            Column('DBA_NAME', String(100)),
                            Column('open_timing', String(70)),
                            Column('closing_timing', String(70)),
                            Column('contact_no', String(15)),
                            Column('payment_option', String(70)),
                            Column('delivery_option', String(70)),
                            Column('rating', Integer),
                            Column('date_time', String(20)),
                            Column('id', Integer, ForeignKey(store_addresses_table.c.id), nullable=False),
                            ForeignKeyConstraint(['id'], [store_addresses_table.c.id]),
                            )
        # print(self.grocery_stores_table)
        self.metadata.create_all(self.sql.engine)

    def insert_stores(self, BUSINESS_NAME, DBA_NAME, open_timing, closing_timing, contact_no, payment_option, delivery_option, rating):
        Session = self.sql.session()
        insert_stores_query = self.grocery_stores_table.insert().values(BUSINESS_NAME=BUSINESS_NAME,
                                                                        DBA_NAME=DBA_NAME,
                                                                        open_timing=open_timing,
                                                                        closing_timing=closing_timing,
                                                                        contact_no=contact_no,
                                                                        payment_option=payment_option,
                                                                        delivery_option=delivery_option,
                                                                        rating=rating,
                                                                        date_time=get_date_time()
                                                                        )
        try:
            with Session() as session:
                session.execute(insert_stores_query)
                session.commit()
                logging.info(f'data inserted in table')
        except DatabaseError as de:
            print(de._message())
        finally:
            self.sql.disconnect()

    @timed
    def find_store(self, address):
        Session = self.sql.session()

        sub_query = join(self.grocery_stores_table, self.store_addresses_table, (self.grocery_stores_table.c.id == self.store_addresses_table.c.id & self.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%')))
        query = select(self.store_addresses_table.c.STREET_ADDRESS, self.store_addresses_table.c.ZIP_CODE, self.grocery_stores_table).select_from(sub_query)

        # sub_query = select(self.store_addresses_table.c.id).where(self.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%'))
        # query = select(self.grocery_stores_table).where(self.grocery_stores_table.c.id.in_(sub_query))

        with Session() as session:
            stores = session.execute(query).fetchall()
        # print(stores)
        return stores

    @timed
    def multiprocess_store_search(self, address, chunk_count, process_count):
        Session = self.sql.session()
        res =[]
        with Session() as session:
            index_count = session.execute(select(func.min(self.store_addresses_table.c.id), func.count(self.store_addresses_table.c.id))).fetchone()
            # print(index_count)
            chunk_size = round((index_count[1]-index_count[0])/chunk_count)
            # print(chunk_size)
            chunks = [(x, x+chunk_size, address) for x in range(index_count[0], index_count[1], chunk_size)]
            print(chunks)
        with Pool(processes=process_count) as pool:
            result = pool.starmap(search_chunk, chunks)
            # print(result)
        for i in result:
            res.extend(i) 
        return res
        
@timed
def search_chunk(min_id, max_id, address):
    db_temp = connect_db('grocery')
    gs = grocery_stores()
    Session = db_temp.session()
    with Session() as session:
        sub_query = join(gs.grocery_stores_table, gs.store_addresses_table, (gs.grocery_stores_table.c.id == gs.store_addresses_table.c.id & gs.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%') & gs.store_addresses_table.c.id.between(min_id, max_id)))
        query = select(gs.store_addresses_table.c.STREET_ADDRESS, gs.store_addresses_table.c.ZIP_CODE, gs.grocery_stores_table).select_from(sub_query)
        result = session.execute(query).fetchall()
        db_temp.disconnect()
        return result

if __name__=='__main__':
    gs = grocery_stores()
    
    # stores = gs.multiprocess_store_search(address='9860 NATIONAL BLVD',chunk_count=10,process_count=4)
    # stores = gs.find_store(address='9860 NATIONAL BLVD')
    # print(to_df(stores))

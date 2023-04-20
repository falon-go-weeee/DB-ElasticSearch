import pandas as pd
import re
from sqlalchemy import Table, Column, Integer, String, MetaData, select, join, CheckConstraint
from sqlalchemy.exc import DatabaseError
from connect_db import connect_db
from logger import timed, logging
from db_df import to_df, table_to_dict
from date_time import get_date_time
from locationIQ import complete_address

class store_addresses():
    metadata = MetaData()
    
    def __init__(self):
        self.sql = connect_db('grocery')
        self.store_addresses_table = Table('store_addresses', self.metadata,
                            Column('id', Integer, primary_key=True, autoincrement='auto'),
                            Column('STREET_ADDRESS', String(70)),
                            Column('CITY', String(50)),
                            Column('STATE', String(20)),
                            Column('ZIP_CODE', String(10)),
                            Column('date_time', String(20))
                            )
        self.metadata.create_all(self.sql.engine)

    def insert_address(self, building_no, street, city, state, zip_code):
        Session = self.sql.session()
        insert_store_addresses_query = self.store_addresses_table.insert().values(BUILDING_NO=building_no,
                                                                                    STREET_ADDRESS=street,
                                                                                    CITY=city,
                                                                                    STATE=state,
                                                                                    ZIP_CODE=zip_code,
                                                                                    date_time=get_date_time()
                                                                                    )
        try:
            with Session() as session:
                session.execute(insert_store_addresses_query)
                session.commit()
                logging.info(f'data inserted in table')
        except DatabaseError as de:
            print(de._message())
        finally:
            self.sql.disconnect()
    
    # def segregate_df_address(func):
    #     def wrapper(df):                          #wrapper function iterates over DF and calls decorated function for each row
    #         city,state,zip_code = func()                                   #decorated function called here
    #         street = []
    #         for index, row in df.iterrows():
    #             # print(df.loc[index,'address'])
    #             s = row['address'].split(" ")
    #             if "" in s:
    #                 s.remove("")
    #                 s = " ".join(s)
    #                 row['address'] = s
    #             street.append(re.sub(f"{city}|{state}|{zip_code}|,", "", row['address']))
    #         df['street address'] = street
    #         df['city'] = df['address'].str.extract(f'({city})', expand=True)
    #         df['state'] = df['address'].str.extract(f'({state})', expand=True)
    #         df['Zip_code'] = df['address'].str.extract(f'({zip_code})', expand=True)
    #         df = df.drop('address', axis=1)
    #         return df
    #     return wrapper

    def find_address_id(self, address):
        Session = self.sql.session()
        query = select(self.store_addresses_table.c.id).where(self.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%'))
        with Session() as session:
            result = session.execute(query).fetchall()
        ids = tuple([id[0] for id in result])
        # print(ids)
        return ids
    
    def run_geocode(self):
        Session = self.sql.session()
        query = select(self.store_addresses_table)
        with Session() as session:
            result = session.execute(query).fetchall()
        for address in result:
            address = ", ".join(address[1:-1])
            print(address, " : ", complete_address(address))
        # print(result)
        # return result

    def find_address_col(self, address):
        Session = self.sql.session()
        query = select(self.store_addresses_table).where(self.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%'))
        with Session() as session:
            result = session.execute(query).fetchall()
        # print(result)
        return result

if __name__=='__main__':
    sa = store_addresses()

    ids = sa.find_address_col('9860 NATIONAL BLVD')
    print(ids)

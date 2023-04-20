from multiprocessing import Pool, cpu_count
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.sql import func
from connect_db import connect_db
from logger import timed, logging
from tables import products_table, addresses_table, metadata
from locationIQ import complete_address
from test_geo import geocode
@timed
def process_chunk(min_id, max_id):
    db = connect_db('Akshay')
    Session = db.session()
    ls = []
    with Session() as session:
        query = select(addresses_table.c.address).where(addresses_table.c.id.between(min_id, max_id))
        result = session.execute(query).fetchall()
        for res in result:
            ls.append([res, geocode(res)])
        # for row in session.execute(query):
        #     print(row)
            # pass
    return ls

@timed
def multiprocess_fetch(table, process_count):
    db = connect_db('Akshay')
    Session = db.session()
    res =[]
    with Session() as session:
        min_count = session.execute(select(func.min(table.c.id))).scalar()
        # print(min_count)
        max_count = session.execute(select(func.count(table.c.id))).scalar()
        # print(max_count)
        chunk_size = round((max_count-min_count)/process_count)
        # print(chunk_size)
        chunks = [(x, x+chunk_size) for x in range(min_count, max_count, chunk_size)]
        # print(chunks)
    with Pool(processes=process_count) as pool:
        result = pool.starmap(process_chunk, chunks)
        # print(result)
        for i in result:
            res.extend(i) 
        for j in res:
            print(j)
        print(len(res))

def search_chunk(min_id, max_id, address):
    db = connect_db('Akshay')
    Session = db.session()
    with Session() as session:
        query = select(addresses_table).where(addresses_table.c.id.between(min_id, max_id) & addresses_table.c.address.like(f'%{address}%'))
        result = session.execute(query).fetchall()
        return result
        # for row in session.execute(query):
        #     print(row)
            # pass
@timed
def multiprocess_search(address, process_count):
    db = connect_db('Akshay')
    Session = db.session()
    res =[]
    with Session() as session:
        min_count = session.execute(select(func.min(addresses_table.c.id))).scalar()
        # print(min_count)
        max_count = session.execute(select(func.count(addresses_table.c.id))).scalar()
        # print(max_count)
        chunk_size = round((max_count-min_count)/process_count)
        # print(chunk_size)
        chunks = [(x, x+chunk_size, address) for x in range(min_count, max_count, chunk_size)]
        print(chunks)
    with Pool(processes=process_count) as pool:
        result = pool.starmap(search_chunk, chunks)
        # print(result)
        for i in result:
            res.extend(i) 
        print(res)


if __name__ == '__main__':
    multiprocess_fetch(table=addresses_table, process_count=8)
    # multiprocess_search('mason',4)
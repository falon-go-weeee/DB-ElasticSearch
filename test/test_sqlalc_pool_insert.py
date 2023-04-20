from multiprocessing import Pool, cpu_count
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, select
from sqlalchemy.sql import func
from app.connect_db import connect_db, disconnect_db
from app.logger import timed, logging
from app.tables import products_table, addresses_table, metadata


@timed
def process_chunk(min_id, max_id):
    engine, Session = connect_db('Akshay')
    with Session() as session:
        query = select(products_table).where(products_table.c.id.between(min_id, max_id))
        for row in session.execute(query):
            print(row)
            # pass

@timed
def multiprocess_fetch(table, process_count):
    engine, Session = connect_db('Akshay')
    metadata.create_all(engine)

    with Session() as session:
        min_count = session.execute(select(func.min(table.c.id))).scalar()
        # print(min_count)
        max_count = session.execute(select(func.count(table.c.id))).scalar()
        # print(max_count)
        chunks = [(x, x + 100) for x in range(min_count, max_count, 100)]
        # print(chunks)
    with Pool(processes=process_count) as pool:
        pool.starmap(process_chunk, chunks)

if __name__ == '__main__':
    multiprocess_fetch(table=products_table, process_count=4)

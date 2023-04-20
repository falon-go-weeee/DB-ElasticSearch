from sqlalchemy import select
from store_addresses import store_addresses
import pandas as pd
from multiprocessing import Pool
from logger import timed

@timed
def find_address(df):
    query_ls = []
    result = []
    sa = store_addresses()
    for i,row in df.iterrows():
        address = row['address']
        query = select(sa.store_addresses_table).where(sa.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%'))
        query_ls.append(query)
    with Pool(processes=4) as pool:
        rows = pool.map(run_query, query_ls)
    for row in rows:
        result.extend(row)
    # print(result)
    return(result)

@timed
def find_address_single(df):
    query_ls = []
    result = []
    sa = store_addresses()
    for i,row in df.iterrows():
        address = row['address']
        query = select(sa.store_addresses_table).where(sa.store_addresses_table.c.STREET_ADDRESS.like(f'%{address}%'))
        run_query(query)
        # query_ls.append(query)
    # print(result)
    return(result)

# @timed
def run_query(query):
    result = []
    sa = store_addresses()
    Session = sa.sql.session()
    with Session() as session:
        res = session.execute(query).fetchall()
    result.extend(res)
    sa.sql.disconnect()
    # print(result)
    return result

if __name__=='__main__':
    addresses = ['9860 NATIONAL BLVD', 
                 '600 E 7TH STREET', 
                 '1573 W ADAMS BLVD', 
                 '1000 GLENDON AVENUE', 
                 '1239   PRODUCE ROW', 
                 '1770 N HIGHLAND AVENUE', 
                 '426 E PICO BLVD', 
                 '3798 ARLINGTON AVENUE', 
                 '3500 W 6TH STREET', 
                 '3975 WILSHIRE BLVD', 
                 '1999 AVENUE OF THE STARS',
                 '9860 NATIONAL BLVD', 
                 '600 E 7TH STREET', 
                 '1573 W ADAMS BLVD', 
                 '1000 GLENDON AVENUE', 
                 '1239   PRODUCE ROW', 
                 '1770 N HIGHLAND AVENUE', 
                 '426 E PICO BLVD', 
                 '3798 ARLINGTON AVENUE', 
                 '3500 W 6TH STREET', 
                 '3975 WILSHIRE BLVD', 
                 '1999 AVENUE OF THE STARS',
                 '9860 NATIONAL BLVD', 
                 '600 E 7TH STREET', 
                 '1573 W ADAMS BLVD', 
                 '1000 GLENDON AVENUE', 
                 '1239   PRODUCE ROW', 
                 '1770 N HIGHLAND AVENUE', 
                 '426 E PICO BLVD', 
                 '3798 ARLINGTON AVENUE', 
                 '3500 W 6TH STREET', 
                 '3975 WILSHIRE BLVD', 
                 '1999 AVENUE OF THE STARS',
                 '9860 NATIONAL BLVD', 
                 '600 E 7TH STREET', 
                 '1573 W ADAMS BLVD', 
                 '1000 GLENDON AVENUE', 
                 '1239   PRODUCE ROW', 
                 '1770 N HIGHLAND AVENUE', 
                 '426 E PICO BLVD', 
                 '3798 ARLINGTON AVENUE', 
                 '3500 W 6TH STREET', 
                 '3975 WILSHIRE BLVD', 
                 '1999 AVENUE OF THE STARS',
                 '9860 NATIONAL BLVD', 
                 '600 E 7TH STREET', 
                 '1573 W ADAMS BLVD', 
                 '1000 GLENDON AVENUE', 
                 '1239   PRODUCE ROW', 
                 '1770 N HIGHLAND AVENUE', 
                 '426 E PICO BLVD', 
                 '3798 ARLINGTON AVENUE', 
                 '3500 W 6TH STREET', 
                 '3975 WILSHIRE BLVD', 
                 '1999 AVENUE OF THE STARS']
    df = pd.DataFrame(addresses, columns=['address'])
    # print(df)

    addresses = find_address(df)
    res1 = find_address_single(df)
    # for address in addresses:
    #     print(address)
    
import pandas as pd
import re
from sqlalchemy.exc import DatabaseError
from connect_db import connect_db
from logger import timed, logging
# from tables import grocery_stores_table, store_addresses_table, raw_address_table, metadata
from grocery_stores import grocery_stores
from store_addresses import store_addresses
from date_time import get_date_time
import random


def clean_df():
    df = pd.read_csv('/home/akshay/Chistats/Grocery_stores/Grocery_Stores.csv')
    df = df.drop(['LOCATION END DATE'],axis=1)
    df = df.drop(['COUNCIL DISTRICT'],axis=1)
    df = df.drop(['MAILING ADDRESS'],axis=1)
    df = df.drop(['MAILING CITY'],axis=1)
    df = df.drop(['MAILING ZIP CODE'],axis=1)
    df = df.drop(['NAICS'],axis=1)
    df = df.drop(['PRIMARY NAICS DESCRIPTION'],axis=1)
    df = df.drop(['LOCATION'],axis=1)
    df = df.drop(['LOCATION DESCRIPTION'],axis=1)
    df = df.drop(['LOCATION START DATE'],axis=1)

    df["DBA_NAME"].fillna('', inplace = True)

    df['id'] = range(1, len(df)+1, 1)
    df = df.set_index('id')

    df = df.drop(['LOCATION ACCOUNT #'],axis=1)

    zip = []

    for i, row in df.iterrows():
        zip_code = re.findall(r'\d{5}', row['ZIP_CODE'])
        if len(zip_code)>0:
            zip.append(zip_code[0])
        else:
            zip.append(None)
        # print(zip_code)
    df['ZIP_CODE'] = zip
    # print(df)
    return df

def create_dummy_data(df):

    open_timing = ('7:00 AM', '8:00 AM' ,'9:00 AM', '10:00 AM', '11:00 AM', '12:00 PM', '7:30 AM', '8:30 AM' ,'9:30 AM', '10:30 AM', '11:30 AM', '12:30 PM', '7:45AM', '8:45 AM' ,'9:45 AM', '10:45 AM', '11:45 AM', '12:45 PM')
    closing_timing = ('6:00 PM', '7:00 PM', '8:00 PM','9:00 PM', '10:00 PM', '11:00 PM', '12:00 AM', '6:30 PM', '7:30 PM', '8:30 PM','9:30 PM', '10:30 PM', '11:30 PM', '12:30 AM', '6:45 PM', '7:45 PM', '8:45 PM','9:45 PM', '10:45 PM', '11:45 PM', '12:45 AM')
    payment_option = ('cash', 'credit card', 'debit card', 'online', 'apple pay', 'Gift cards', 'checks')
    delivery_option = ('home delivery', 'store pickup')
    contact_no = (213, 310, 424, 661, 818, 323)
    state = 'California'
    date_time = get_date_time()

    state_col = []
    rating_col = []
    open_timing_col = []
    closing_timing_col = []
    contact_no_col = []
    payment_option_col = []
    delivery_option_col = []
    date_time_col = []
    Building_no_col = []
    id = []
    for index, row in df.iterrows():
        open_timing_col.append(random.choice(open_timing))
        closing_timing_col.append(random.choice(closing_timing))
        payment_option_col.append(', '.join(random.sample(payment_option, random.randint(1,4))))
        delivery_option_col.append(random.choice(delivery_option))
        contact_no_col.append((random.choice(contact_no)*(10**7))+(random.randint(10**7,(10**8-1))))
        rating_col.append(random.randint(1,10))
        state_col.append(state)
        date_time_col.append(date_time)
        Building_no_col.extend(re.findall('^\d+', row['STREET_ADDRESS']))
        id.append(index)
    temp = pd.DataFrame({'open_timing': open_timing_col,
                         'closing_timing': closing_timing_col,
                         'contact_no': contact_no_col,
                         'payment_option': payment_option_col,
                         'delivery_option': delivery_option_col,
                         'rating': rating_col,
                         'STATE': state_col,
                         'BUILDING_NO': Building_no_col,
                         'date_time': date_time_col,
                         'id': id}
                         )
    temp = pd.concat([df, temp], axis=1)
    return temp

def create_addresses_tables(df):
    # print(df.info())
    sa = store_addresses()
    Session = sa.sql.session()
    query_ls = []
    df = df[['id', 'STREET_ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 'date_time']]
    df_ls = df.to_dict('records')
    with Session() as session:
        # for i,row in df.iterrows():
        #     insert_store_addresses_query = sa.store_addresses_table.insert().values(id=i,                 
        #                                                         STREET_ADDRESS=row['STREET_ADDRESS'],
        #                                                         CITY=row['CITY'],
        #                                                         STATE=row['STATE'],
        #                                                         ZIP_CODE=row['ZIP_CODE'],
        #                                                         date_time=row['date_time']
        #                                                          )
            # insert_raw_addresses_query = gs.raw_address_table.insert().values(id=i, 
            #                                                     ADDRESS=row['STREET_ADDRESS']+','+row['CITY']+row['STATE']+','+row['ZIP_CODE']
            #                                                     )
            
            # print(insert_query)
        insert_store_addresses_query = sa.store_addresses_table.insert().values(df_ls)
        session.execute(insert_store_addresses_query)
        session.commit()

def create_stores_tables(df):
    gs = grocery_stores()
    Session = gs.sql.session()
    df = df[['BUSINESS_NAME', 'DBA_NAME', 'open_timing', 'closing_timing', 'contact_no', 'payment_option', 'delivery_option', 'rating', 'date_time', 'id']]
    df_ls = df.to_dict('records')
    with Session() as session:
        # for i,row in df.iterrows():
        #     # print(row)
        #     # col_values = [row[col_name] for col_name in df.keys().to_list()]
        #     # print(len(df.keys().to_list()))
        #     insert_stores_query = gs.grocery_stores_table.insert().values(store_id=i,
        #                                                         BUSINESS_NAME=row['BUSINESS_NAME'],
        #                                                         DBA_NAME=row['DBA_NAME'],
        #                                                         open_timing=row['open_timing'],
        #                                                         closing_timing=row['closing_timing'],
        #                                                         contact_no=int(row['contact_no']),
        #                                                         payment_option=row['payment_option'],
        #                                                         delivery_option=row['delivery_option'],
        #                                                         rating=row['rating'],
        #                                                         date_time=row['date_time'],
        #                                                         id=i
        #           )
        insert_stores_query = gs.grocery_stores_table.insert().values(df_ls)
        session.execute(insert_stores_query)

        
            # session.execute(insert_raw_addresses_query)
            # breaks
        session.commit()

if __name__=='__main__':
    df = clean_df()
    # print(df.columns)
    temp = create_dummy_data(df)
    # temp = temp[['STREET_ADDRESS', 'CITY', 'STATE', 'ZIP_CODE', 'date_time']]
    print(temp.columns)
    create_addresses_tables(temp.dropna())
    create_stores_tables(temp.dropna())
    

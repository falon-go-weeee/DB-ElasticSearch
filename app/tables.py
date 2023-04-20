from sqlalchemy import Table, Column, Integer, String, MetaData, select, join, CheckConstraint, ExceptionContext


metadata = MetaData()

# stores_table = Table('stores', metadata,
#                     Column('id', Integer, primary_key=True, autoincrement='auto'),
#                     Column('gender', String(10), nullable=False),
#                     Column('age_group', String(10), nullable=False),
#                     Column('material', String(50)),
#                     Column('season', String(10)),
#                     Column('rating', Integer,),
#                     CheckConstraint('rating > 0', name='valid_rating')
#                     )

# addresses_table = Table('raw_addresses', metadata,
#                         Column('id', Integer, primary_key=True, autoincrement='auto'),
#                         Column('address', String(50), nullable=False)
#                         )

# products_table = Table('fashion_stores', metadata,
#     Column('id', Integer, primary_key=True),
#     Column('gender', String(10)),
#     Column('age_group', String(10)),
#     Column('material', String(50)),
#     Column('season', String(10)),
#     Column('rating', Integer)
# )

grocery_stores_table = Table('grocery_stores', metadata,
                            Column('id', Integer, primary_key=True, autoincrement='auto'),
                            Column('BUSINESS_NAME', String(100)),
                            Column('DBA_NAME', String(100)),
                            Column('open_timing', String(70)),
                            Column('closing_timing', String(70)),
                            Column('contact_no', String(15)),
                            Column('payment_option', String(70)),
                            Column('delivery_option', String(70)),
                            Column('rating', Integer),
                            Column('date_time', String(20))
                            )

store_addresses_table = Table('store_addresses', metadata,
                            Column('id', Integer, primary_key=True, autoincrement='auto'),
                            Column('BUILDING_NO',String(10)),
                            Column('STREET_ADDRESS', String(70)),
                            Column('CITY', String(50)),
                            Column('STATE', String(20)),
                            Column('ZIP_CODE', String(10)),
                            Column('date_time', String(20))
                            )

raw_address_table = Table('store_raw_addresses', metadata,
                            Column('id', Integer, primary_key=True, autoincrement='auto'),
                            Column('ADDRESS', String(200))
                            )

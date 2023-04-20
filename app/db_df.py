import pandas as pd
from sqlalchemy import Table, Column, Integer, String, select, join, CheckConstraint
from sqlalchemy.exc import DatabaseError
from connect_db import connect_db
from logger import timed, logging

sql = connect_db('grocery')

def table_to_dict(table, query):
    Session = sql.session()
    ls = []
    with Session() as session:
        result = session.execute(query).fetchall()
        # ls.extend(result)
        # print(result)
    # with sql.engine.connect() as conn:
    #     res = conn.execute(select(table))
    #     col_names = list(res.keys())
    #     # print(list(col_names))
    # dd =[]
    # for data in ls:
    #     # print(data)
    #     dd.append(dict(zip(col_names,data)))
    dd = to_df(result).to_dict()
    # print(dd)
    return dd

def to_df(dic):
    df = pd.DataFrame(dic)
    return df


# Postgres wrapper

import psycopg2 as db
import pandas as pd
from uuid import uuid4
import os
from psycopg2.extras import execute_values


class SQLConnection:
    def __init__(
        self,
        dbname,
        user,
        host,
        password
    ):
        self.current_cursor = str(uuid4())
        self.db_name = f''
        self.auth = dict(
            dbname=dbname,
            user=user,
            host=host,
            password=password
        )

    def q(self, query: str):
        """Executes a query and returns the result"""
        res = None
        with db.connect(**self.auth) as con:
            cur = con.cursor()
            for q in query.split(";"):
                try:
                    res = pd.read_sql_query(q.strip(), con)
                except (TypeError, ValueError):
                    pass
        return res
    
    def batch_insert(self, vals:dict, table:str):
        """Executes a batch insert into an existing table"""
        columns = vals[0].keys()

        if table == "rides":
            query = "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING".format(table,','.join(columns))
        else:
            query = "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING".format(table,','.join(columns))

        values = [[value for value in elems.values()] for elems in vals]

        with db.connect(**self.auth) as con:
            cur = con.cursor()
            execute_values(cur, query, values)
            con.commit()
            print("success")
            return({"success":"Values inserted into db"})


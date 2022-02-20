# -*- coding: utf-8 -*-
"""
Created on Sun Feb 20 06:28:59 2022

@author: Andy
"""

import sqlite3
import pandas as pd

#proj_path = "D:/Documents/archives/"
proj_path = ""
db_file = proj_path + "combatfootage.sqlite"
db_update_file = proj_path + "cf_update.sqlite"

con = sqlite3.connect(db_file)
con_update = sqlite3.connect(db_update_file)

update_db = pd.read_sql_query(f"SELECT * FROM comments", con_update)
con_update.close()

cf_db = pd.read_sql_query(f"SELECT * FROM comments", con)
cf_db_posts = pd.read_sql_query(f"SELECT * FROM posts", con)

x = update_db[~update_db.id.isin(cf_db.id)]
x = x[x.post_id.isin(cf_db_posts.id)]

x.to_sql("comments", con, if_exists = "append", index = False)
con.close()



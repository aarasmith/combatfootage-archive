# -*- coding: utf-8 -*-


import sqlite3
import pandas as pd
import yt_dlp
import sys
import time
import os


#proj_path = "D:/Documents/archives/"
proj_path = ""
db_file = proj_path + "combatfootage.sqlite"

ad_list = os.listdir("cf_vids")
ad_list = [x[:-4] for x in ad_list]

place=open(proj_path + 'place.txt','r').read()

con = sqlite3.connect(db_file)

cf_db = pd.read_sql_query(f"SELECT * FROM posts WHERE created_utc < {place}", con)
con.close()
cf_db = cf_db.fillna("0")
cf_db = cf_db.loc[~cf_db.link_flair_text.str.contains("Rule 8")]
cf_db = cf_db.loc[~cf_db.link_flair_text.str.contains("Rule 2")]
cf_db.sort_values(by=["created_utc"], inplace=True, ascending=False)
cf_db = cf_db.iloc[0:10000]
#cf_db = cf_db.iloc[0:5]
#cf_db = cf_db.sample(100)

cf_db = cf_db.loc[~cf_db.id.isin(ad_list)]

#no_download_ids = pd.DataFrame(columns=["id_", "exception_type", "exception_message"])
#downloaded_ids = pd.DataFrame(columns=["id_"])
if len(cf_db) > 0:
    no_download_ids = pd.read_parquet(proj_path + "no_download_list.parquet")
    downloaded_ids = pd.read_parquet(proj_path + "downloaded_list.parquet")
    
    try:
        for i in list(range(0, len(cf_db), 1)):
            entry = cf_db.iloc[i]
            url = entry.url
            file_name = proj_path + "cf_vids/" + entry.id
            ydl_opts = {
                "format": "bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best",
                "outtmpl": f"{file_name}.mp4",
                "cookiefile": "yt_cookies.txt",
                "noplaylist": True
                }
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    ydl.download([url])
            except BaseException as ex:
                try:
                    ex_type, ex_value = sys.exc_info()[0:2]
                    error = pd.DataFrame({"id_": [entry.id], "exception_type": [ex_type.__name__], "exception_message": [ex_value.msg]})
                except:
                    error = pd.DataFrame({"id_": [entry.id], "exception_type": ["exception error"], "exception_message": ["exception error"]})
                no_download_ids = pd.concat([no_download_ids, error])
                #no_download_ids.to_parquet(proj_path + "no_download_list.parquet")
            else:
                complete = pd.DataFrame({"id_": [entry.id]})
                downloaded_ids = pd.concat([downloaded_ids, complete])
                #downloaded_ids.to_parquet(proj_path + "downloaded_list.parquet")
    except:
        no_download_ids.to_parquet(proj_path + "no_download_list.parquet")
        downloaded_ids.to_parquet(proj_path + "downloaded_list.parquet")
    else:
        no_download_ids.to_parquet(proj_path + "no_download_list.parquet")
        downloaded_ids.to_parquet(proj_path + "downloaded_list.parquet")
        
    
    #no_download_ids.to_parquet(proj_path + "no_download_list.parquet")
    #downloaded_ids.to_parquet(proj_path + "downloaded_list.parquet")
    
    old_place = str(place)
    new_place = str(min(cf_db.created_utc))
    
    with open(proj_path + "place.txt", 'w') as f:
        f.write(new_place)
    
    print(f"Log entry: Downloaded from {old_place} to {new_place} at {time.time()}")

else:
    print("Log entry: No more entries to download")
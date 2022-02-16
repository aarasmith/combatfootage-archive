# -*- coding: utf-8 -*-
"""

install wkhtmltopdf
ffmpeg
youtube_dl
"""

import youtube_dl
import pdfkit
import sqlite3
import pandas as pd
import re
import datetime
from bs4 import BeautifulSoup
import requests
import urllib.request

proj_path = "D:/Documents/scw_old_archive/"
db_file = proj_path + "syriancivilwar.db"

con = sqlite3.connect(db_file)
cur = con.cursor()

scw_db = pd.read_sql_query("SELECT * FROM submissions", con)
scw_db = scw_db.fillna("0")
nyt = scw_db[scw_db["url"].str.contains("nytimes")]
reut = scw_db[scw_db["url"].str.contains("reuters.com")]
bbc = scw_db[scw_db["url"].str.contains("bbc.co.uk")]
sana = scw_db[scw_db["url"].str.contains("sana.sy")]
isw = scw_db[scw_db["url"].str.contains("understandingwar.org")]


re.search("\/([A-Za-z,-]*)\.html", test_cap).group(1)


def nyt_scrape():
    path = proj_path + "nyt/"
    errors = pd.DataFrame(columns = ["idint", "error_type"])
    completed = pd.DataFrame(columns = ["idint"])
    for i in list(range(0, len(nyt))):
        regex_error = False
        entry = nyt.iloc[i]
        if entry.idint not in completed:
            id_ = entry.idint
            url = entry.url
            try:
                url = re.search("(.*\.html)", url).group(1)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "url regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                title = re.search("\/(.[^\/]*)\.html", url).group(1)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "title regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                date = re.search("\/([0-9]*\/[0-9]*\/[0-9]*)\/[A-Za-z]", url).group(1).replace("/", "-")
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "date regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            file = path + date + "-" + title + "-id=" + str(id_) + ".pdf"
            
            if not regex_error:
                try:
                    pdfkit.from_url(url, file)
                except:
                    this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                    errors = pd.concat([errors, this_error])
                    print(f"pdfkit error on {id_}")
                else:
                    completed = pd.concat([completed, pd.DataFrame({"idint": [id_]})])
                    print(f"completed {id_}")
            else:
                print(f"regex error on {id_}")
            
    
    completed.to_parquet(path + "completed.parquet")
    errors.to_parquet(path + "errors.parquet")

reut1 = reut.head()

def reut_scrape(reut, completed = None):
    path = proj_path + "reuters/"
    errors = pd.DataFrame(columns = ["idint", "error_type"])
    if completed is None:
        completed = pd.DataFrame(columns = ["idint"])
    for i in list(range(0, len(reut))):
        regex_error = False
        entry = reut.iloc[i]
        if entry.idint not in list(completed.idint):
            id_ = entry.idint
            url = entry.url
            try:
                pat = re.compile("\/([0-9]*\/[0-9]*\/[0-9]*\/)")
                url_new = re.sub(pat, "/", url)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "url regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                title = entry.title[0:75].rstrip()
                title = re.sub("[^\s\w]", "", title)
                title = re.sub("[^\w]", "-", title)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "title regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                date = re.search("\/([0-9]*\/[0-9]*\/[0-9]*)\/", url).group(1).replace("/", "-")
            except:
                try:
                    date = datetime.datetime.fromtimestamp(entry.created)
                    date = str(date)[0:10]
                except:
                    this_error = pd.DataFrame({"idint": [id_], "error_type": "date regex error"})
                    errors = pd.concat([errors, this_error])
                    regex_error = True
            file = path + date + "-" + title + "-id=" + str(id_) + ".pdf"
            
            if not regex_error:
                try:
                    pdfkit.from_url(url_new, file)
                except:
                    this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                    errors = pd.concat([errors, this_error])
                    print(f"pdfkit error on {id_}")
                else:
                    completed = pd.concat([completed, pd.DataFrame({"idint": [id_]})])
                    print(f"completed {id_}")
            else:
                print(f"{this_error.error_type.values[0]} on {id_}")
            
    
    completed.to_parquet(path + "completed.parquet")
    errors.to_parquet(path + "errors.parquet")



def bbc_scrape(bbc, completed = None):
    path = proj_path + "bbc/"
    errors = pd.DataFrame(columns = ["idint", "error_type"])
    if completed is None:
        completed = pd.DataFrame(columns = ["idint"])
    for i in list(range(0, len(bbc))):
        regex_error = False
        entry = bbc.iloc[i]
        if entry.idint not in list(completed.idint):
            id_ = entry.idint
            url = entry.url
            try:
                title = entry.title[0:75].rstrip()
                title = re.sub("[^\s\w]", "", title)
                title = re.sub("[^\w]", "-", title)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "title regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                date = datetime.datetime.fromtimestamp(entry.created)
                date = str(date)[0:10]
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "date regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            file = path + date + "-" + title + "-id=" + str(id_) + ".pdf"
            
            if not regex_error:
                try:
                    pdfkit.from_url(url, file)
                except:
                    this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                    errors = pd.concat([errors, this_error])
                    print(f"pdfkit error on {id_}")
                else:
                    completed = pd.concat([completed, pd.DataFrame({"idint": [id_]})])
                    print(f"completed {id_}")
            else:
                print(f"{this_error.error_type.values[0]} on {id_}")
            
    
    completed.to_parquet(path + "completed.parquet")
    errors.to_parquet(path + "errors.parquet")


def sana_scrape(sana, completed = None):
    path = proj_path + "sana/"
    errors = pd.DataFrame(columns = ["idint", "error_type"])
    if completed is None:
        completed = pd.DataFrame(columns = ["idint"])
    for i in list(range(0, len(sana))):
        regex_error = False
        entry = sana.iloc[i]
        if entry.idint not in list(completed.idint):
            id_ = entry.idint
            url = entry.url
            try:
                title = entry.title[0:75].rstrip()
                title = re.sub("[^\s\w]", "", title)
                title = re.sub("[^\w]", "-", title)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "title regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                date = datetime.datetime.fromtimestamp(entry.created)
                date = str(date)[0:10]
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "date regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            file = path + date + "-" + title + "-id=" + str(id_) + ".pdf"
            
            if not regex_error:
                try:
                    pdfkit.from_url(url, file)
                except:
                    this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                    errors = pd.concat([errors, this_error])
                    print(f"pdfkit error on {id_}")
                else:
                    completed = pd.concat([completed, pd.DataFrame({"idint": [id_]})])
                    print(f"completed {id_}")
            else:
                print(f"{this_error.error_type.values[0]} on {id_}")
            
    
    completed.to_parquet(path + "completed.parquet")
    errors.to_parquet(path + "errors.parquet")


def isw_scrape(isw, completed = None):
    a_list = []
    path = proj_path + "isw/"
    errors = pd.DataFrame(columns = ["idint", "error_type"])
    if completed is None:
        completed = pd.DataFrame(columns = ["idint"])
    for i in list(range(0, len(isw))):
        regex_error = False
        entry = isw.iloc[i]
        if entry.idint not in list(completed.idint):
            id_ = entry.idint
            url = entry.url
            if "post.understanding" in url:
                url = re.sub("post\.understanding", "understanding", url)
            try:
                title = entry.title[0:75].rstrip()
                title = re.sub("[^\s\w]", "", title)
                title = re.sub("[^\w]", "-", title)
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "title regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            try:
                date = datetime.datetime.fromtimestamp(entry.created)
                date = str(date)[0:10]
            except:
                this_error = pd.DataFrame({"idint": [id_], "error_type": "date regex error"})
                errors = pd.concat([errors, this_error])
                regex_error = True
            file = path + date + "-" + title + "-id=" + str(id_) + ".pdf"
            
            if not regex_error:
                bs_error = False
                if ".pdf" in url:
                    try:
                        urllib.request.urlretrieve(url, file)
                    except:
                        this_error = pd.DataFrame({"idint": [id_], "error_type": "urllib error"})
                        errors = pd.concat([errors, this_error])
                        print(f"urllib error on {id_}")
                if ".jpg" in url:
                    try:
                        file = path + date + "-" + title + "-id=" + str(id_) + ".jpg"
                        urllib.request.urlretrieve(url, file)
                    except:
                        this_error = pd.DataFrame({"idint": [id_], "error_type": "urllib error"})
                        errors = pd.concat([errors, this_error])
                        print(f"urllib error on {id_}")
                else:
                    try:
                        response = requests.get(url)
                        soup = BeautifulSoup(response.text, "html.parser")
                        links = soup.find_all("a")
                        a_num = 0
                        for link in links:
                            if(".pdf" in link.get("href", [])):
                                this_a = link.get("href", [])
                                if this_a not in a_list:
                                    a_list += [this_a]
                                    a_num += 1
                                    attachment = path + date + "-" + "attachment" + str(a_num) + "-id=" + str(id_) + ".pdf"
                                    response = requests.get(link.get("href"))
                                    pdf = open(attachment, "wb")
                                    pdf.write(response.content)
                                    pdf.close()
                    except:
                        this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                        errors = pd.concat([errors, this_error])
                        bs_error = True
                        print(f"BS attachment error on {id_}")
                    try:
                        pdfkit.from_url(url, file)
                    except:
                        this_error = pd.DataFrame({"idint": [id_], "error_type": "pdfkit error"})
                        errors = pd.concat([errors, this_error])
                        print(f"pdfkit error on {id_}")
                    else:
                        if not bs_error:
                            completed = pd.concat([completed, pd.DataFrame({"idint": [id_]})])
                            print(f"completed {id_}")
            else:
                print(f"{this_error.error_type.values[0]} on {id_}")
            
    
    completed.to_parquet(path + "completed.parquet")
    errors.to_parquet(path + "errors.parquet")


file = path + date + "-" + "attachment" + "-id=" + str(id_) + ".pdf"
url = "https://www.understandingwar.org/backgrounder/battle-aleppo"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")    
links = soup.find_all("script")
i = 0
for link in links:
    if ('.pdf' in link.get('href', [])):
        i += 1
        response = requests.get(link.get('href'))
        pdf = open("test"+str(i)+".pdf", 'wb')
        pdf.write(response.content)
        pdf.close()

urllib.request.urlretrieve("http://www.understandingwar.org/sites/default/files/Damascus-Aug2013_0.jpg", "filename.jpg")
urllib.request.urlretrieve("http://understandingwar.org/sites/default/files/SectarianandRegionalConflictintheMiddleEast_3JUL.pdf", "test1.pdf")

"http://players.brightcove.net/665003303001/nahv11Vw34_default/index.html?videoId=2558803258001"
for link in links:
    if ('https://players.brightcove.net' in str(link)):
        my_link = re.search('https://players.brightcove.net[^"]*', str(link))[0]
        print(my_link)


subprocess.run(["C:\\Program Files\\Git\\git-bash.exe", "-l", "update.src"], cwd="D:\\Documents\\archives\\")

try:
    with yt_dlp.YoutubeDL() as ydl:
        ydl.download("asdfjsadf.com")
except BaseException as ex:
    # Get current system exception
    ex_type, ex_value = sys.exc_info()[0:2]

    
    error = pd.DataFrame({"exception_type": [ex_type.__name__], "exception_message": [ex_value]})
import urllib.request
import json
import pyodbc
from datetime import date

SetId = ''
DrugName = ''

conn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "Server=10.10.20.109;"
    "Database=DailyMedDB_Live;"
    "UID=sa;"
    "PWD=Admin!@#20;"
    # "Trusted_Connection=yes;"
)


def ImportData(DrugName, SetId):
    cursor_new = conn.cursor()
    cursor_new.execute("select SetId from Spl_Ids where SetId='"+SetId+"'")
    flag= True
    for row in cursor_new:
        flag= False
        print(SetId)
        print("Allready Exist")
    if(flag==True):
        cursor = conn.cursor()
        print(SetId)
        print("Insert")
        cursor.execute(
            'INSERT INTO Spl_Ids (Drug_Name,SetId,AddedOn) VALUES (?,?,?)',
            (DrugName, SetId,date.today())
        )
        conn.commit()

def Scrapper():
    global TotalPages
    with urllib.request.urlopen("https://dailymed.nlm.nih.gov/dailymed/services/v2/spls") as url:
        Data = json.loads(url.read().decode())
        TotalPages = Data["metadata"]["total_pages"]

    for pageid in range(TotalPages):
        print("PageId:"+str(pageid))
        # print("https://dailymed.nlm.nih.gov/dailymed/services/v2/spls?pagesize=100&page="+str(pageid+1))
        with urllib.request.urlopen("https://dailymed.nlm.nih.gov/dailymed/services/v2/spls?pagesize=100&page="+str(pageid+1)) as url:
            Data = json.loads(url.read().decode())
            for index, allData in enumerate(Data["data"]):
                DrugName = allData["title"]
                SetId = allData["setid"]
                ImportData(DrugName, SetId)

Scrapper()


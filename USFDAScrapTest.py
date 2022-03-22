import requests
from datetime import datetime
import pyodbc

ApprovalDate = ''
DrugName = ''
Type = ''
Number = ''
Submission = ''
ActiveIngredients = ''
Company = ''
SubmissionClassification = ''
SubmissionStatus = ''


conn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "Server=Developer3;"
    "Database=DailyMedDB;"
    "UID=sa;"
    "PWD=Admin!@#20;"
    # "Trusted_Connection=yes;"
)


def ImportData(ApprovalDate, DrugName, Type, Number, Submission,ActiveIngredients,Company,SubmissionClassification,SubmissionStatus):
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO Us_Fda_Data (Approval_Date,Drug_Name,Type,Number,Submission,Active_Ingredients,Company,Submission_Classification,Submission_Status) VALUES (?,?,?,?,?,?,?,?,?)',
        (ApprovalDate, DrugName, Type, Number, Submission,ActiveIngredients,Company,SubmissionClassification,SubmissionStatus)
    )
    conn.commit()

def UsfdaScrapper():
    global LastDate
    LastDate=''
    StartYear=1900
    EndYear=datetime.today().year+1
    StartMonth=1
    cursor_new = conn.cursor()
    cursor_new.execute("select MAX(CONVERT(date,Approval_Date)) from Us_Fda_Data")
    for row in cursor_new:
        LastDate = row[0]
        StartYear = LastDate.year
        StartMonth = LastDate.month    
    YearChangeFlag= False
    for year in range (StartYear,EndYear):
        if YearChangeFlag == True:
            StartMonth=1
        for month in range(StartMonth,13):
            YearChangeFlag= True
            url = 'https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=reportsSearch.process&rptName=0&reportSelectMonth='+str(month)+'&reportSelectYear='+str(year)
            response = requests.get(url)
            if response.status_code == 200:
                output = response.content.decode("utf-8")
                output=output.split('>')
                for abc, line in enumerate(output):
                    if "tbody" in line:
                        index= abc
                        while True:
                            # print("New")
                            # print(output[index+3].split('<')[0])
                            # print(output[index+6].split('<')[0])
                            # print(output[index+7].split('<')[0].split('#')[0].strip())
                            # print(output[index+7].split('<')[0].split('#')[1].strip())
                            # print(output[index+10].split('<')[0])
                            # print(output[index+12].split('<')[0])
                            # print(output[index+14].split('<')[0])
                            # print(output[index+16].split('<')[0])
                            # print(output[index+18].split('<')[0])

                            date_obj = datetime.strptime(str(output[index+3].split('<')[0]), '%m/%d/%Y')
                            date = date_obj.strftime('%d-%b-%Y')
                            ApprovalDate = date
                            DrugName = output[index+6].split('<')[0]
                            Type = output[index+7].split('<')[0].split('#')[0].strip()
                            Number = output[index+7].split('<')[0].split('#')[1].strip()
                            Submission = output[index+10].split('<')[0]
                            ActiveIngredients = output[index+12].split('<')[0]
                            Company = output[index+14].split('<')[0]
                            SubmissionClassification = output[index+16].split('<')[0]
                            SubmissionStatus = output[index+18].split('<')[0]
                            CompareApprovalDate = date_obj.strftime('%Y-%m-%d')
                            d1 = datetime(int(CompareApprovalDate.split('-')[0]), int(CompareApprovalDate.split('-')[1]), int(CompareApprovalDate.split('-')[2]))
                            d2 = datetime(LastDate.year,LastDate.month,LastDate.day)
    
                            if (d1 > d2):
                                print("Insert")
                                ImportData(ApprovalDate, DrugName, Type, Number, Submission,ActiveIngredients,Company,SubmissionClassification,SubmissionStatus)
                            else:
                                print("Allready exist")
                            index+=19
                            # print(index)
                            if "/tbody" in output[index+1]:
                                break
                        break
        
            

UsfdaScrapper()

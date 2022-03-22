import requests
from lxml import etree
from datetime import datetime
import pyodbc
import json
import urllib.request


# Database Connection
conn = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "Server=10.10.20.109;"
    "Database=DailyMedDB_Live;"
    "UID=sa;"
    "PWD=Admin!@#20;"
    "MARS_Connection=Yes;"
    # "Trusted_Connection=false;"
)
conn_new = pyodbc.connect(
    "Driver={SQL Server Native Client 11.0};"
    "Server=10.10.20.109;"
    "Database=DailyMedDB_Live;"
    "UID=sa;"
    "PWD=Admin!@#20;"
    "MARS_Connection=Yes;"
    # "Trusted_Connection=yes;"
)


def ReadSetId():
    cursor_new = conn_new.cursor()
    cursor_new.execute("select SetId from Spl_Ids where SetId not in(select SetId from Daily_Med_Data) and SetId not in(select SetId from Invalid_SetId_Data)")
    # i = 1
    for row in cursor_new:
        # DataScrapper('162f9ede-2818-40b6-8ccb-ece81fefcc5e')
        print(row[0])
        DataScrapper(row[0])
        # if i == 1:
        #     exit()
        # i += 1

# UpdateSalesDataForExistingData updated code is in API with sync offline
def UpdateSalesDataForExistingData():
    DailyMedId=''
    CompanyName=''
    ActiveIngredients=''
    ActiveStrength=''
    Packaging=''
    Repackager=''
    LabelerName=''
    ManufacturerName=''

    cursor = conn_new.cursor()
    cursor.execute("select d.DailyMedId,d.Company_Name,ai.Active_Ingredient,ai.Strength,p.Packaging,d.Repackager,d.Labeler_Name,esmd.Manufacturer_Name From Daily_Med_Data d left join Active_Ingredient ai on d.DailyMedId =ai.DailyMedId left join Packaging p on d.DailyMedId =p.DailyMedId left join EstablshmentManufacure_Data esmd on d.DailyMedId =esmd.DailyMedId where d.DailyMedId not in(select distinct Daily_Med_Id from Sales_Data)")
    for row in cursor:
        DailyMedId=row[0]
        CompanyName=row[1].replace("'","''")
        ActiveIngredients=row[2].replace("'","''")
        ActiveStrength=row[3].replace("'","''")
        Packaging=row[4]
        Repackager=row[5]
        LabelerName=row[6]
        ManufacturerName=row[7]
        RLMFlag=True
        print("Processed Sales Data for DailyMedId:"+str(DailyMedId))  
        
        #Check same company name is exist or not if it's not blank
        if Repackager !=None and Repackager !="" and CompanyName.lower() not in Repackager.lower():
            RLMFlag=False 
        if LabelerName !=None and LabelerName !="" and CompanyName.lower() not in LabelerName.lower():
            RLMFlag=False                 
        if ManufacturerName !=None and ManufacturerName !="" and CompanyName.lower() not in ManufacturerName.lower():
            RLMFlag=False         
        
        #check if alone and if Company Name is there into Repackager,LabelerName,ManufacturerName
        if ";" not in ActiveIngredients and RLMFlag:  
            InsertSalesData(CompanyName,ActiveIngredients,ActiveStrength,DailyMedId,Packaging)     

def InsertSalesData(CompanyName,ActiveIngredients,ActiveStrength,DailyMedId,Packaging):
    ActiveStrength=ActiveStrength.replace(" ","").replace("'","")
    Country=''
    Sector=''
    IntStrength=''
    IntPack=''
    IntPackSize=''
    PatentExpiryDate=''
    CountingUnits2016=''
    USDollarMNF2016=''
    CountingUnits2017=''
    USDollarMNF2017=''
    CountingUnits2018=''
    USDollarMNF2018=''
    # if Corporation = Company Name,Country = US,Molecule_List = Active_Ingredient(Alone),Int_Strength=Active_Strength
    cursor1 = conn_new.cursor()
    cursor1.execute(
        "select Country,Sector,Int_Strength,Int_Pack,Int_Pack_Size,Patent_Expiry_Date,Counting_Units_2016,US_Dollar_MNF_2016,Counting_Units_2017,US_Dollar_MNF_2017,Counting_Units_2018,US_Dollar_MNF_2018 from Sales_Data_Input where Corporation='"+CompanyName+"' and  Country='US' and Molecule_List='"+ActiveIngredients+"' and Int_Strength = '"+ActiveStrength+"'")
    for row in cursor1:
        Country=row[0]
        Sector=row[1]
        IntStrength=row[2]
        IntPack=row[3]
        IntPackSize=row[4]
        PatentExpiryDate=row[5]
        CountingUnits2016=row[6]
        USDollarMNF2016=row[7]
        CountingUnits2017=row[8]
        USDollarMNF2017=row[9]
        CountingUnits2018=row[10]
        USDollarMNF2018=row[11]

        # Check IntPackSize is exist in Packaging
        if IntPackSize in Packaging:  
            cursor = conn.cursor()
            cursor.execute(
            'insert into Sales_Data(Daily_Med_Id,Country,Sector,Int_Strength,Int_Pack,Patent_Expiry_Date,Counting_Units_2016,US_Dollar_MNF_2016,Counting_Units_2017,US_Dollar_MNF_2017,Counting_Units_2018,US_Dollar_MNF_2018) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)',
            (DailyMedId,Country,Sector,IntStrength,IntPack,PatentExpiryDate,CountingUnits2016,USDollarMNF2016,CountingUnits2017,USDollarMNF2017,CountingUnits2018,USDollarMNF2018)
            )
            conn.commit()  
            print("Added Sales Data for DailyMedId:"+str(DailyMedId))  

# UpdateTherapeuticCategoryForExistingData updated code is in API with sync offline
def UpdateTherapeuticCategoryForExistingData():
    DailyMedId=''
    ActiveIngredients=''
    cursor = conn_new.cursor()
    cursor.execute("select d.DailyMedId,ai.Active_Ingredient From Daily_Med_Data d left join Active_Ingredient ai on d.DailyMedId =ai.DailyMedId")
    for row in cursor:
        DailyMedId=row[0]
        ActiveIngredients=row[1].replace("'","''")
        print("Processed UpdateTherapeuticCategory for DailyMedId:"+str(DailyMedId))  
        UpdateTherapeuticCategory(DailyMedId,ActiveIngredients)

def UpdateTherapeuticCategory(DailyMedId,ActiveIngredients): 
    TherapeuticCategory=''
    SubCategory=''
    cursor = conn_new.cursor()
    cursor.execute("select distinct Therapeutic_Category,Sub_Category from Therapy_Index_USFDA_Data where Active_Ingredients='"+ActiveIngredients+"'")
    for row in cursor:
        TherapeuticCategory=row[0].replace("'","''")
        SubCategory=row[1].replace("'","''")
        break
    cursor = conn.cursor()
    cursor.execute("update Daily_Med_Data set Therapeutic_Category='"+TherapeuticCategory+"',Subcategory='"+SubCategory+"' where DailyMedId="+str(DailyMedId))
    conn.commit()  
    print("Updated TherapeuticCategory for DailyMedId:"+str(DailyMedId))

def ImportData(conn):
    cursor = conn.cursor()
    cursor.execute(
        'INSERT INTO Daily_Med_Data (SetId,Approval_Year,Approval_Date,Filing_Type,Application_Number,Brand_Name,Sub_Route_Of_Administration,Dosage,Type_of_Dosage_Form,Type_Of_Release,Therapeutic_Category,Marketing_Status,Marketing_Start_Date,Product_Image,Category,Label_Image,Submission,Submission_Type,Submission_Status,User_Created_Timestamp,User_Created_Id,User_Modified_Timestamp,User_Modified_Id,Registrant_Name,Labeler_Name,Manufactured_For,Manufactured_By,Distributed_By,Company_Name,Subsidiaries,Repackager,Therapeutic_Equivalents,Route_Of_Administration) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
        (SetId, Year, ApprovalDate, NdaAndaBla, ApplicationNumber, BrandName, SubRouteOfAdministration, Dosage, TypeofDosageForm, TypeOfRelease, TherapeuticCategory, MarketingStatus,
        MarketingStartDate, ProductImage, Category, LabelImage, Submission, SubmissionType, SubmissionStatus, UserCreatedTimestamp, UserCreatedId, UserModifiedTimestamp, UserModifiedId, RegisterantName, LablerName, ManufacturedFor, ManufacturedBy, DistributedBy,CompanyName,Subsidiaries,Repackager,TherapeuticEquivalents,RouteOfAdministration)
    )
    # Getting id of latest inserted record
    cursor.execute("SELECT @@IDENTITY AS ID;")
    DailyMedId = format(cursor.fetchone()[0])

    InsertActiveIngradients(DailyMedId)
    InsertInActiveIngradients(DailyMedId)
    InsertEstablshmentManufacures(DailyMedId)
    InsertPackaging(DailyMedId)
    # InsertPatentExclusivity(DailyMedId)
    Images(DailyMedId)
    conn.commit()

def InsertInvalidSetId(SetId,Reason):
    cursor = conn.cursor()
    cursor.execute('INSERT INTO Invalid_SetId_Data (SetId,Reason) VALUES (?,?)',(SetId, Reason))
    conn.commit()

def DataScrapper(XMLSetId):
    # variable declarations
    global SetId
    global Year
    global ApprovalDate
    global NdaAndaBla
    global ApplicationNumber
    global BrandName
    global SubRouteOfAdministration
    global RouteOfAdministration
    global Dosage
    global TypeOfRelease
    global TherapeuticCategory
    global Subcategory
    global ManufacturerName
    global ManufacturerAddress
    global ManufacturingCountry
    global PatentName
    global MarketingStartDate
    global ProductImage
    global Category
    global LabelImage
    global Submission
    global SubmissionType
    global SubmissionStatus
    global PatentNo
    global ExpirationDate
    global UserCreatedTimestamp
    global UserCreatedId
    global UserModifiedTimestamp
    global UserModifiedId
    global AIStrength
    global IIStrength
    global ActiveIngredients
    global InActiveIngredients
    global AiUniiNumber
    global IiUniiNumber
    global DUNSNumber
    global NdcCodeList
    global PackageDescriptionList
    global RegisterantName
    global EstablishmentList
    global EstablishmentDUNSNumber
    global LablerName
    global ManufacturerName
    global ManufacturerAddress
    global ManufacturingCountry
    global TypeofDosageForm
    global ManufacturedFor
    global ManufacturedBy
    global DistributedBy
    global allData
    global ImageName
    global ImageUrl
    global CompanyName 
    global Subsidiaries
    global Repackager
    global TherapeuticEquivalents
    global MarketingStatus
    MarketingStatus=''
    ManufacturerName = ''
    ManufacturerAddress = ''
    ManufacturingCountry = ''
    ManufacturedFor = ''
    ManufacturedBy = ''
    DistributedBy = ''
    SetId = ''
    Year = ''
    ApprovalDate = ''
    NdaAndaBla = ''
    ApplicationNumber = ''
    BrandName = ''
    SubRouteOfAdministration = ''
    RouteOfAdministration = ''
    Dosage = ''
    TypeOfRelease = ''
    TherapeuticCategory = ''
    Subcategory = ''
    ManufacturerName = ''
    ManufacturerAddress = ''
    ManufacturingCountry = ''
    PatentName = ''
    MarketingStartDate = ''
    ProductImage = ''
    Category = ''
    LabelImage = ''
    Submission = ''
    SubmissionType = ''
    SubmissionStatus = ''
    PatentNo = ''
    ExpirationDate = ''
    TypeofDosageForm = ''
    UserCreatedTimestamp = datetime.now()
    UserCreatedId = 'Admin'
    UserModifiedTimestamp = datetime.now()
    UserModifiedId = 'Admin'
    UNII_Number = ''
    AIStrength = []
    IIStrength = []
    DUNSNumber = ''
    NdcCodeList = []
    PackageDescriptionList = []
    EstablishmentDUNSNumber = []
    RegisterantName = ''
    LablerName = ''
    EstablishmentList = []
    ActiveIngredients = []
    InActiveIngredients = []
    AiUniiNumber = []
    IiUniiNumber = []
    allData = []
    ImageName = []
    ImageUrl = []
    CompanyName = ''
    Subsidiaries = ''
    Repackager = ''
    TherapeuticEquivalents =''

    SetId = XMLSetId
    url = 'https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/'+SetId+'.xml'

    response = requests.get(url)

    if response.status_code == 200:

        withoutnamespace = response.content.replace(
            'urn:hl7-org:v3'.encode(), ''.encode())
        
        validXmlFlag=True
        try:
            if withoutnamespace.decode() != 'None':
                tree = etree.fromstring(withoutnamespace)
        except:
            validXmlFlag=False

        if withoutnamespace.decode() != 'None' and validXmlFlag==True:
            tree = etree.fromstring(withoutnamespace)

            # Get Component Index Begins
            ComponentIndex = 1
            result = tree.xpath(
                '/document/component/structuredBody/component[1]/section/code/@displayName')
            if len(result) > 0:
                if result[0].lower() != "SPL listing data elements section".lower() and result[0].lower() != "SPL PRODUCT DATA ELEMENTS SECTION".lower():
                    i = 1
                    while True:
                        result = tree.xpath(
                            '/document/component/structuredBody/component['+str(i)+']/section/code/@displayName')
                        if len(result) > 0:
                            if result[0].lower() == "SPL listing data elements section".lower() or result[0].lower() == "SPL PRODUCT DATA ELEMENTS SECTION".lower():
                                ComponentIndex = i
                                break
                            i += 1
                        if len(result) == 0:
                            break
            # Get Component Index Ends

            result = tree.xpath('/document/component/structuredBody/component['+str(
                ComponentIndex)+']/section/subject/manufacturedProduct/subjectOf[2]/approval/code/@displayName')
            if len(result) > 0:
                NdaAndaBla = result[0]

            if len(result) == 0:
                result = tree.xpath('/document/component/structuredBody/component['+str(
                    ComponentIndex)+']/section/subject/manufacturedProduct/subjectOf[1]/approval/code/@displayName')
                if len(result) > 0:
                    NdaAndaBla = result[0]

            # if NdaAndaBla == "NDA" or NdaAndaBla == "ANDA" or NdaAndaBla == "BLA" or NdaAndaBla == "NADA" or NdaAndaBla == "ANADA":
            if True: #Get All types of products
                print(SetId)
                print(response.status_code)

                # Check Number of products on same page begins
                SubjectCount=1
                result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject')
                if len(result) > 0:
                    SubjectCount=len(result)
                # Check Number of products on same page ends   

                for SubjectIndex in range(SubjectCount): # Scrap and insert multiple products from same page
                    SubjectIndex=SubjectIndex+1
                    print("Product:"+str(SubjectIndex))

                    ManufacturerName = ''
                    ManufacturerAddress = ''
                    ManufacturingCountry = ''
                    ManufacturedFor = ''
                    ManufacturedBy = ''
                    DistributedBy = ''
                    Year = ''
                    ApprovalDate = ''
                    NdaAndaBla = ''
                    ApplicationNumber = ''
                    BrandName = ''
                    SubRouteOfAdministration = ''
                    Dosage = ''
                    TypeOfRelease = ''
                    TherapeuticCategory = ''
                    Subcategory=''
                    ManufacturerName = ''
                    ManufacturerAddress = ''
                    ManufacturingCountry = ''
                    PatentName = ''
                    MarketingStartDate = ''
                    ProductImage = ''
                    Category = ''
                    LabelImage = ''
                    Submission = ''
                    SubmissionType = ''
                    SubmissionStatus = ''
                    PatentNo = ''
                    ExpirationDate = ''
                    TypeofDosageForm = ''
                    UserCreatedTimestamp = datetime.now()
                    UserCreatedId = 'Admin'
                    UserModifiedTimestamp = datetime.now()
                    UserModifiedId = 'Admin'
                    UNII_Number = ''
                    AIStrength = []
                    IIStrength = []
                    DUNSNumber = ''
                    NdcCodeList = []
                    PackageDescriptionList = []
                    EstablishmentDUNSNumber = []
                    RegisterantName = ''
                    LablerName = ''
                    EstablishmentList = []
                    ActiveIngredients = []
                    InActiveIngredients = []
                    AiUniiNumber = []
                    IiUniiNumber = []
                    allData = []
                    ImageName = []
                    ImageUrl = []
                    CompanyName = ''
                    Subsidiaries = ''
                    Repackager = ''
                    TherapeuticEquivalents =''

                    result = tree.xpath('/document/code/@displayName')
                    if len(result) > 0:
                        Category = result[0]

                    if "ANIMAL".lower() in Category.lower():
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[1]/marketingAct/effectiveTime/low/@value')
                        
                        if len(result) > 0:
                            datetimeobject = datetime.strptime(result[0], '%Y%m%d')
                            newformat = datetimeobject.strftime('%d-%b-%Y')
                            ApprovalDate = newformat
                            Year = datetimeobject.year
                        if len(result) == 0:
                            result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[2]/marketingAct/effectiveTime/low/@value')
                            
                            if(len(result) > 0):
                                try:
                                    datetimeobject = datetime.strptime(result[0], '%Y%m%d')
                                    newformat = datetimeobject.strftime('%d-%b-%Y')
                                    ApprovalDate = newformat
                                    Year = datetimeobject.year
                                except:
                                    print("Invalid Approval date")    

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[2]/approval/code/@displayName')
                    if len(result) > 0:
                        NdaAndaBla = result[0]

                    if len(result) == 0:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[1]/approval/code/@displayName')
                        if len(result) > 0:
                            NdaAndaBla = result[0]

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[2]/approval/id/@extension')
                    if len(result) > 0:
                        ApplicationNumber = result[0].replace(
                            'ANDA', '').replace('NDA', '').replace('BLA', '').replace('ANADA','').replace('NADA','')
                        # ReadUsfdaData(ApplicationNumber)
                    if len(result) == 0:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[1]/approval/id/@extension')
                        if len(result) > 0:
                            ApplicationNumber = result[0].replace(
                                'ANDA', '').replace('NDA', '').replace('BLA', '').replace('ANADA','').replace('NADA','')
                            # ReadUsfdaData(ApplicationNumber)
                    
                    # Therapeutic Equivalents for ANDA begins  
                    if ApplicationNumber != '' and NdaAndaBla=='ANDA':
                        url = 'https://www.accessdata.fda.gov/scripts/cder/daf/index.cfm?event=overview.process&ApplNo='+ApplicationNumber+''
                        response = requests.get(url)
                        if response.status_code == 200:
                            output = response.content.decode("utf-8")
                            output=output.split('>')
                            TherapeuticEquivalentsRemark=False
                            for index, line in enumerate(output):
                                if "Therapeutic Equivalents".lower() in line.lower():
                                    TherapeuticEquivalentsRemark=True
                                if "tbody" in line and TherapeuticEquivalentsRemark==True: 
                                    # print(output[index+13].split('<')[0])
                                    # print(output[index+18].split('<')[0])
                                    RLD=output[index+13].split('<')[0]
                                    if RLD.lower()=='Yes'.lower():
                                        TherapeuticEquivalents = output[index+18].split('<')[0]
                                    break    
                    # Therapeutic Equivalents for ANDA ends      
                    
                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/name')
                    if len(result) > 0:
                        if result[0].text != None:
                            BrandName = result[0].text

                            result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/name/suffix')
                            if len(result) > 0:
                                if result[0].text != None:
                                    BrandName = BrandName + " " + result[0].text

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient/ingredientSubstance/name')
                    if len(result) > 0:
                        AllIngredients = [ingredient.text for ingredient in result]

                    else:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part/partProduct/ingredient/ingredientSubstance/name')
                        if len(result) > 0:
                            AllIngredients = [ingredient.text for ingredient in result]

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient/ingredientSubstance/code/@code')
                    if len(result) > 0:
                        AllIngredientsUNII = [ingredient for ingredient in result]

                    else:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part/partProduct/ingredient/ingredientSubstance/code/@code')
                        if len(result) > 0:
                            AllIngredientsUNII = [ingredient for ingredient in result]

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient/@classCode')
                    if len(result) > 0:
                        AllIngredientsType = [ingredient for ingredient in result]

                    else:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part/partProduct/ingredient/@classCode')
                        if len(result) > 0:
                            AllIngredientsType = [ingredient for ingredient in result]

                    if len(result) > 0:
                        for index, ingredient in enumerate(AllIngredients):

                            # Get Strength Begins
                            Strength = ''
                            result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient['+str(index+1)+']/quantity/numerator/@value')
                            if len(result) > 0:
                                Strength = Strength + result[0]
                            else:
                                result = tree.xpath('/document/component/structuredBody/component['+str(
                                    ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part['+str(index+1)+']/partProduct/ingredient/quantity/numerator/@value')
                                if len(result) > 0:
                                    Strength = Strength + result[0]
                            
                            result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient['+str(index+1)+']/quantity/numerator/@unit')
                            if len(result) > 0:
                                Strength = Strength + " " + result[0]
                            else:
                                result = tree.xpath('/document/component/structuredBody/component['+str(
                                    ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part['+str(index+1)+']/partProduct/ingredient/quantity/numerator/@unit')
                                if len(result) > 0:
                                    Strength = Strength + " " + result[0]

                            resultIn = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient['+str(index+1)+']/quantity/denominator/@value')
                            if len(resultIn) <= 0:
                                resultIn = tree.xpath('/document/component/structuredBody/component['+str(
                                    ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part['+str(index+1)+']/partProduct/ingredient/quantity/denominator/@value')

                            result = tree.xpath('/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/ingredient['+str(index+1)+']/quantity/denominator/@unit')
                            if len(result) > 0:
                                if result[0] != "1":
                                    Strength = Strength + " in " + \
                                        resultIn[0] + " " + result[0]
                            else:
                                result = tree.xpath('/document/component/structuredBody/component['+str(
                                    ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/part['+str(index+1)+']/partProduct/ingredient/quantity/denominator/@unit')
                                if len(result) > 0:
                                    if result[0] != "1":
                                        Strength = Strength + " in " + \
                                            resultIn[0] + " " + result[0]

                            # Get Strength Ends

                            if AllIngredientsType[index] == "ACTIB" or AllIngredientsType[index] == "ACTIM" or AllIngredientsType[index] == "ACTIR":
                                ActiveIngredients.append(ingredient)
                                AiUniiNumber.append(
                                    ingredient + " : " + AllIngredientsUNII[index])
                                AIStrength.append(Strength)
                            if AllIngredientsType[index] == "IACT":
                                InActiveIngredients.append(ingredient)
                                IiUniiNumber.append(
                                    ingredient + " : " + AllIngredientsUNII[index])
                                IIStrength.append(Strength)

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/consumedIn/substanceAdministration/routeCode/@displayName')
                    if len(result) > 0:
                        SubRouteOfAdministration = result[0]

                    else:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct//manufacturedProduct/part/consumedIn/substanceAdministration/routeCode/@displayName')
                        if len(result) > 0:
                            SubRouteOfAdministration = result[0]

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/formCode/@displayName')
                    if len(result) > 0:
                        Dosage = result[0].split(',')[0]

                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/formCode/@displayName')
                    if len(result) > 0:
                        if "," in result[0]:
                            TypeOfRelease = result[0].split(',')[1]
                            if "Delayed".lower() not in TypeOfRelease.lower() and "Extended".lower() not in TypeOfRelease.lower() and "Immediate".lower() not in TypeOfRelease.lower():
                                TypeOfRelease = ''

                    # Chewing/Film Coated/For Suspension/Liquid Filled are Type of Dosage Form
                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/formCode/@displayName')
                    if len(result) > 0:
                        if "," in result[0]:
                            TypeofDosageForm = result[0].split(',')[1]
                            if "Coated".lower() not in TypeofDosageForm.lower() and "Chewing".lower() not in TypeofDosageForm.lower() and "Film Coated".lower() not in TypeofDosageForm.lower() and "For Suspension".lower() not in TypeofDosageForm.lower() and "Liquid Filled".lower() not in TypeofDosageForm.lower() and "chewable".lower() not in TypeofDosageForm.lower():
                                TypeofDosageForm = ''

                    # Read Purpose begins
                    # i = 1
                    # while True:
                    #     result = tree.xpath(
                    #         '/document/component/structuredBody/component['+str(i)+']/section/code/@displayName')
                    #     if len(result) > 0:
                    #         if result[0].lower() == "OTC - PURPOSE SECTION".lower():
                    #             result = tree.xpath(
                    #                 '/document/component/structuredBody/component['+str(i)+']/section/text/paragraph')
                    #             if len(result) > 0:
                    #                 AllParagraphs = [para.text for para in result]
                    #                 for line in AllParagraphs:
                    #                     if line != None:
                    #                         TherapeuticCategory = TherapeuticCategory + line + ", "
                    #                 TherapeuticCategory = "PURPOSE: "+TherapeuticCategory
                    #             break
                    #         i += 1
                    #     if len(result) == 0:
                    #         break

                    # Read Indications and Usage begins
                    # i = 1
                    # while True:
                    #     result = tree.xpath(
                    #         '/document/component/structuredBody/component['+str(i)+']/section/code/@displayName')

                    #     if len(result) > 0:
                    #         if result[0].lower() == "INDICATIONS & USAGE SECTION".lower() or result[0].lower() == "VETERINARY INDICATIONS SECTION".lower():
                    #             result = tree.xpath(
                    #                 '/document/component/structuredBody/component['+str(i)+']/section')
                    #             if len(result) > 0:
                    #                 output = etree.tostring(
                    #                     result[0], method='html', with_tail=False)
                    #                 output = output.split('>'.encode())
                    #                 for line in output:
                    #                     if not line.decode("utf-8").strip().startswith("<"):
                    #                         TherapeuticCategory = TherapeuticCategory + \
                    #                             line.decode(
                    #                                 "utf-8").strip().split('<')[0] + " "
                                            
                    #             break
                    #         i += 1
                    #     if len(result) == 0:
                    #         break
                    # Read Indications and Usage ends

                    # Begin Indications and Usage FOR SPL UNCLASSIFIED SECTION
                    # i = 1
                    # while True:
                    #     result = tree.xpath(
                    #         '/document/component/structuredBody/component['+str(i)+']/section/code/@displayName')

                    #     if len(result) > 0:
                    #         if result[0].lower() == "SPL UNCLASSIFIED SECTION".lower():
                    #             result = tree.xpath(
                    #         '/document/component/structuredBody/component['+str(i)+']/section/title')
                    #             if len(result) > 0:
                    #                 if result[0].text != None and result[0].text.lower() == "INDICATIONS AND USAGE".lower():
                    #                     result = tree.xpath(
                    #                         '/document/component/structuredBody/component['+str(i)+']/section')
                    #                     if len(result) > 0:
                    #                         output = etree.tostring(
                    #                             result[0], method='html', with_tail=False)
                    #                         output = output.split('>'.encode())
                    #                         for line in output:
                    #                             if not line.decode("utf-8").strip().startswith("<"):
                    #                                 TherapeuticCategory = TherapeuticCategory + \
                    #                                     line.decode(
                    #                                         "utf-8").strip().split('<')[0] + " "              
                    #                     break
                    #         i += 1
                    #     if len(result) == 0:
                    #         break
                    # Read Indications and Usage FOR SPL UNCLASSIFIED SECTION ends
                    
                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[1]/marketingAct/effectiveTime/low/@value')
                    if len(result) > 0:
                        try:
                            datetimeobject = datetime.strptime(result[0], '%Y%m%d')
                            newformat = datetimeobject.strftime('%d-%b-%Y')
                            MarketingStartDate = newformat
                        except:
                            print("Invalid marketing start date")    

                    if len(result) == 0:
                        result = tree.xpath('/document/component/structuredBody/component['+str(
                            ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/subjectOf[2]/marketingAct/effectiveTime/low/@value')
                        if(len(result) > 0):
                            try:  #31 June is invalid date 04f67d13-d0fa-409c-94bb-0f0e780bc812                
                                datetimeobject = datetime.strptime(result[0], '%Y%m%d')
                                newformat = datetimeobject.strftime('%d-%b-%Y')
                                MarketingStartDate = newformat
                            except:
                                print("Invalid marketing start date")    

                    result = tree.xpath('/document/code/@displayName')
                    if len(result) > 0:
                        Category = result[0]

                    id = tree.xpath(
                        '/document/author/assignedEntity/representedOrganization/id /@extension')
                    result = tree.xpath(
                        '/document/author/assignedEntity/representedOrganization/name')
                    if len(result and id) > 0:
                        AllParagraphs = [para.text for para in result]
                        AllParagraphs.append(id[0])
                        LablerName = AllParagraphs[0]+'('+AllParagraphs[1]+')'

                    id = tree.xpath(
                        '/document/author/assignedEntity/representedOrganization/assignedEntity/assignedOrganization/id/@extension')
                    result = tree.xpath(
                        '/document/author/assignedEntity/representedOrganization/assignedEntity/assignedOrganization/name')
                    if len(result and id) > 0:
                        AllParagraphs = [para.text for para in result]
                        AllParagraphs.append(id[0])
                        RegisterantName = AllParagraphs[0]+'('+AllParagraphs[1]+')'

                    result = tree.xpath('/document/author/assignedEntity/representedOrganization/assignedEntity/assignedOrganization/assignedEntity/assignedOrganization/id/@extension')
                    if len(result) > 0:
                        EstablishmentDUNSNumber = result

                    # NDC code and packaging begins
                    result = tree.xpath('/document/component/structuredBody/component['+str(
                        ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/asContent')
                    if len(result) > 0:
                        AllAsContent = [asContent for asContent in result]
                        for index, asContent in enumerate(AllAsContent):
                            ContainerCount = 0
                            BasePath = '/document/component/structuredBody/component['+str(
                                ComponentIndex)+']/section/subject['+str(
                                SubjectIndex)+']/manufacturedProduct/manufacturedProduct/asContent['+str(index+1)+']/'
                            output = etree.tostring(
                                asContent, method='html', with_tail=False)
                            output = output.split('>'.encode())
                            for line in output:
                                if "/containerPackagedProduct".lower() in line.decode("utf-8").strip().lower():
                                    ContainerCount += 1

                            for counter in range(ContainerCount, 0, -1):
                                NDCCode = ''
                                PackageDes = ''

                                TempPath = 'containerPackagedProduct/'
                                for loop in range(counter-1):
                                    TempPath = TempPath+'asContent/containerPackagedProduct/'
                                NDCCodePath = BasePath+TempPath+'code/@code'
                                PackaageDisplayNamePath = BasePath+TempPath+'formCode/@displayName'

                                result = tree.xpath(NDCCodePath)
                                if len(result) > 0:
                                    NDCCode = result[0]
                                NdcCodeList.append(NDCCode)

                                TempPath = ''
                                for loop in range(counter-1):
                                    TempPath = TempPath+'containerPackagedProduct/asContent/'
                                PackageNumeratorValuePath = BasePath+TempPath+'quantity/numerator/@value'
                                PackageNumeratorUnitPath = BasePath+TempPath+'quantity/numerator/@unit'
                                PackageDenominatorValuePath = BasePath+TempPath+'quantity/denominator/@value'
                                PackageDenominatorUnitPath = BasePath+TempPath+'quantity/denominator/@unit'

                                result = tree.xpath(PackageNumeratorValuePath)
                                if len(result) > 0:
                                    PackageDes = result[0]
                                result = tree.xpath(PackageNumeratorUnitPath)
                                if len(result) > 0:
                                    if result[0] != "1":
                                        PackageDes = PackageDes + ' ' + result[0]

                                result = tree.xpath(PackageDenominatorValuePath)
                                if len(result) > 0:
                                    PackageDes = PackageDes + ' in ' + result[0]
                                result = tree.xpath(PackageDenominatorUnitPath)
                                if len(result) > 0:
                                    if result[0] != "1":
                                        PackageDes = PackageDes + ' ' + result[0]
                                result = tree.xpath(PackaageDisplayNamePath)
                                if len(result) > 0:
                                    PackageDes = PackageDes + ' ' + result[0]

                                if counter == 1:
                                    result = tree.xpath(
                                        BasePath+'subjectOf/characteristic/value/@displayName')
                                    if len(result) > 0:
                                        PackageDes = PackageDes + '; ' + result[0]

                                PackageDescriptionList.append(PackageDes)

                    # NDC code and packaging ends

                    # ManufacturedFor, ManufacturedBy and DistributedBy Begins
                    # url = 'https://dailymed.nlm.nih.gov/dailymed/fda/fdaDrugXsl.cfm?setid='+SetId+'&type=display'
                    # response = requests.get(url)
                    # if response.status_code == 200:
                    #     output = response.content.decode("utf-8")
                    #     output=output.split('>')
                    #     for index, line in enumerate(output):
                    #         if "Manufactured for".lower() in line.lower():
                    #             p=index
                    #             while(True):
                    #                 ManufacturedFor = ManufacturedFor + output[p].split('<')[0].strip() + ' '
                    #                 p+=1
                    #                 if "Section".lower() in output[p].lower() or "Figure".lower() in output[p].lower() or "Manufactured by".lower() in output[p].lower() or "Distributed by".lower() in output[p].lower():
                    #                     break

                    #         if "Manufactured by".lower() in line.lower():
                    #             p=index
                    #             while(True):
                    #                 ManufacturedBy = ManufacturedBy + output[p].split('<')[0].strip() + ' '
                    #                 p+=1
                    #                 if "Section".lower() in output[p].lower() or "Figure".lower() in output[p].lower() or "Manufactured for".lower() in output[p].lower() or "Distributed by".lower() in output[p].lower():
                    #                     break

                    #         if "Distributed by".lower() in line.lower():
                    #             p=index
                    #             while(True):
                    #                 DistributedBy = DistributedBy + output[p].split('<')[0].strip() + ' '
                    #                 print(output[p].split('<')[0].strip() + ' ')
                    #                 p+=1
                    #                 abc = "<div data-sectioncode="'42229-5'""
                    #                 if "Section".lower() in abc or "Figure".lower() in output[p].lower() or "Manufactured for".lower() in output[p].lower() or "Manufactured by".lower() in output[p].lower():
                    #                     break
                    # ManufacturedFor, ManufacturedBy and DistributedBy Ends

                    with urllib.request.urlopen('https://dailymed.nlm.nih.gov/dailymed/services/v2/spls/'+SetId+'/media.json') as url:
                        Data = json.loads(url.read().decode())
                        for index, allData in enumerate(Data["data"]["media"]):
                            Name = allData["name"]
                            ImageName.append(Name)
                            Url = allData["url"]
                            ImageUrl.append(Url)
                    
                    #Repackager
                    url = 'https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid='+SetId+''
                    response = requests.get(url)
                    if response.status_code == 200:
                        output = response.content.decode("utf-8")
                        output=output.split('>')
                        for strong, line in enumerate(output):
                            if "This is a repackaged label".lower() in line.lower():
                                result = tree.xpath('/document/author/assignedEntity/representedOrganization/name')
                                if len(result) > 0:
                                    Repackager = result[0].text
                                break    
                    #Get RouteOfAdministraion
                    RouteOfAdministration= GetRouteOfAdministartion(SubRouteOfAdministration)

                    #Convert to Upper Begins
                    NdaAndaBla=NdaAndaBla.upper()
                    BrandName=BrandName.upper()
                    Dosage=Dosage.upper()
                    SubRouteOfAdministration=SubRouteOfAdministration.upper()
                    RouteOfAdministration=RouteOfAdministration.upper()
                    TypeofDosageForm=TypeofDosageForm.upper()
                    TypeOfRelease=TypeOfRelease.upper()
                    CompanyName=CompanyName.upper()
                    Subsidiaries=Subsidiaries.upper()
                    Repackager=Repackager.upper()
                    LablerName=LablerName.upper()
                    MarketingStatus=MarketingStatus.upper()
                    Category=Category.upper()
                    SubmissionStatus=SubmissionStatus.upper()
                    #Convert to Upper Ends
                    #Assign NA for blank begins
                    if SubRouteOfAdministration=='':
                        SubRouteOfAdministration='NA'
                    if RouteOfAdministration=='':
                        RouteOfAdministration='NA'    
                    if Dosage=='':
                        Dosage='NA'   
                    if TypeofDosageForm=='':
                        TypeofDosageForm='NA'  
                    if TypeOfRelease=='':
                       TypeOfRelease='NA'        
                    #Assign NA for blank ends
                    #Correct Dosage and Type_Of_Dosage_Form begins
                    if Dosage.strip()=='C47916':
                        Dosage='INJECTION'
                    if Dosage.strip()=='FOR SOLUTION':
                        Dosage='SOLUTION'
                    if Dosage.strip()=='FOR SUSPENSION':
                        Dosage='SUSPENSION'
                    if Dosage.strip()=='INJECTABLE':
                        Dosage='INJECTION'            

                    if TypeofDosageForm.strip()=='FOR SUSPENSION':
                        TypeofDosageForm='SUSPENSION'
                    if TypeofDosageForm.strip()=='CHEWING':
                        TypeofDosageForm='CHEWABLE'
                    if TypeofDosageForm.strip()=='COATED':
                        TypeofDosageForm='FILM COATED'        
                    #Correct Dosage and Type_Of_Dosage_Form ends
                    ImportData(conn)
            else:
                print(SetId)
                print("InvalidNdaAndaBla:"+NdaAndaBla)
                # InsertInvalidSetId(SetId,"InvalidNdaAndaBla:"+NdaAndaBla)
        else:
            print(SetId)
            print("EmptyXmlFile")
            InsertInvalidSetId(SetId,"EmptyXmlFile")
    else:
        print(SetId)
        print(response.status_code)
        InsertInvalidSetId(SetId,"ResponseCode:"+str(response.status_code))

# Get Route of administration by SubRouteOfAdministration
def GetRouteOfAdministartion(SubRouteOfAdministration):
    output='' 
    cursor = conn_new.cursor()
    cursor.execute("select distinct Route_Of_Administration from Route_And_Sub_Route_Of_Administration where Sub_Route_Of_Administration='"+SubRouteOfAdministration+"'")
    for row in cursor:
        output=row[0]
        break
    return output
# Read Usfda data

def ReadUsfdaData(UsfdaApplicationNumber):
    global Year
    global Submission
    global SubmissionType
    global SubmissionStatus
    global MarketingStatus
    global ApprovalDate
    global CompanyName 
    Submission = ''
    SubmissionType = ''
    SubmissionStatus = ''
    MarketingStatus = ''
   #ApprovalDate = ''
    CompanyName = ''
    cursor = conn.cursor()
    cursor.execute(
        "select * from Us_Fda_Data Where Number='"+UsfdaApplicationNumber+"'")
    for row in cursor:
        # print(f'row = {row}')
        # date_obj = datetime.strptime(str(row[1]), '%m/%d/%Y')
        # date = date_obj.strftime('%d-%b-%Y')
        
        ApprovalDate = row[1]
        Submission = row[5]
        SubmissionType = row[8]
        if "ANIMAL".lower() in Category.lower() and (NdaAndaBla=="ANADA" or NdaAndaBla=="NADA"):
            CompanyName = ''
        else:    
            CompanyName  = row[7]
        SubmissionStatus = row[9]
        my_date = datetime.strptime(row[1], '%d-%b-%Y')
        Year = my_date.year
        if(Submission == 'ORIG-1'):
            break

    cursor1 = conn.cursor()
    cursor1.execute("SELECT MarketingStatusDescription from MarketingStatus INNER JOIN MarketingStatus_Lookup ON  MarketingStatus_Lookup.MarketingStatusID=MarketingStatus.MarketingStatusID where ApplNo='"+UsfdaApplicationNumber+"'")
    for rows in cursor1:
        MarketingStatus = rows[0]

# Insert PackageDescription & NdcCode


def InsertPackaging(DailyMedId):
    ndcList=''
    PackageList=''
    cursor = conn.cursor()
    for index, Package in enumerate(PackageDescriptionList):
        ndc = NdcCodeList[index]
        ndcList= ndcList + ndc + '; '
        PackageList= PackageList  + Package+ '; '
    
    ndcList=FormatText(ndcList)
    PackageList=FormatText(PackageList)

    cursor.execute(
        'INSERT INTO Packaging (DailyMedId,Packaging,NDC_Code,User_Created_Timestamp,User_Created_Id,User_Modified_Timestamp,User_Modified_Id) VALUES (?,?,?,?,?,?,?)',
        (DailyMedId, PackageList, ndcList, UserCreatedTimestamp,
         UserCreatedId, UserModifiedTimestamp, UserModifiedId)
    )
    conn.commit()

# Insert Images
def Images(DailyMedId):
    ImageNameList=''
    ImageUrlList=''
    cursor = conn.cursor()
    for index, Images in enumerate(ImageName):
        ImageNameList= ImageNameList  + Images+ '; '
        ImageUrlList = ImageUrlList  + ImageUrl[index] + '; '
    
    ImageNameList=FormatText(ImageNameList)
    ImageUrlList=FormatText(ImageUrlList)
    
    cursor.execute(
        'INSERT INTO Images (DailyMedId,Image_Name,Image_Url) VALUES (?,?,?)',
        (DailyMedId, ImageNameList, ImageUrlList)
    )
    conn.commit()

def FormatText(input):
    output=input
    while(output.endswith("; ")):
        output= output[:-2]
    output=str(output).upper()
    return output    

# Insert Active Ingradients
def InsertActiveIngradients(DailyMedId):
    ActiveIngredientsList=''
    AiUniiNumberList=''
    AIStrengthList=''
    cursor = conn.cursor()
    for index, Activeingredient in enumerate(ActiveIngredients):
        ActiveIngredientsList = ActiveIngredientsList + Activeingredient + '; '
        AiUniiNumberList= AiUniiNumberList  + AiUniiNumber[index] + '; '
        AIStrengthList= AIStrengthList + AIStrength[index] + '; '
   
    ActiveIngredientsList=FormatText(ActiveIngredientsList)
    AiUniiNumberList=FormatText(AiUniiNumberList) 
    AIStrengthList=FormatText(AIStrengthList)   

    cursor.execute(
        'INSERT INTO Active_Ingredient (DailyMedId,Active_Ingredient,UNII_Number,Strength,User_Created_Timestamp,User_Created_Id,User_Modified_Timestamp,User_Modified_Id) VALUES (?,?,?,?,?,?,?,?)',
        (DailyMedId, ActiveIngredientsList, AiUniiNumberList, AIStrengthList,
         UserCreatedTimestamp, UserCreatedId, UserModifiedTimestamp, UserModifiedId)
    )
    conn.commit()


# Insert InActive Ingradients
def InsertInActiveIngradients(DailyMedId):
    InActiveIngredientsList=''
    IiUniiNumberList=''
    IIStrengthList=''
    cursor = conn.cursor()
    for index, InActiveingredient in enumerate(InActiveIngredients):
        InActiveIngredientsList=InActiveIngredientsList+ InActiveingredient + '; '
        IiUniiNumberList=IiUniiNumberList + IiUniiNumber[index] + '; '
        IIStrengthList=IIStrengthList + IIStrength[index] + '; '
    
    InActiveIngredientsList=FormatText(InActiveIngredientsList)
    IiUniiNumberList=FormatText(IiUniiNumberList)
    IIStrengthList=FormatText(IIStrengthList)       
    
    cursor.execute(
        'INSERT INTO InActive_Ingredient (DailyMedId,In_Active_Ingredient,UNII_Number,Strength,User_Created_Timestamp,User_Created_Id,User_Modified_Timestamp,User_Modified_Id) VALUES (?,?,?,?,?,?,?,?)',
        (DailyMedId, InActiveIngredientsList, IiUniiNumberList, IIStrengthList,
         UserCreatedTimestamp, UserCreatedId, UserModifiedTimestamp, UserModifiedId)
    )
    conn.commit()

#  Insert Establshment Manufacure Data


def InsertEstablshmentManufacures(DailyMedId):
    cursor = conn.cursor()
    if len(EstablishmentDUNSNumber) > 0:
        establishmentDunsNumberList=''
        ManufacturerNameList=''
        ManufacturerAddressList=''
        ManufacturingCountryList=''
        for index, establishmentDunsNumber in enumerate(EstablishmentDUNSNumber):
            ManufacturerName=''
            address=''
            ManufacturerAddress=''
            ManufacturingCountry=''
            cursor.execute("select * from Manufacure_Data Where DUNS_NUMBER='" +
                           establishmentDunsNumber+"'")                      
            for row in cursor:
                # print(f'row = {row}')
                ManufacturerName = row[3]
                address = row[4].rsplit(',', 1)
                ManufacturerAddress = address[0]
                ManufacturingCountry = address[1]

            establishmentDunsNumberList= establishmentDunsNumberList + establishmentDunsNumber + '; '
            ManufacturerNameList= ManufacturerNameList + ManufacturerName + '; '
            ManufacturerAddressList= ManufacturerAddressList + ManufacturerAddress + '; '
            ManufacturingCountryList= ManufacturingCountryList + ManufacturingCountry + '; '

        establishmentDunsNumberList=FormatText(establishmentDunsNumberList)
        ManufacturerNameList=FormatText(ManufacturerNameList)
        ManufacturerAddressList=FormatText(ManufacturerAddressList)
        ManufacturingCountryList=FormatText(ManufacturingCountryList)
        
        cursor.execute(
            'INSERT INTO EstablshmentManufacure_Data(DailyMedId,Duns_Number,Manufacturer_Name,Manufacturer_Address,Manufacturing_Country) VALUES (?,?,?,?,?)',
            (DailyMedId, establishmentDunsNumberList, ManufacturerNameList,
             ManufacturerAddressList, ManufacturingCountryList)
        )       
    conn.commit()


#  Insert Patent Exclusivity Data
def InsertPatentExclusivity(DailyMedId):
    Patent_No = []
    Expiration_Date = []
    Exclusivity_Date = []
    ExclusivityData = []
    cursor = conn.cursor()
    cursor1 = conn.cursor()
    ApplicationNumber1 = ApplicationNumber.replace('0', '', 0)

    cursor.execute(
        "select distinct Patent_No,Patent_Expire_Date from Patent_Data Where Appl_No='"+ApplicationNumber1+"'")
    myresult = cursor.fetchall()
    for x in myresult:
        Patent_No.append(x[0])
        # Patent Expiration Year Correction Begins
        ExpDate= x[1].split('/')
        if ApprovalDate.split('-')[2] >= ExpDate[2]:
            ResultExpDate= ExpDate[0]+"/"+ExpDate[1]+"/" +str(int(ExpDate[2])+100)
            Expiration_Date.append(ResultExpDate)
            Patent_No.append(x[0])
            # Patent Expiration Year Correction Begins
        else:
            Expiration_Date.append(x[1])

    cursor1.execute(
        "select distinct Exclusivity_Date from Exclusivity_Data Where Appl_No='"+ApplicationNumber1+"'")
    myresult1 = cursor1.fetchall()
    for y in myresult1:
        Exclusivity_Date.append(y[0])

    PatentNo = Patent_No
    ExpirationDate = Expiration_Date
    ExclusivityDate = Exclusivity_Date

    PatentNameList=''
    patentNoList=''
    expirationDateList=''
    exclusivityDateList=''
    if len(PatentNo) > 0:   
        for index, PaNumber in enumerate(PatentNo):
            # if len(PaNumber) <= index:
            #     patentNo = ''
            # else:
            #     patentNo = PaNumber
            if PaNumber == None:
                patentNo = ''
            else:
                patentNo = PaNumber
            if len(ExpirationDate) <= index:
                expirationDate = ''
            else:
                expirationdate_obj = datetime.strptime(
                    str(ExpirationDate[index]), '%m/%d/%Y')
                expirationDate = expirationdate_obj.strftime('%d-%b-%Y')
            if len(ExclusivityDate) <= index:
                exclusivityDate = ''
            else:
                expirationdate_obj = datetime.strptime(
                    str(ExclusivityDate[index]), '%m/%d/%Y')
                exclusivityDate = expirationdate_obj.strftime('%d-%b-%Y')

            PatentNameList=PatentNameList+PatentName+'; '
            patentNoList=patentNoList+patentNo +'; '
            expirationDateList=expirationDateList+expirationDate +'; '
            exclusivityDateList=exclusivityDateList+ exclusivityDate +'; '
        
        PatentNameList= FormatText(PatentNameList)
        patentNoList=FormatText(patentNoList)
        expirationDateList=FormatText(expirationDateList)
        exclusivityDateList=FormatText(exclusivityDateList)
        
        cursor.execute(
            'INSERT INTO PatentExclusivity_Data(DailyMedId,Patent_Name,Patent_No,Expiration_Date,Exclusivity_Date) VALUES (?,?,?,?,?)',
            (DailyMedId, PatentNameList, patentNoList, expirationDateList, exclusivityDateList)
        )
        conn.commit()
    else:
        for index, exclusivityDate in enumerate(ExclusivityDate):
            # if len(PaNumber) <= index:
            #     patentNo = ''
            # else:
            #     patentNo = PaNumber
            if len(PatentNo) <= index:
                patentNo = ''
            else:
                patentNo = PatentNo[index]
            if len(ExpirationDate) <= index:
                expirationDate = ''
            else:
                expirationdate_obj = datetime.strptime(
                    str(ExpirationDate[index]), '%m/%d/%Y')
                expirationDate = expirationdate_obj.strftime('%d-%b-%Y')
            if len(ExclusivityDate) <= index:
                exclusivityDate = ''
            else:
                expirationdate_obj = datetime.strptime(
                    str(ExclusivityDate[index]), '%m/%d/%Y')
                exclusivityDate = expirationdate_obj.strftime('%d-%b-%Y')

            PatentNameList=PatentNameList+PatentName+'; '
            patentNoList=patentNoList+patentNo +'; '
            expirationDateList=expirationDateList+expirationDate +'; '
            exclusivityDateList=exclusivityDateList+ exclusivityDate +'; '    
        
        PatentNameList= FormatText(PatentNameList)
        patentNoList=FormatText(patentNoList)
        expirationDateList=FormatText(expirationDateList)
        exclusivityDateList=FormatText(exclusivityDateList)
      
        cursor.execute(
            'INSERT INTO PatentExclusivity_Data(DailyMedId,Patent_Name,Patent_No,Expiration_Date,Exclusivity_Date) VALUES (?,?,?,?,?)',
            (DailyMedId, PatentNameList, patentNoList, expirationDateList, exclusivityDateList)
        )
        conn.commit()

# Logic to insert EstablshmentManufacure_Data from existing data. Using Application Number
def InsertEstablshmentManufacuresUsingApplicationNumber():
    print("InsertEstablshmentManufacuresUsingApplicationNumber...")
    DailyMedId=''
    ApplicationNumber=''
    ManufacturerName=''
    ManufacturerAddress=''
    ManufacturingCountry=''
    cursor = conn.cursor()
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    cursor.execute("select DailyMedId,Application_Number from Daily_Med_Data where DailyMedId not in(select distinct DailyMedId from EstablshmentManufacure_Data)")
    for row in cursor:
        DailyMedId=row[0]
        ApplicationNumber=row[1]
        cursor1.execute(
            "select * from EstablshmentManufacure_Data  where DailyMedId in(select DailyMedId from Daily_Med_Data where Application_Number='" + ApplicationNumber+"')")
        for row in cursor1:
            if len(row) > 0:
                for index, data in enumerate(row):
                    DunsNumber = row[2]
                    ManufacturerName = row[3]
                    ManufacturerAddress = row[4]
                    ManufacturingCountry = row[5]
            cursor2.execute(
                'INSERT INTO EstablshmentManufacure_Data(DailyMedId,Duns_Number,Manufacturer_Name,Manufacturer_Address,Manufacturing_Country) VALUES (?,?,?,?,?)',
                (DailyMedId, DunsNumber, ManufacturerName,
                ManufacturerAddress, ManufacturingCountry)
                )
            break           
    conn.commit()     
        

ReadSetId()
# InsertEstablshmentManufacuresUsingApplicationNumber()

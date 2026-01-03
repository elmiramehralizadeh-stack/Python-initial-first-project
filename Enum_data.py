from enum import Enum

class names(Enum):
    Operational = 'Operational'
    COGS        = 'COGS'
    Quantity_Turnover = 'Quantity_Turnover'
    RawMaterial = 'RawMaterial'
    Overhead ='Overhead'
    Cost = 'Cost'
    Others = 'Others'
    Incoeme_Statment = 'Incoeme_Statment'
    Monthly_report = 'Monthly_report'

class sheets(Enum):
    Operational = "&SheetId=20"
    COGS        = "&SheetId=20"
    Quantity_Turnover = "&SheetId=21"
    RawMaterial = "&SheetId=21"
    Overhead = "&SheetId=22"
    Cost = "&SheetId=22"
    Others = "&SheetId=24"
    Incoeme_Statment = "&SheetId=1"
    Monthly_report = ""
    


class tabels(Enum):
    Operational = 1
    COGS = 5
    Quantity_Turnover= [1,2,3]
    RawMaterial = [4,5,6]
    Overhead = 1
    Cost = 1
    Others = 1
    Incoeme_Statment = 0
    Monthly_report = [0,1,2,4]

class cols(Enum):
    Operational = 2
    COGS = 0
    Quantity_Turnover = 20
    RawMaterial = 20
    Overhead = 20
    Cost = 20 
    Others = 0
    Incoeme_Statment = 5
    Monthly_report = 3
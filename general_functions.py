import re
import json
import requests
import polars as pl
from persiantools import digits

BASE_URL = 'https://codal.ir'

def getPeriod(results: list) -> int:
    for res in results: 
        try:
            if res == 'سال':
                return 12
            elif isinstance(int(res), int):
                return digits.fa_to_en(res)           
        except:
            continue

def has_empty_string(df: pl.DataFrame) -> bool:
    str_cols=[c for c, dt in zip(df.columns,df.dtypes) if dt == pl.Utf8]
    if not str_cols:
        return False
    
    df = df.with_columns(pl.col(pl.Utf8).replace("", "0").cast(pl.Int64)).filter(~pl.all_horizontal(pl.all() == 0))
    if df.height == 0:
        return True
    return False
    

def parse_date_persian(report_date: int) -> str:
    return f"{str(report_date)[:4]}%2F{str(report_date)[4:6]}%2F{str(report_date)[6:]}"

#=======================================
# قسمت جستجوی نماد

import requests

def get_company_name(symbol:str):

    url = "https://search.codal.ir/api/search/v1/companies"

    payload = {}
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Origin': 'https://codal.ir',
    'Connection': 'keep-alive',
    'Referer': 'https://codal.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Cookie': 'TS018fb0f7=01f9930bd292db3978fa6745f43db4d0f5ab81ff5b156066683c72d5f33f43e4bc61c124795af847e4d364adb3179924b444adeb82; Unknown=1076170924.20480.0000'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    response.json()

    for company in response.json():
        if symbol == company['sy']:
            return(company['n'])
        

def get_financial_years(symbol: str, year: int) -> list:
    name = get_company_name(symbol)
    url = f"https://search.codal.ir/api/search/v1/financialYears?Name={name}&Symbol={symbol}"

    payload = {}
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:146.0) Gecko/20100101 Firefox/146.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Origin': 'https://codal.ir',
    'Connection': 'keep-alive',
    'Referer': 'https://codal.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Cookie': 'TS018fb0f7=01f9930bd2b59609eb8fd991e794d235655d41c51e935441ea24044699c05f2d3bfa3c15e1c72d5f3df050098deeeb6298fda2d326; Unknown=1076170924.20480.0000'
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    response.json()
    years = []
    for date in response.json():
        date = date.split()[0].replace('/','')
        if int(date[0:4])>year:
            years.append(date)
    return sorted(years)


def get_results(symbol: str, parse_date: str, report_type: str, sheet_num: int ,sort: bool = True) -> json:

    category = 3 if report_type == 'Monthly_report' else 1 
    letter_type = 58 if report_type == 'Monthly_report' else 6
    
    url = f"https://search.codal.ir/api/search/v2/q?&Audited=true&AuditorRef=-1&Category={category}&Childs=false&CompanyState=0&CompanyType=-1&Consolidatable=true&IsNotAudited=false&Length=-1&LetterType={letter_type}&Mains=true&NotAudited=true&NotConsolidatable=true&PageNumber=1&Publisher=false&ReportingType=-1&Symbol={symbol}&TracingNo=-1&YearEndToDate={parse_date}&search=true"
    payload = {}
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Origin': 'https://www.codal.ir',
    'Connection': 'keep-alive',
    'Referer': 'https://www.codal.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Cookie': 'TS018fb0f7=01f9930bd2042dcf9847aa2bc829c3c06ffc32f99581aeb3be233ae7927f334551012da7bebd35160e4cfe5b1483035393c7c97f49; Unknown=1076170924.20480.0000'
    }

    response = requests.request("GET", url, headers=headers, data=payload)
    results = response.json()
    reports={}
    for result in results['Letters']:
        res_list = str(result['Title']).split()
        for res in res_list:
            # if res == 'سال':
            #     period = 12
            # elif len(res) == 1 and res!='و':
            #     period = digits.fa_to_en(res)
            if '/' in res:
                res = re.sub(r"\([^)]*\)", "", res)
                r = int(digits.fa_to_en(res).replace('/',''))
                if r not in reports.keys():
                    period = getPeriod(res_list)
                    p = int(digits.fa_to_en(result['PublishDateTime'].split()[0]).replace('/',''))
                    reports[r] = {'period': period, 'publish': p, 'title': result['Title'], 'url': BASE_URL+result['Url']+f"{sheet_num}"}

    sorted_reports = {}
    for date in sorted(reports.keys(), reverse=False):
        sorted_reports[date] = reports[date]
    
    return sorted_reports if sort else reports

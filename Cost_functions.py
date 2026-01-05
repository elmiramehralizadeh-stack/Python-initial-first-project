
import requests
import json
import re
import polars as pl
import Enum_data as ed
import general_functions as gf
from persiantools import characters, digits
pl.Config.set_tbl_rows(1000)

def get_table(url: str, desc: str):
    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:143.0) Gecko/20100101 Firefox/143.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate, br, zstd',
    'Origin': 'https://www.codal.ir',
    'Connection': 'keep-alive',
    'Referer': 'https://www.codal.ir/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Cookie': 'TS018fb0f7=01f9930bd2e2675d04882f623c888052df60031e7775d8e3c459dc4cb96bf8e870e0a9bf0e0eedff68bcce23fd9701c7a7fc0855c4; Unknown=1076170924.20480.0000'
    }
    response = requests.request("GET", url)
    statement = response.text
    pattern = r"var datasource = (.*?});"
    match = re.search(pattern, statement)
    if match:
        text = match.group(1)
    records = []
    records.append(
        (statement, text))
    for _, data in records:
        continue
    items = json.loads(data)['sheets']

    for ind, table in enumerate(items[0]['tables']):
        if characters.ar_to_fa(desc) == characters.ar_to_fa(str(table['title_Fa'])):
            break
    
    cells = items[0]['tables'][ind]['cells']
    return [(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in cells]

def create_dict_dataframes(url: str, date: int, report_type: str) -> dict:
    all_data ={ 'report_last_year' : pl.DataFrame(),
                'report_this_year': pl.DataFrame(),
                'est_remain': pl.DataFrame(),
                'est_next_year': pl.DataFrame()}
    
    cells_tuples = get_table(url, "هزینه های سربار و هزینه های عمومی و اداری شرکت")
    return cells_tuples
    dates = sorted(list(set([i[-1] for i in cells_tuples if i[-1] != ''])))
    for date_ in dates:
        filtered_cells = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == '' or i[-1] == date_]
        df = pl.from_records(filtered_cells, schema=["col", "row", "value"], orient="row")
        df = df.pivot(values="value", on="col", index="row").sort("row")
        for i, col in enumerate(df.columns):
            value = df[col][1]        # value from first row
            if(isinstance(value, str)):
                for val in value.split():
                    if'/' in val:
                        num = int(val.replace('/', ''))
                        if num < date :
                            all_data['report_last_year'] = df
                            #all_data['report_last_year'] = all_data['report_last_year'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
                        if num == date :
                            all_data['report_this_year'] = df
                            #all_data['report_this_year'] = all_data['report_this_year'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
                        else:
                            if(val[:4] == str(date)[:4]):
                                all_data['est_remain'] = df
                                #all_data['est_remain'] = all_data['est_remain'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))                          
                                
                                all_data['est_next_year'] = df
                                #all_data['est_next_year'] = all_data['est_next_year'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
    for key, value in all_data.items():
        if(value.is_empty()):
            all_data[key] = None
    return all_data

def creat_Cost_dataframe(symbol: str, url: str, date: int, period: int, publish: int) -> dict:
    all_data = create_dict_dataframes(url, date, 'Cost')

    col_name = ['description',
                'Wages and Salaries Expense',
                'Depreciation Expense',
                'Energy Expense (Water, Electricity, Gas, and Fuel)',
                'Consumable Materials Expense',
                'Advertising Expense',
                'Sales Commission and Brokerage Fee',
                'After-Sales Service Expense',
                'Doubtful Accounts Expense / Bad Debt Expense',
                'Transportation and Delivery Expense',
                'Other Expenses']
            
    for key, data in all_data.items():
        if data is None: continue
        data = data.transpose(include_header=False)
        data = data.drop(data.columns[12])
        data = data[3]
        table_date = [d for d in str(data.row(0)[1]).split() if '/' in d][0]
        data = data.drop(data.columns[1])
        data = data.rename(dict(zip(data.columns, col_name)))
        data = data.insert_column(0, pl.lit(symbol).alias("Symbol"))
        data = data.insert_column(1, pl.lit(int(digits.fa_to_en(table_date).replace('/', ''))).alias("Date"))
        data = data.insert_column(2, pl.lit(period).alias("Period"))
        data = data.insert_column(3, pl.lit(publish).alias("Publish"))
        all_data[key] = data 

    return all_data     
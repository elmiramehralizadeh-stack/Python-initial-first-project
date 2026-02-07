import re
import json
import requests
import polars as pl

from persiantools import characters, digits

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

    f1 = statement[statement.find('سال'):statement.find('سال')+200]
    found = re.search(r"(\d{4}/\d{2}/\d{2})", f1).group(1)
    fin_year = int(found.replace('/',''))

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
    return [(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in cells], fin_year

def create_dict_dataframes(url: str, date: int, report_type: str) -> dict:
    all_data ={ 'report_last_year' : pl.DataFrame(),
                'report_this_year': pl.DataFrame(),
                'est_remain': pl.DataFrame(),
                'est_next_year': pl.DataFrame()}
    
    cells_tuples, fin_year = get_table(url, "هزینه های سربار و هزینه های عمومی و اداری شرکت")
    dates = sorted(list(set([i[-1] for i in cells_tuples if i[-1] != ''])))

    col_num = -2 if report_type == 'Cost' else -1
    
    for date_ in dates:
        filtered_cells = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == '' or i[-1] == date_]
        df = pl.from_records(filtered_cells, schema=["col", "row", "value"], orient="row")
        df = df.pivot(values="value", on="col", index="row").sort("row")
        df = df[[col for col in df.columns if "0" != df[col].to_list()[-1]]]
        
        for _, col in enumerate(df.columns):
            value = df[col][1]        # value from first row
            if(isinstance(value, str)):
                for val in value.split():
                    if'/' in val:
                        num = int(val.replace('/', ''))
                        df = df.with_columns(pl.arange(len(df)).alias("row"))
                        df = df.fill_null(strategy="forward")
                        df = df.filter(pl.col('row') != 0)
                        df = df.with_columns(pl.arange(len(df)).alias("row"))
                        df = df.sort("row")
                        df = df.select(["row", *[col for col in df.columns if col != "row"]])
                        df = df.filter(pl.col(df.columns[1]) != "")
                        df = df.drop('row')
                        if num < date :
                            
                            all_data['report_last_year'] = df.drop(df.columns[col_num])
                        elif num == date :
                            all_data['report_this_year'] = df.drop(df.columns[col_num])
                        else:
                            
                            next_year_cols = []
                            estimate_cols = []
                            for column in df.columns:
                                if 'سال' in df[0][column][0]:
                                    next_year_cols.append(column)
                                else:
                                    estimate_cols.append(column)
                                                            
                            if(val[:4] == str(fin_year)[:4]):
                                all_data['est_remain'] = df[estimate_cols].drop(df[estimate_cols].columns[col_num])
                                                                
                                if len(next_year_cols) == 0:
                                    continue
                                
                                next_year_cols.append('1')
                                next_year_cols = sorted(next_year_cols)
                                all_data['est_next_year'] = df[next_year_cols].drop(df[next_year_cols].columns[col_num])

    for key, value in all_data.items():
        if(value.is_empty()):
            all_data[key] = None  
    return all_data


def creat_Overhead_Cost_dataframe(symbol: str, url: str, date: int, period: int, publish: int, report_type: str) -> dict:
    all_data = create_dict_dataframes(url, date, report_type)

    col_name = ['Wages and Salaries Expense',
                'Depreciation Expense',
                'Energy Expense (Water, Electricity, Gas, and Fuel)',
                'Consumable Materials Expense',
                'Advertising Expense',
                'Sales Commission and Brokerage Fee',
                'After-Sales Service Expense',
                'Doubtful Accounts Expense / Bad Debt Expense',
                'Transportation and Delivery Expense',
                'Other Expenses'
            ]

    for key, data in all_data.items():
        if data is None: continue
        data = data.transpose(include_header=False)
        data = data.drop(data.columns[-1])
        # take last row, first column
        raw_value = str(data.row(-1)[0])
        table_date = next((d for d in raw_value.split() if "/" in d), None)
        data = data.select(data.columns[1:])
        headers = data.row(0)
        data = data.rename(dict(zip(data.columns, headers))).slice(1)
        
        if len(data.columns) > 10:    
            fixed_cols = data.columns[:10]
            dynamic_cols = data.columns[10:]
            
            data = data.with_columns(
                pl.struct([pl.col(c).alias(c) for c in dynamic_cols])
                .alias("dynamic_data")).select(fixed_cols + ["dynamic_data"])
        
        data = data[:10].rename(dict(zip(data.columns, col_name)))
        data = data.insert_column(0, pl.lit(symbol).alias("Symbol"))
        data = data.insert_column(1, pl.lit(int(digits.fa_to_en(table_date).replace('/', ''))).alias("Date"))
        data = data.insert_column(2, pl.lit(period).alias("Period"))
        data = data.insert_column(3, pl.lit(publish).alias("Publish"))

        all_data[key] = data 

    return all_data
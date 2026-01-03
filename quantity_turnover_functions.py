import requests
import pandas as pd
import json
import re
import polars as pl
import Enum_data as ed
pl.Config.set_tbl_rows(1000)
from persiantools import characters, digits
import sqlite3
import general_functions as gf

def get_table(url: str, table: int):
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

    if isinstance(table, list):
        cells = []
        for t in table:
            raw_cells = items[0]['tables'][t]['cells']
            cells.append([(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in raw_cells])
        return [x for xs in cells for x in xs]
    
    cells = items[0]['tables'][table]['cells']
    return [(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in cells]

def create_dict_dataframes(url: str, date: int, report_type: str) -> dict:
    all_data ={ 'report_this_year': pl.DataFrame(),
                'est_remain': pl.DataFrame(),
                'est_next_year': pl.DataFrame()}

    cells_tuples = get_table(url, ed.tabels[report_type].value)
    dates = sorted(list(set([i[-1] for i in cells_tuples if i[-1] != ''])))
    for date_ in dates:
        products = []
        filtered_cells = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == date_]
        products = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == '']
        df = pl.from_records(filtered_cells, schema=["col", "row", "value"], orient="row")
        df = df.pivot(values="value", on="col", index="row").sort("row")
        
        df_products = pl.from_records(products, schema=["col", "row", "value"], orient="row")
        df_products = df_products.pivot(values="value", on="col", index="row").sort("row")

        df_products = df_products.filter(df_products['row']>=df['row'][0] ,df_products['row']<=df['row'][-1])
        df = df_products.join(df, on="row", how="left")

        for i, col in enumerate(df.columns):
            cols = ed.cols[report_type].value
            value = df[col][0]        # value from first row
            if(isinstance(value, str)):
                for val in value.split():
                    if'/' in val:
                        num = int(val.replace('/', ''))
                        next_cols = df.columns[i : i + cols + 1]
                        cols_to_select = ['1','2', *next_cols]
                        
                        if num == date :
                            all_data['report_this_year'] = df.select(cols_to_select)
                            all_data['report_this_year'] = all_data['report_this_year'].with_row_index("row")
                            all_data['report_this_year'] = all_data['report_this_year'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
                        else:
                            rows = len(all_data['report_this_year']['row'])
                            if(val[:4] == str(date)[:4]):
                                est_df = df.select(cols_to_select)
                                est_df = est_df.with_row_index("row")
                                all_data['est_remain'] = est_df.head(rows)
                                all_data['est_remain'] = all_data['est_remain'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))                          
                                
                                all_data['est_next_year'] = est_df.tail(rows)
                                all_data['est_next_year'] = all_data['est_next_year'].insert_column(-1, pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
    for key, value in all_data.items():
        if(value.is_empty()):
            all_data[key] = None
    return all_data


def create_Quantity_Turnover_dataframe(symbol: str, url: str, date: int, period: int, publish: int) -> dict:
    all_data = create_dict_dataframes(url, date, 'Quantity_Turnover')
    for key, data in all_data.items():
        if data is None: continue

        cols = data.columns
        sorted_cols = sorted(cols, key=lambda x: (0 if x in ["row","Date"] else 1, int(x) if x.isdigit() else float("inf")))
        data = data.select(sorted_cols)

        data = data.rename({old:str(i) for i, old in enumerate(data.columns)})
        cols = [str(i) for i in range(4,len(data.columns))]
        data = data.with_columns(pl.col(pl.Utf8).replace("", "0")).drop_nulls().with_columns(pl.col(cols).cast(pl.Int64))
        data = data.drop([col for col in cols if data[col].tail(1).item() != 0])
        
        data = data.rename({old:str(i) for i, old in enumerate(data.columns)})
        cols = [str(i) for i in range(4,len(data.columns))]
        data = data.with_columns(pl.col(pl.Utf8).replace("", "0")).filter(pl.sum_horizontal(cols) != 0)
        
        col_name = {'row' : '0',
                    'Date' : '1',
                    'Product' : '2',
                    'unit': '3',
                    "Beginning Inventory_qn": '4',
                    "Beginning Inventory_pr": '5',
                    "Production_qn": '6',
                    "Production_pr": '7',
                    "Adjustments_qn": '8',
                    "Adjustments_pr": '9',
                    "Sales_qn": '10',
                    "Sales_pr": '11',
                    "Ending_Inventory_qn": '12',
                    "Ending_Inventory": '13'
        }
        
        rename_map = {v: k for k, v in col_name.items()}
        data = data.rename(rename_map)
        
        data = data.insert_column(0, pl.lit(symbol).alias("Symbol"))
        data = data.insert_column(1, pl.lit(period).alias("Period"))
        data = data.insert_column(2, pl.lit(publish).alias("Publish"))
        cs = ['Symbol','Period','Publish','Date', 'Product' , 'unit',"Beginning Inventory_qn","Beginning Inventory_pr","Production_qn","Production_pr","Adjustments_qn","Adjustments_pr","Sales_qn","Sales_pr","Ending_Inventory_qn","Ending_Inventory" ]
        data = data[cs]
        all_data[key] = data
    return all_data 
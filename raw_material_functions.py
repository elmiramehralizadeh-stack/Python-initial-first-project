import requests
import json
import re
import polars as pl
import Enum_data as ed
import general_functions as gf
from persiantools import characters, digits


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
    
    # get fin_year from bs4
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
    
    if isinstance(table, list):
        cells = []
        for t in table:
            raw_cells = items[0]['tables'][t]['cells']
            cells.append([(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in raw_cells])
        return [x for xs in cells for x in xs], fin_year
    
    cells = items[0]['tables'][table]['cells']
    return [(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in cells], fin_year

def create_dict_dataframes(url: str, date: int, report_type: str) -> dict:
    all_data ={ 'report_this_year': pl.DataFrame(),
                'est_remain': pl.DataFrame(),
                'est_next_year': pl.DataFrame()}

    cells_tuples, fin_year = get_table(url, ed.tabels[report_type].value)

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
                            all_data['report_this_year'] = all_data['report_this_year'].insert_column(len(df.columns), pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))

                        elif num == fin_year:

                            df = df.with_columns(pl.col("1").map_elements(lambda x: characters.ar_to_fa(x)).alias("1"))
                            split_row = (df.filter(pl.col("1") == 'جمع کـل').select(pl.col("row").first()).item())
                            est_df_remain = df.filter(pl.col("row") <= split_row)
                            all_data['est_remain'] = est_df_remain
                            all_data['est_remain'] = all_data['est_remain'].insert_column(len(est_df_remain.columns), pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))
                        
                        elif num > fin_year:

                            df = df.with_columns(pl.col("1").map_elements(lambda x: characters.ar_to_fa(x)).alias("1"))
                            split_row = (df.filter(pl.col("1") == 'جمع کـل').select(pl.col("row").first()).item())
                            est_next_year = df.filter(pl.col("row") <= split_row)
                            all_data['est_next_year'] = est_next_year
                            all_data['est_next_year'] = all_data['est_next_year'].insert_column(len(est_next_year.columns), pl.lit(int(digits.fa_to_en(date_).replace('/', ''))).alias("Date"))

    
    for key, value in all_data.items():
        if(value.is_empty()):
            all_data[key] = None
    return all_data

def creat_raw_material_dataframe(symbol: str, url: str, date: int, period: int, publish: int) -> dict:
    all_data = create_dict_dataframes(url, date, 'RawMaterial')

    for key, data in all_data.items():
        if data is None: continue      
        cols = data.columns
        sorted_cols = sorted(cols, key=lambda x: (0 if x == "row" else 1, int(x) if x.isdigit() else float("inf")))
        sorted_cols = ['row', '1', '2', '3', '4', '6', '7', '9', '10', '12', '13']
        data = data.select(sorted_cols)
        
        df_date, df_period = gf.get_date_period(data)
        
        col_name = {
            "Raw Material": '1',
            'Unit': '2',
            "Beginning Inventory_qn": '3',
            "Beginning Inventory_pr": '4',
            "Purchases During the Period_qn": '6',
            "Purchases During the Period_pr": '7',
            "Consumption_qn": '9',
            "Consumption_pr": '10',
            "Ending Inventory_qn": '12',
            "Ending Inventory_pr": '13'
        }
        
        data = data.with_columns(data["1"].map_elements(lambda x: characters.ar_to_fa(x)).alias('1'))
        idx_internal_raw = data.filter(data['1'] == "مواد اولیه داخلی:")['row'][0]
        idx_total_internal_raw= data.filter(data['1']== "جمع مواد اولیه داخلی")['row'][0]
        df_internal_raw = data.filter(data['row']>idx_internal_raw,data['row']<idx_total_internal_raw-1)
        df_internal_raw=df_internal_raw.with_columns(pl.lit("داخلی").alias("Type"))
        idx_export_raw = data.filter(data['1'] == "مواد اولیه وارداتی:")['row'][0]
        idx_total_export_raw= data.filter(data['1']== "جمع مواد اولیه وارداتی")['row'][0]
        df_export_raw = data.filter(data['row']>idx_export_raw,data['row']<idx_total_export_raw-1)
        df_export_raw=df_export_raw.with_columns(pl.lit("وارداتی").alias("Type"))
        df_goods = pl.concat([df_internal_raw,df_export_raw])

        rename_map = {v: k for k, v in col_name.items()}
        df_goods = df_goods.rename(rename_map)
                        
        df_goods = df_goods.insert_column(0, pl.lit(symbol).alias("Symbol"))
        df_goods = df_goods.insert_column(1, pl.lit(df_date).alias("Date"))
        df_goods = df_goods.insert_column(2, pl.lit(df_period).alias("Period"))
        df_goods = df_goods.insert_column(3, pl.lit(publish).alias("Publish"))
        cs = ['Symbol','Date','Period','Publish', 'Raw Material', 'Unit', 'Type', 'Beginning Inventory_qn', 'Beginning Inventory_pr', 'Purchases During the Period_qn', 'Purchases During the Period_pr', 'Consumption_qn', 'Consumption_pr', 'Ending Inventory_qn', 'Ending Inventory_pr']
        df_goods = df_goods[cs]
        all_data[key] = df_goods
    
    return all_data
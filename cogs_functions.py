
import requests
import json
import re
import polars as pl
import Enum_data as ed
import general_functions as gf
from persiantools import characters, digits
pl.Config.set_tbl_rows(1000)

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
    all_data ={'report_last_year': pl.DataFrame(),
                'report_this_year': pl.DataFrame(),
                'est_remain': pl.DataFrame(),
                'est_next_year': pl.DataFrame()}

    cells_tuples = get_table(url, ed.tabels[report_type].value)
    dates = sorted(list(set([i[-1] for i in cells_tuples if i[-1] != ''])))
           
    for date_ in dates:
        filtered_cells = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == '' or i[-1] == (date_)]
        df = pl.from_records(filtered_cells, schema=["col", "row", "value"], orient="row")
        df = df.pivot(values="value", on="col", index="row").sort("row")
        df = df.with_columns(pl.col('1').map_elements(characters.ar_to_fa, return_dtype=pl.String))
        
        for i, col in enumerate(df.columns):
            cols = ed.cols[report_type].value
            value = df[col][0]        # value from first row
            if(isinstance(value, str)):
                for val in value.split():
                    if'/' in val:
                        num = int(val.replace('/', ''))
                        next_cols = df.columns[i : i + cols + 1]
                        cols_to_select = ['1','2', *next_cols] if report_type in ['Operational'] else ['1', *next_cols]
                        if num < date :
                            all_data['report_last_year'] = df.select(cols_to_select)
                            all_data['report_last_year'] = all_data['report_last_year'].with_row_index("row")
                        elif num == date :
                            all_data['report_this_year'] = df.select(cols_to_select)
                            all_data['report_this_year'] = all_data['report_this_year'].with_row_index("row")
                        else:
                            if(val[:4] == str(date)[:4]):
                                all_data['est_remain'] = df.select(cols_to_select)
                                all_data['est_remain'] = all_data['est_remain'].with_row_index("row")
                            else:
                                all_data['est_next_year'] = df.select(cols_to_select)
                                all_data['est_next_year'] = all_data['est_next_year'].with_row_index("row")
    for key, value in all_data.items():
        if(value.is_empty()):
            all_data[key] = None
    return all_data

def create_cogs_dataframe(symbol: str, url: str, date: int, period: int, publish: int) -> dict:
    all_data = create_dict_dataframes(url, date, 'COGS')
    
    col_names =["description",
                "Direct Materials Used",
                "Direct Labor",
                "Manufacturing Overhead",
                "Total",
                "Under-absorbed Manufacturing Overhead",
                "Total Manufacturing Costs",
                "Net Work-in-Process Inventory",
                "Abnormal Waste",
                "Cost of Goods Manufactured",
                "Beginning Finished Goods Inventory",
                "Ending Finished Goods Inventory",
                "Cost of Goods Sold (COGS)",
                "Cost of Services Rendered",
                "Total Cost"
                ]
    for key, data in all_data.items():
        if data is None: continue
        data = data.transpose(include_header=False)
        data = data.rename(dict(zip(data.columns, col_names)))
        data = data.tail(1)
        
        table_date = [d for d in str(data.row(0)[0]).split() if '/' in d][0]
        data = data.drop(data.columns[0])
        data = data.insert_column(0, pl.lit(symbol).alias("Symbol"))
        data = data.insert_column(1, pl.lit(int(digits.fa_to_en(table_date).replace('/', ''))).alias("Date"))
        data = data.insert_column(2, pl.lit(period).alias("Period"))
        data = data.insert_column(3, pl.lit(publish).alias("Publish"))
        all_data[key] = data
    return all_data 
  

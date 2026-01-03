import requests
import json
import re
import polars as pl
import Enum_data as ed
import general_functions as gf
from persiantools import characters, digits
pl.Config.set_tbl_rows(1000)

def get_table(url: str, table_content: str, table: int):
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
            if characters.ar_to_fa(table_content) in characters.ar_to_fa(items[0]['tables'][t]['title_Fa']):
                raw_cells = items[0]['tables'][t]['cells']
                cells.append([(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in raw_cells])
                return [x for xs in cells for x in xs]
    
    cells = items[0]['tables'][table]['cells']
    
    return [(i['columnSequence'], i['rowSequence'], i['value'], i['periodEndToDate']) for i in cells]

def create_dict_dataframes(url: str, date: int,  report_type: str, content_id: int) -> dict:
    table_content = {1: 'تولید', 2: 'مواد', 3: 'انرژی'}
    
    st_date = f"{str(date)[:4]}/{str(date)[4:6]}/{str(date)[6:]}"
    all_data ={ 'report_this_month': pl.DataFrame(),
                'report_total': pl.DataFrame()}

    cells_tuples = get_table(url, table_content[content_id], ed.tabels[report_type].value)

    filtered_cells = [(i[0], i[1], i[2]) for i in cells_tuples if i[-1] == '' or i[-1] == st_date]
    df = pl.from_records(filtered_cells, schema=["col", "row", "value"], orient="row")
    df = df.pivot(values="value", on="col", index="row").sort("row")
    df = df.with_columns(pl.col('1').map_elements(characters.ar_to_fa, return_dtype=pl.String))
    
    df = df.drop(df.columns[3:6])
    common_cols = df.columns[:3]

    all_data['report_this_month'] = df.select(common_cols + df.columns[3:6])
    all_data['report_total'] = df.select(common_cols + df.columns[7:11])

    return all_data  
      
    
def create_Monthly_report_dataframe(symbol: str, url: str, date: int, publish: int) -> dict:
    all_data = create_dict_dataframes(url, date, 'Monthly_report', 1)
    for name, df in all_data.items():
        df = df.rename({c: str(i) for i, c in enumerate(df.columns)})
        df = df.rename({'0': 'row'})
        table_date = [d for d in str(df[df.columns[3]][0]).split() if '/' in d][0]
        df_internal = df.clone()
        idx_internal = df_internal.filter(df_internal['1'] == "فروش داخلی:")['row'][0]
        idx_total_internal = df_internal.filter(df_internal['1'] == "جمع فروش داخلی")['row'][0]
        df_internal = df_internal.filter(df_internal['row'] > idx_internal, df_internal['row'] < idx_total_internal-1)[['1','2']]
        df_internal=df_internal.with_columns(pl.lit("داخلی").alias("3"))
        df_export=df.clone()
        idx_export = df_export.filter(df_export['1'] == "فروش صادراتی:")['row'][0]
        idx_total_export = df_export.filter(df_export['1'] == "جمع فروش صادراتی")['row'][0]
        df_export = df_export.filter(df_export['row']>idx_export ,df_export['row']<idx_total_export -1)[['1','2']]
        df_export = df_export.with_columns(pl.lit("صادراتی").alias('3'))
        df_products = pl.concat([df_internal,df_export])

        for i in df[0].rows(named=True):
            for k, v in i.items():
                if '/' in str(v):

                    col_num_production = k
                    dummy_data = df[col_num_production:str(int(col_num_production)+2)]
                    dummy_data = dummy_data.insert_column(0, df['row'])
                       
                    sales_internal = dummy_data.filter(dummy_data['row'] > idx_internal, dummy_data['row'] < idx_total_internal-1)
                    sales_export = dummy_data.filter(dummy_data['row']>idx_export ,dummy_data['row']<idx_total_export -1)
                    sales_products = pl.concat([sales_internal, sales_export])               
                    sales_products = sales_products.drop(sales_products.columns[0])
                    
                    #Rename columns to range(0,1,...)
                    sales_products = sales_products.rename(dict(zip(sales_products.columns, map(str, range(len(sales_products.columns))))))
                    #If "" is in the dataframe

                    if gf.has_empty_string(sales_products):
                        continue
                    sales_products = sales_products.with_columns(pl.col(pl.Utf8).replace("", "0").cast(pl.Int64)).filter(~pl.all_horizontal(pl.all() == 0))
                    #Convert string to Int from Col 1 to end
                    sales_products = sales_products.with_columns([pl.col(sales_products.columns).cast(pl.Int64)])
                    # concat label and values
                    df_products = df_products.rename({c: "_" + c for c in df_products.columns})
                    
                    data = pl.concat([df_products, sales_products], how="horizontal")

                    data.columns = [str(i) for i in range(data.width)]
                    data = data.insert_column(0, pl.lit(symbol).alias("Symbol"))
                    data = data.insert_column(1, pl.lit(int(digits.fa_to_en(table_date).replace('/', ''))).alias("Date"))
                    data = data.insert_column(2, pl.lit(publish).alias("Publish"))
                    data = data.drop_nulls()
                    all_data[name] = data
    return all_data
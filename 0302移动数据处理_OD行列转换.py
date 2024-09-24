"""读取OD数据处理成 A-B 的唯一数据，另存为parquet"""

import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
import numpy as np
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa

def make_unique(columns):
    seen = set()
    for item in columns:
        fudge = 1
        newitem = item
        while newitem in seen:
            fudge += 1
            newitem = "{}_{}".format(item, fudge)
        yield newitem
        seen.add(newitem)

def process_data(file_path, parquet_output_path):
    print("开始数据处理...")
    
    # 读取Parquet文件的元数据
    parquet_file = pq.ParquetFile(file_path)
    
    # 分块处理数据
    chunk_size = 1000000  # 根据实际情况调整
    
    # 创建一个空的DataFrame来存储结果
    result_df = pd.DataFrame()
    
    for batch in tqdm(parquet_file.iter_batches(batch_size=chunk_size), desc="处理数据块"):
        # 将PyArrow表转换为Pandas DataFrame
        chunk = batch.to_pandas()
        
        # 对每个块进行处理
        grouped = chunk.groupby(['StartGridID', 'EndGridID', 'Date', 'Time', 'Age', '常住流动', '出行方式'])['PeopleCount'].sum().reset_index()
        pivoted_df = grouped.pivot_table(
            values='PeopleCount', 
            index=['StartGridID', 'EndGridID', 'Date', 'Time'],
            columns=['Age', '常住流动', '出行方式'],
            fill_value=0
        ).reset_index()

        # 处理列名等操作...
        pivoted_df.columns = [
            '_'.join(str(item) for item in col).strip() if isinstance(col, tuple) else str(col)
            for col in pivoted_df.columns.values
        ]

        # 重命名列
        rename_dict = {0: '12岁以下', 1: '12-18岁', 2: '18-22岁', 3: '22-30岁', 4: '30-40岁', 5: '40-50岁', 6: '50-60岁', 7: '60岁以上', 8: '其他'}
        population_dict = {1: '常住人口', 0: '流动人口'}
        travel_mode_dict = {0: '步行', 1: '地铁', 2: '汽车', 3: '其他'}        
        for old_col in pivoted_df.columns:
            if '_' in old_col:
                parts = old_col.split('_')
                if len(parts) == 3:
                    age, pop_type, travel_mode = parts
                    try:
                        new_col = f"{rename_dict.get(int(age), age)}_{population_dict.get(int(pop_type), pop_type)}_{travel_mode_dict.get(int(travel_mode), travel_mode)}"
                    except ValueError:
                        new_col = old_col  # 如果无法转换为整数，保留原始列名
                    pivoted_df.rename(columns={old_col: new_col}, inplace=True)
        
        # 处理列名和添加总计列
        pivoted_df = pivoted_df.rename(columns={col: col.replace('__', '') for col in pivoted_df.columns if '__' in col})
   
        for col in ['12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他', '常住人口', '流动人口', '步行', '汽车', '地铁', '其他']:
            if col not in pivoted_df.columns:
                pivoted_df[col] = pivoted_df.filter(like=col).sum(axis=1)
        
        pivoted_df['TotalPeopleCount'] = pivoted_df.loc[:, ['12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他']].sum(axis=1)
     
        # 只保留指定的列
        保留列 = ['StartGridID', 'EndGridID', 'Date', 'Time', 
                 '12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他',
                 '常住人口', '流动人口', '步行', '汽车', '地铁', '其他', 'TotalPeopleCount']
        pivoted_df = pivoted_df[保留列]
        
        # 确保列名唯一
        pivoted_df.columns = list(make_unique(pivoted_df.columns))
        
        # 将指定列转换为int16类型
        columns_to_convert = [
            '12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', 
            '其他', '常住人口', '流动人口', '步行', '汽车', '地铁', '其他_2', 'TotalPeopleCount'
        ]
        
        for col in columns_to_convert:
            if col in pivoted_df.columns:
                pivoted_df[col] = pivoted_df[col].astype('int16')
        
        # 将结果追加到result_df
        result_df = pd.concat([result_df, pivoted_df], ignore_index=True)
        
        # 如果result_df变得太大，就写入文件并清空
        if len(result_df) > 5000000:  # 根据实际情况调整这个阈值
            write_to_parquet(result_df, parquet_output_path, mode='append')
            result_df = pd.DataFrame()
         
    # # 处理最后剩余的数据
    # print("df信息")
    # print(result_df.columns)
    # print(result_df)
    # #print(pivoted_df['TotalPeopleCount'])       
    
    if not result_df.empty:
        write_to_parquet(result_df, parquet_output_path, mode='append')
    print(f"数据已成功保存为Parquet文件: {parquet_output_path}")


def write_to_parquet(df, path, mode='append'):
    if mode == 'append' and os.path.exists(path):
        existing_df = pd.read_parquet(path)
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df.to_parquet(path, index=False)
    else:
        df.to_parquet(path, index=False)

if __name__ == "__main__":
    folder = r"I:\移动_广州市城市规划设计有限公司\0809"
    name = "gzup_chuxing_od_stat_output_20240814_珠三角九市"
    file_path = os.path.join(folder, f"{name}.parquet")
    parquet_output_path = os.path.join(folder, f"{name}_转置.parquet")
    # 检查输出文件是否存在,如果存在则删除
    if os.path.exists(parquet_output_path):
        os.remove(parquet_output_path)
        print(f"已删除现有的输出文件: {parquet_output_path}")
    process_data(file_path, parquet_output_path)

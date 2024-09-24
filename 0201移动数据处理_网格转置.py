import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import geopandas as gpd
import pyarrow.parquet as pq
from functools import partial

# 设置环境变量
os.environ['USE_PYGEOS'] = '0'

# 定义路径
folder = r"I:\移动_广州市城市规划设计有限公司"
project = "合并4个大桥范围"
dataname = f"01_net_id_20240515_{project}"
#"gzup_chuxing_xq_stat_output_20240814", "gzup_chuxing_od_stat_output_20240814", "gzup_chuxing_xq_stat_output_20240817", "gzup_chuxing_od_stat_output_20240817"]  
#"01_net_id_20240515", "01_net_id_20240518", "02_net_od_20240515", "02_net_od_20240518"

wangge = 500

file_path = os.path.join(folder, "0809", f"{dataname}.txt")
gdb_path = os.path.join(folder, "0809", f"{dataname}_转置_geo.gdb")
gdb_layer_name = f"{dataname}_转置_geo"
parquet_output_path = os.path.join(folder, "0809", f"{dataname}_转置_geo.parquet")
parquet_grid_path = f"I:\移动_广州市城市规划设计有限公司\原始网格数据\guangdong_grid_{wangge}m.parquet"

# 定义重命名字典 
rename_dict = {
    0: '12岁以下', 1: '12-18岁', 2: '18-22岁', 3: '22-30岁',
    4: '30-40岁', 5: '40-50岁', 6: '50-60岁', 7: '60岁以上', 8: '其他'
}
work_live_dict = {0: '工作人口', 1: '居住人口', 2: '其他人口', 3: '工作+居住'}
population_dict = {1: '常住人口', 0: '流动人口'}

def rename_column(col):
    if isinstance(col, tuple) and len(col) == 4:
        metric, age, work_live, pop_type = col
        return f"{metric}_{rename_dict.get(age, age)}_{work_live_dict.get(work_live, work_live)}_{population_dict.get(pop_type, pop_type)}"
    return col

def process_data(df):
    print("开始数据处理...")
    
    print("处理人数_总列...")
    df['人数_总'] = df['人数_总'].replace(5, 1)
    print("人数_总列处理完成")
    
    print("开始数据透视...")
    pivot_table = pd.pivot_table(
        df,
        index='grid_id',
        columns=['Age','工作居住','常住流动'],
        values=['人数_总'],
        aggfunc='sum',
        fill_value=0
    )
    print("数据透视完成")
    
    print("开始重命名列...")
    pivot_table.columns = [rename_column(col) for col in pivot_table.columns]
    pivot_table = pivot_table.reset_index()
    print("列重命名完成")
    
    print("开始计算求和列...")
    sum_columns = ['12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他', '常住人口', '流动人口', '工作人口', '居住人口', '其他人口', '工作+居住']
    for col in sum_columns:
        if col not in pivot_table.columns:
            pivot_table[col] = pivot_table.filter(like=col).sum(axis=1)
    
    pivot_table['TotalPeopleCount'] = pivot_table[['12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他']].sum(axis=1)
    print("求和列计算完成")
    
    return pivot_table

def main():
    print("开始数据处理...")
    
    print("正在读取txt文件...")
    if wangge == 100:
        df = pd.read_csv(file_path, sep='|', header=0, encoding='utf-8', usecols=['name1', 'name3', 'name2', 'name4', 'name6', 'name5', 'name7', 'name8', 'name9', 'name10'])
    else:
        df = pd.read_csv(file_path, sep='|', header=0, encoding='utf-8', usecols=['name1', 'name3', 'name2', 'name4', 'name6', 'name5', 'name7', 'name8'])
    print("txt文件读取完成")
    
    print("正在重命名列...")
    if wangge == 100:
        df.columns = ['grid_id', 'Date', 'Age', '工作居住', '常住流动', '性别', '归属地', '人数_总', '人数_15min', '人数_唯一']
    else:
        df.columns = ['grid_id', 'Date', 'Age', '工作居住', '常住流动', '人数_总', '人数_15min', '人数_唯一']
    print("列重命名完成")
    
    pivot_table = process_data(df)
    
    print("正在合并几何数据...")
    
    gdf = gpd.read_parquet(parquet_grid_path)
    merged_gdf = pivot_table.merge(gdf, left_on='grid_id', right_on='id', how='left')
    print("几何数据合并完成")
    
    # 确保结果是GeoDataFrame
    merged_gdf = gpd.GeoDataFrame(merged_gdf, geometry='geometry', crs=gdf.crs)
    
    print("正在导出为Parquet文件...")
    merged_gdf.to_parquet(parquet_output_path)
    print(f"数据已导出至Parquet文件: {parquet_output_path}")

    print("正在导出为GeoDatabase...")
    merged_gdf.to_file(gdb_path, layer=gdb_layer_name, driver="OpenFileGDB", encoding='utf-8-sig')
    print(f"数据已导出至GeoDatabase: {gdb_path}, 图层名称: {gdb_layer_name}")
    
    print("数据处理完成。")

if __name__ == "__main__":
    main()
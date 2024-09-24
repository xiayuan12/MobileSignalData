import os
import json
import pyarrow.parquet as pq
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import pandas as pd

# 定义路径
datafolder = r"I:\移动_广州市城市规划设计有限公司\0809"
dataname = "02_net_od_20240518_转置_geo"
quyu = "深中通道"
wangge = 500
outputfolder = os.path.join(datafolder, "深中通道")
json_file_path = os.path.join(datafolder, "深中通道", f"guangdong_grid_{wangge}m_{quyu}.json")

parquet_file_path = os.path.join(datafolder, f"{dataname}.parquet")
gdf = gpd.read_parquet(parquet_file_path)
print(gdf.columns)

# 读取黄埔大桥_name_id.json文件
with open(json_file_path, 'r', encoding='utf-8') as f:
    name_id_data = json.load(f)

# 创建一个空的GeoDataFrame来存储所有选中的数据
all_selected_gdf = gpd.GeoDataFrame()

# 遍历JSON中的每个名称和ID列表
for name, ids in name_id_data.items():
    print(f"处理 {name} 的数据:")
    print(f"ID列表: {ids[:5]}...")  # 只打印前5个ID作为示例
    
    # 选取指定ID的数据
    selected_gdf = gdf[(gdf['StartGridID'].isin(ids)) | (gdf['EndGridID'].isin(ids))]

    # 添加新字段来区分不同的name
    selected_gdf['区域名称'] = name

    # 将选中的数据添加到all_selected_gdf中
    all_selected_gdf = pd.concat([all_selected_gdf, selected_gdf], ignore_index=True)

    print(f"{name} 的数据处理完成\n")

# 检查合并后的数据
print(f"合并后的数据行数: {len(all_selected_gdf)}")
print(f"合并后的数据列: {all_selected_gdf.columns}")

# 另存为新的Parquet文件
output_parquet_file_path = os.path.join(outputfolder, f"{dataname}_{quyu}.parquet")
all_selected_gdf.to_parquet(output_parquet_file_path)
print(f"已保存合并后的Parquet文件: {output_parquet_file_path}")

# 另存为地理数据库中的要素
output_gdb_path = os.path.join(outputfolder, f"{dataname}_{quyu}.gdb")
output_layer_name = f"{dataname}_{quyu}"
all_selected_gdf.to_file(output_gdb_path, driver='OpenFileGDB', layer=output_layer_name, crs="EPSG:4326")
print(f"已保存合并后的GDB文件: {output_gdb_path}")

print("所有数据处理完成")
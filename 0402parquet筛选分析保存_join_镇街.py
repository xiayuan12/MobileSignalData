import os
import pandas as pd
import pyarrow.parquet as pq
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
from shapely.geometry import LineString, Point
import json

# 定义路径
datafolder = r"I:\移动_广州市城市规划设计有限公司\0809"
dataname = "gzup_chuxing_od_stat_output_20240814_珠三角九市"
wangge = 100

grid_file_path = os.path.join(r"I:\移动_广州市城市规划设计有限公司\原始网格数据", "国家地理_县级行政区_utf8sig.parquet")
parquet_file_path = os.path.join(datafolder, f"{dataname}_转置_镇街.parquet")
df = pd.read_parquet(parquet_file_path)
# 读取网格数据
grid_gdf = pd.read_parquet(grid_file_path)

print("Parquet数据的列名:")
print(df.columns)
print("网格数据的列名:")
print(grid_gdf.columns)
print("正在进行第一次join操作...")
merged_df = df.merge(grid_gdf[['xian_code', 'centroid_x', 'centroid_y']], left_on='xian_code_start', right_on='xian_code', how='left')
merged_df = merged_df.rename(columns={'centroid_x': 'centroid_x_start', 'centroid_y': 'centroid_y_start'})
print("第一次join操作完成")

print("正在删除 xian_code 列...")
merged_df = merged_df.drop(columns=['xian_code'])
print("xian_code 列已删除")
print(merged_df.columns)
print("正在进行第二次join操作...")
merged_df = merged_df.merge(grid_gdf[['xian_code', 'centroid_x', 'centroid_y']], left_on='xian_code_end', right_on='xian_code', how='left', suffixes=('', '_end'))
merged_df = merged_df.rename(columns={'centroid_x': 'centroid_x_end', 'centroid_y': 'centroid_y_end'})
print("第二次join操作完成")

# 检查并处理缺失值
merged_df = merged_df.dropna(subset=['centroid_x_start', 'centroid_y_start', 'centroid_x_end', 'centroid_y_end'])

# 添加调试信息，检查无效值
invalid_rows = merged_df[(merged_df['centroid_x_start'].isnull()) | (merged_df['centroid_y_start'].isnull()) | (merged_df['centroid_x_end'].isnull()) | (merged_df['centroid_y_end'].isnull())]
if not invalid_rows.empty:
    print("以下行存在无效值:")
    print(invalid_rows)

print("正在创建线段几何对象...")
merged_df['geometry'] = merged_df.apply(lambda row: LineString([Point(row['centroid_x_start'], row['centroid_y_start']), Point(row['centroid_x_end'], row['centroid_y_end'])]), axis=1)
print("线段几何对象创建完成")

print("正在将DataFrame转换为GeoDataFrame...")
gdf = gpd.GeoDataFrame(merged_df, geometry='geometry', crs="EPSG:4326")
print("转换完成")


# 将结果保存到all_gdf_results
all_gdf_results = gdf
print(all_gdf_results.columns)


# 另存为新的Parquet文件
output_parquet_file_path = os.path.join(datafolder, f"{dataname}_转置_镇街_geo.parquet")
all_gdf_results.to_parquet(output_parquet_file_path)
print("parquet文件保存完成")
# 另存为地理数据库中的要素
output_gdb_path = os.path.join(datafolder, f"{dataname}_转置_镇街_geo.gdb")
output_layer_name = f"{dataname}_转置_镇街_geo"
all_gdf_results.to_file(output_gdb_path, driver='OpenFileGDB', layer=output_layer_name, crs="EPSG:4326")
print("所有数据处理完成")
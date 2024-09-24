import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd

# 定义文件路径
grid_path = os.path.join(r"I:\移动_广州市城市规划设计有限公司\原始网格数据", "guangdong_grid_100m.parquet")
folder = r"I:\个人数据库整理\01全国_整理20240826\01基础地理数据\01边界\01_全国_国家地理中心_行政边界_2023_区县"
name = "国家地理_县级行政边界"
fanwei_path = os.path.join(folder, f"{name}.parquet")
output_path = os.path.join(folder, f"guangdong_grid_100m_quxian.parquet")

print("开始读取网格Parquet文件...")
grid_gdf = gpd.read_parquet(grid_path)
print("网格Parquet文件读取完成")

print("开始读取范围Parquet文件...")
fanwei_gdf = gpd.read_parquet(fanwei_path)
print("范围Parquet文件读取完成")

# 检查并转换CRS
if grid_gdf.crs != fanwei_gdf.crs:
    print(f"CRS不匹配。网格CRS: {grid_gdf.crs}, 范围CRS: {fanwei_gdf.crs}")
    print("正在将范围数据转换为EPSG:4326...")
    fanwei_gdf = fanwei_gdf.to_crs(epsg=4326)
    print("CRS转换完成")

print("开始进行空间连接操作...")
joined_gdf = gpd.sjoin(grid_gdf, fanwei_gdf, how="left", predicate="intersects")
print("空间连接操作完成")

print("开始导出为Parquet文件...")
joined_gdf.to_parquet(output_path)
print(f"Parquet文件导出完成，保存路径：{output_path}")

print("处理完成的GeoDataFrame包含以下字段:")
for column in joined_gdf.columns:
    print(f"- {column}")
    print("开始导出为GDB文件...")
    gdb_path = os.path.join(folder, "guangdong_grid_100m_quxian.gdb")
    layer_name = "guangdong_grid_100m_quxian"
    joined_gdf.to_file(gdb_path, layer=layer_name, driver="OpenFileGDB")
    print(f"GDB文件导出完成，保存路径：{gdb_path}")

"""读取广东省边界的Shapefile文件。
将500米转换为经纬度的度数
根据边界生成500米对应经纬度间隔的渔网。
计算每个网格的中心点坐标，并将其赋给网格。
添加一列id。
设置坐标系为WGS 1984。
导出生成的渔网为Shapefile文件和Parquet文件。"""
import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box, Point

# 定义路径
lujin = r'I:\移动_广州市城市规划设计有限公司\原始网格数据'
# 读取数据
guangdong = gpd.read_file(f"{lujin}/省界buffer.shp")  # 省界buffer
wanggechangdu = 100

# 获取边界范围
bounds = guangdong.total_bounds
xmin, ymin, xmax, ymax = bounds

# 计算500米对应的经度和纬度变化值
latitude_change = wanggechangdu / 111320  # 1度纬度约等于111.32公里
latitude = (ymin + ymax) / 2  # 使用区域的中间纬度
longitude_change = wanggechangdu / (111320 * np.cos(np.radians(latitude)))

# 生成渔网
rows = int(np.ceil((ymax - ymin) / latitude_change))
cols = int(np.ceil((xmax - xmin) / longitude_change))

grid = []
centers = []
for i in range(cols):
    for j in range(rows):
        x1 = xmin + i * longitude_change
        y1 = ymin + j * latitude_change
        x2 = x1 + longitude_change
        y2 = y1 + latitude_change
        grid.append(box(x1, y1, x2, y2))
        centers.append(Point((x1 + x2) / 2, (y1 + y2) / 2))

# 创建GeoDataFrame
grid_gdf = gpd.GeoDataFrame({'geometry': grid, 'center': centers}, crs="EPSG:4326")

# 添加中心点x、y坐标
grid_gdf['center_x'] = grid_gdf['center'].apply(lambda p: p.x)
grid_gdf['center_y'] = grid_gdf['center'].apply(lambda p: p.y)

# 删除center列
grid_gdf = grid_gdf.drop(columns='center')

# 添加id列
grid_gdf['id'] = grid_gdf.index
# 定义Parquet文件路径
parquet_path = f"{lujin}/guangdong_grid_{wanggechangdu}m.parquet"

# 导出为Parquet文件
grid_gdf.to_parquet(parquet_path, index=False)
print(f"渔网已导出至 {parquet_path}")

# 定义GDB路径和图层名称
gdb_path = f"{lujin}/guangdong_grid_{wanggechangdu}m.gdb"
layer_name = f"guangdong_grid_{wanggechangdu}m"
# 导出为GDB
grid_gdf.to_file(gdb_path, layer=layer_name, driver="OpenFileGDB")
print(f"渔网已导出至 {gdb_path}, 图层名称: {layer_name}")

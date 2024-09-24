import os
import json
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd

# 定义文件路径
project = "合并4个大桥范围"
name = "合并4个大桥范围"
gridname = "guangdong_grid_500m"
folder = r"I:\移动_广州市城市规划设计有限公司\0809\深中通道"
NAME = "name"


grid_path = os.path.join(r"I:\移动_广州市城市规划设计有限公司\原始网格数据", f"{gridname}.parquet")
fanwei_path = os.path.join(folder, f"{name}.parquet")

print("开始读取范围数据...")
fanwei_gdf = gpd.read_parquet(fanwei_path)
print(f"读取的数据行数: {len(fanwei_gdf)}")
print("范围数据读取完成")

def perform_spatial_intersection(grid_path, fanwei_gdf):
    print("开始读取网格Parquet文件...")
    grid_gdf = gpd.read_parquet(grid_path)
    print("网格Parquet文件读取完成")
    
    print("开始进行CRS转换...")
    fanwei_gdf = fanwei_gdf.to_crs(grid_gdf.crs)
    print("CRS转换完成")

    print("开始进行空间相交操作...")
    intersected = gpd.overlay(grid_gdf, fanwei_gdf, how='intersection')
    print("空间相交操作完成")

    return intersected

# 函数调用
print("开始执行空间相交函数...")
intersected = perform_spatial_intersection(grid_path, fanwei_gdf)
print("空间相交函数执行完成")

print("相交结果包含以下字段:")
for column in intersected.columns:
    print(f"- {column}")

# 创建NAME和id的字典
name_id_dict = intersected.groupby(NAME)['id'].apply(list).to_dict()

# 将字典导出为JSON文件
json_output_path = os.path.join(folder, f"{gridname}_{project}.json")
with open(json_output_path, 'w', encoding='utf-8') as f:
    json.dump(name_id_dict, f, ensure_ascii=False, indent=4)

print(f"NAME和id已导出为JSON文件: {json_output_path}")

# 打印所有的id
print("开始打印所有id...")
all_ids = intersected['id'].tolist()
print(f"总ID数量: {len(all_ids)}")
print("所有id打印完成")

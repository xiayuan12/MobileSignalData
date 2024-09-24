import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import box, Point

"""将 guangdong 和 jizhan 转换为投影坐标系。
生成 50000*50000 的渔网并逐步切分，从32000、16000、800、400、200
将 fishnet_100 转换为点，并计算点的 x、y 坐标。
将这些点与 fishnet_100 进行空间连接。
增加一列 "id"。
导出最终的 shape 文件。"""
# 定义路径
lujin = r'E:\2024数据中心\20240625中国移动数据购买\数据处理'
# 读取数据
guangdong = gpd.read_file(f"{lujin}/省界buffer.shp")#省界buffer
jizhan = gpd.read_file(f"{lujin}/广东省手机基站数据.shp")

# 将数据转换为投影坐标系，WGS 1984 Web Mercator 坐标系
guangdong = guangdong.to_crs(epsg=3857)
jizhan = jizhan.to_crs(guangdong.crs)

def create_fishnet(bounds, cell_size):
    """生成一个鱼网格"""
    minx, miny, maxx, maxy = bounds
    cols = list(np.arange(minx, maxx, cell_size))
    rows = list(np.arange(miny, maxy, cell_size))
    cells = []
    for x in cols:
        for y in rows:
            cells.append(box(x, y, x+cell_size, y+cell_size))
    return gpd.GeoDataFrame(cells, columns=['geometry'], crs=guangdong.crs)

def count_points_in_grid(grid, points):
    """统计每个网格中的点数量"""
    joined = gpd.sjoin(points, grid, how='left', predicate='within')
    counts = joined.groupby('index_right').size()
    grid['count'] = counts
    grid['count'].fillna(0, inplace=True)
    return grid

# 生成50000*50000的渔网面
bounds = guangdong.total_bounds
fishnet_50000 = create_fishnet(bounds, 32000)

# 统计每个网格中的基站数量
fishnet_50000 = count_points_in_grid(fishnet_50000, jizhan)

# 切分并统计更小的网格
def recursive_split(grid, points, cell_size, threshold, new_cell_size):
    new_grids = []
    for idx, row in grid.iterrows():
        if row['count'] > threshold:
            new_grid = create_fishnet(row['geometry'].bounds, new_cell_size)
            new_grid = count_points_in_grid(new_grid, points)
            new_grids.append(new_grid)
        else:
            new_grids.append(gpd.GeoDataFrame([row], columns=grid.columns, crs=grid.crs))
    return gpd.GeoDataFrame(pd.concat(new_grids, ignore_index=True), crs=grid.crs)

# 切分50000*50000的网格为10000*10000
fishnet_10000 = recursive_split(fishnet_50000, jizhan, 32000, 0, 16000)

# 切分10000*10000的网格为1000*1000
fishnet_1000 = recursive_split(fishnet_10000, jizhan, 16000, 0, 800)

# 切分1000*1000的网格为500*500
fishnet_500 = recursive_split(fishnet_1000, jizhan, 800, 10, 400)

# 切分500*500的网格为100*100
fishnet_100 = recursive_split(fishnet_500, jizhan, 400, 5, 200)

# 保存最终结果
fishnet_100.to_file(f"{lujin}/最终改的shape1.shp")

# 将fishnet_100转为点
fishnet_100_points = fishnet_100.copy()
# 将数据转换为投影坐标系，WGS 1984坐标系
#fishnet_100_points = fishnet_100_points.to_crs(epsg=4326)
fishnet_100_points['geometry'] = fishnet_100_points.centroid

# 计算点的x、y坐标
fishnet_100_points['x'] = fishnet_100_points.geometry.x
fishnet_100_points['y'] = fishnet_100_points.geometry.y

# 空间join
fishnet_100 = gpd.sjoin(fishnet_100, fishnet_100_points[['geometry', 'x', 'y']], how='left', predicate='contains')

# 增加一列“id”
fishnet_100['id'] = fishnet_100.index

# 导出最终的shape
fishnet_100.to_file(f"{lujin}/guangdong_grid_10000m_500m.shp")

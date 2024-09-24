import os
import pyarrow.parquet as pq
import pyarrow as pa

folder = r"I:\移动_广州市城市规划设计有限公司\0809"
name = "gzup_chuxing_od_stat_output_20240814_珠三角九市"
wangge = 100 #500

input_parquet_path = os.path.join(folder, f"{name}_转置.parquet")
grid_quxian = f"I:\移动_广州市城市规划设计有限公司\原始网格数据\guangdong_grid_{wangge}m_quxian.parquet"
# 读取parquet文件
table = pq.read_table(input_parquet_path)


# 读取grid_quxian中的指定列
grid_table = pq.read_table(grid_quxian, columns=['id', 'xian_code', 'name_sheng', 'name_shi', 'name'])
print("已读取grid_quxian文件中的指定列")

# 确保StartGridID和id的数据类型一致
table = table.cast(pa.schema([
    pa.field('StartGridID', pa.int64()),
    pa.field('EndGridID', pa.int64()),
    *[f for f in table.schema if f.name not in ['StartGridID', 'EndGridID']]
]))

grid_table = grid_table.cast(pa.schema([
    pa.field('id', pa.int64()),
    *[f for f in grid_table.schema if f.name != 'id']
]))

# 进行join操作
merged_table = table.join(grid_table, keys='StartGridID', right_keys='id', join_type='left outer')
merged_table = merged_table.join(grid_table, keys='EndGridID', right_keys='id', join_type='left outer', left_suffix="_start", right_suffix="_end")
print("join操作完成")

# 按照 'name_sheng', 'name_shi', 'name' 进行聚合
grouped_table = merged_table.group_by(['name_sheng_start', 'name_shi_start', 'name_start', 'xian_code_start', 'name_sheng_end', 'name_shi_end', 'name_end', 'xian_code_end', 'Time']).aggregate([
    ('12岁以下', 'sum'),
    ('12-18岁', 'sum'),
    ('18-22岁', 'sum'),
    ('22-30岁', 'sum'),
    ('30-40岁', 'sum'),
    ('40-50岁', 'sum'),
    ('50-60岁', 'sum'),
    ('60岁以上', 'sum'),
    ('其他', 'sum'),
    ('常住人口', 'sum'),
    ('流动人口', 'sum'),
    ('步行', 'sum'),
    ('汽车', 'sum'),
    ('地铁', 'sum'),
    ('其他_2', 'sum'),
    ('TotalPeopleCount', 'sum')
])
print("聚合操作完成")

# 打印聚合后的结果
print(grouped_table.to_pandas().head())

# 导出parquet
output_parquet_path = os.path.join(folder, f"{name}_转置_镇街.parquet")
# 检查输出文件是否存在,如果存在则删除
if os.path.exists(output_parquet_path):
    os.remove(output_parquet_path)
    print(f"已删除现有的输出文件: {output_parquet_path}")
    
pq.write_table(grouped_table, output_parquet_path)
print(f"已将结果导出到 {output_parquet_path}")

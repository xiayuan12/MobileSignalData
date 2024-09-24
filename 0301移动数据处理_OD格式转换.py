"""txt数据转换为parquet,提高运算效率"""

import os
os.environ['USE_PYGEOS'] = '0'
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

# 定义路径和读取文件
folder = r"I:\移动_广州市城市规划设计有限公司\0809"
name = "gzup_chuxing_od_stat_output_20240814_珠三角九市"
wangge = 100
file_path = os.path.join(folder, f"{name}.txt")

# 获取文件总行数
total_rows = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))

# 定义列名和数据类型
if wangge == 500:
    columns = ['StartGridID', 'EndGridID', 'Date', 'Time', 'Age', '常住流动', '出行方式', 'PeopleCount']
    dtypes = {
        'StartGridID': 'int64', 'EndGridID': 'int64', 'Date': 'int32',
        'Time': 'int32', 'Age': 'float32', '常住流动': 'int8',
        '出行方式': 'category', 'PeopleCount': 'int32'
    }
else:
    columns = ['StartGridID', 'EndGridID', 'Date', 'Time', 'Age', '常住流动', '性别', '来源市', '出行方式', 'PeopleCount']
    dtypes = {
        'StartGridID': 'int64', 'EndGridID': 'int64', 'Date': 'int32',
        'Time': 'int32', 'Age': 'float32', '常住流动': 'int8',
        '性别': 'int8', '来源市': 'category', '出行方式': 'category', 'PeopleCount': 'int32'
    }

# 使用tqdm创建进度条
with tqdm(total=total_rows, desc="读取和处理数据") as pbar:
    # 分块读取和处理数据
    chunks = pd.read_csv(file_path, sep='|', header=0, encoding='utf-8', names=columns, 
                         dtype=dtypes, chunksize=100000)

    processed_chunks = []
    for chunk in chunks:
        # 数据清洗和转换
        chunk['PeopleCount'] = chunk['PeopleCount'].where(chunk['PeopleCount'] != 5, 1)
        chunk['Age'] = chunk['Age'].fillna(8).astype('int8')
        chunk['常住流动'] = chunk['常住流动'].fillna(3).astype('int8')

        # 处理出行方式
        chunk['出行方式'] = chunk['出行方式'].cat.add_categories('其他').fillna('其他')
        出行方式映射 = {'步行': 0, '地铁': 1, '汽车': 2, '其他': 3}
        chunk['出行方式'] = chunk['出行方式'].map(出行方式映射).astype('int8')

        processed_chunks.append(chunk)
        pbar.update(len(chunk))

    # 合并所有处理后的块
    processed_df = pd.concat(processed_chunks, ignore_index=True)

# 打印数据信息
print("修改后的前10行数据:")
print(processed_df.head(10))
print("\n数据信息:")
processed_df.info()
print("\n数据描述:")
print(processed_df.describe())
print("\n缺失值统计:")
print(processed_df.isnull().sum())

# 定义输出路径
parquet_output_path = os.path.join(folder, f"{name}.parquet")

# 将处理后的数据保存为Parquet格式
table = pa.Table.from_pandas(processed_df)
pq.write_table(table, parquet_output_path)

print(f"处理后的数据已保存至: {parquet_output_path}")

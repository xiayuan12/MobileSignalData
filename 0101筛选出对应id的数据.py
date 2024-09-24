import os
import json
import pandas as pd
import numpy as np
from tqdm import tqdm

folder = r"I:\移动_广州市城市规划设计有限公司\0809"

wangge = 100
project = "合并4个大桥范围"
json_path = os.path.join(r"I:\移动_广州市城市规划设计有限公司\0809\深中通道", f"guangdong_grid_{wangge}m_{project}.json")
datanames = ["gzup_chuxing_xq_stat_output_20240814", "gzup_chuxing_od_stat_output_20240814", "gzup_chuxing_xq_stat_output_20240817", "gzup_chuxing_od_stat_output_20240817"]  
#"01_net_id_20240515", "01_net_id_20240518", "02_net_od_20240515", "02_net_od_20240518"


for dataname in datanames:
    file_path = os.path.join(folder, f"{dataname}.txt")
    with open(json_path, 'r', encoding='utf-8') as f:
        name_id_dict = json.load(f) 

    # 将所有ID合并为一个集合
    all_ids = set()
    for ids in name_id_dict.values():
        all_ids.update(ids)

    # 将ID集合转换为NumPy数组以提高查找效率
    all_ids_array = np.array(list(all_ids))

    # 分块读取和处理数据
    chunk_size = 1000000  # 根据可用内存调整此值
    output_txt_path = os.path.join(folder, f"{dataname}_{project}.txt")

    # 获取文件总行数
    total_rows = sum(1 for _ in open(file_path, 'r', encoding='utf-8'))

    with tqdm(total=total_rows, desc="处理数据") as pbar:
        for chunk in pd.read_csv(file_path, sep='|', header=0, encoding='utf-8', chunksize=chunk_size):
            # 使用numpy的isin函数进行快速筛选
            mask = np.isin(chunk['name1'].values, all_ids_array) | np.isin(chunk['name2'].values, all_ids_array)
            selected_chunk = chunk[mask]

            # 追加到输出文件
            selected_chunk.to_csv(output_txt_path, mode='a', index=False, sep='|', encoding='utf-8', header=not os.path.exists(output_txt_path))

            pbar.update(len(chunk))

    print(f"已保存TXT文件: {output_txt_path}")

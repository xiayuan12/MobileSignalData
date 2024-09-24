import pandas as pd
import os
import matplotlib.pyplot as plt
from matplotlib import rcParams

# 设置显示汉字
rcParams['font.sans-serif'] = ['SimHei']
rcParams['axes.unicode_minus'] = False

folder_path = r"I:\移动_广州市城市规划设计有限公司\0809"
dataname = "gzup_chuxing_od_stat_output_20240814_深中通道_转置_镇街_geo"
input_parquet_file = f"{dataname}.parquet"
# 读取数据，根据Time列，分析TotalPeopleCount列，计算每个时间段的流量变化

# 读取Parquet文件
df = pd.read_parquet(os.path.join(folder_path, input_parquet_file))
# 筛选出namexianend为中山市、宝安区、香洲区的数据
df = df[df['name_end'].isin(['中山市', '宝安区', '香洲区']) | df['name_start'].isin(['中山市', '宝安区', '香洲区'])]
print(df.columns)
# 检查是否成功读取数据
if df.empty:
    print("读取的数据为空，请检查文件路径和文件内容。")
else:
    print("数据读取成功。")

# 提取Time列的最后两位作为小时
df['Hour'] = df['Time'].astype(str).str[-2:].astype(int)

# 按小时分组并计算每小时的TotalPeopleCount_sum总和
hourly_traffic = df.groupby('Hour')['TotalPeopleCount_sum'].sum()#TotalPeopleCount

# 增加其他列的统计
columns_to_sum = ['12岁以下_sum', '12-18岁_sum', '18-22岁_sum', '22-30岁_sum', '30-40岁_sum', '40-50岁_sum', '50-60岁_sum', '60岁以上_sum', '其他_sum', '常住人口_sum', '流动人口_sum', '步行_sum', '汽车_sum', '地铁_sum', '其他_2_sum']#'12岁以下', '12-18岁', '18-22岁', '22-30岁', '30-40岁', '40-50岁', '50-60岁', '60岁以上', '其他', '常住人口', '流动人口', '步行', '汽车', '地铁', '其他_2'
hourly_stats = df.groupby('Hour')[columns_to_sum].sum()

# 合并统计结果
hourly_traffic = pd.concat([hourly_traffic, hourly_stats], axis=1)

# 检查分组结果
if hourly_traffic.empty:
    print("分组结果为空，请检查数据处理过程。")
else:
    print("分组结果成功生成。")

# 打印每小时的流量变化
print("每小时的流量变化:")
print(hourly_traffic)

# 将结果保存为新的CSV文件
output_csv_path = os.path.join(folder_path, "分析结果", f"{dataname}_每小时流量变化.csv")
hourly_traffic.to_csv(output_csv_path, encoding='utf-8-sig')
print(f"每小时流量变化已保存至: {output_csv_path}")

# 生成流量变化图
plt.figure(figsize=(15, 10))

# 绘制TotalPeopleCount的柱状图
ax1 = plt.subplot(2, 1, 1)
hourly_traffic['TotalPeopleCount_sum'].plot(kind='bar', ax=ax1)
ax1.set_title('每小时流量变化')
ax1.set_xlabel('小时')
ax1.set_ylabel('TotalPeopleCount')
ax1.set_xticks(range(len(hourly_traffic.index)))
ax1.set_xticklabels(hourly_traffic.index, rotation=0)
ax1.grid(True)

# 绘制其他数据的折线图
ax2 = plt.subplot(2, 1, 2)
for column in columns_to_sum:
    hourly_traffic[column].plot(kind='line', ax=ax2, label=column)
ax2.set_title('每小时其他数据变化')
ax2.set_xlabel('小时')
ax2.set_ylabel('数量')
ax2.set_xticks(range(len(hourly_traffic.index)))
ax2.set_xticklabels(hourly_traffic.index, rotation=0)
ax2.legend()
ax2.grid(True)

# 保存图像
output_image_path = os.path.join(folder_path, "分析结果", f"{dataname}_每小时流量变化.png")
plt.savefig(output_image_path)
print(f"流量变化图已保存至: {output_image_path}")

# 显示图像
plt.show()


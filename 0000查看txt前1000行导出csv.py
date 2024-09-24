
import os
import csv

folder = r"I:\移动_广州市城市规划设计有限公司\0809"
name = "gzup_chuxing_od_stat_output_20240814_珠三角九市"

file_path = os.path.join(folder, f"{name}.txt")
output_csv_path = os.path.join(folder, f"{name}_前1000行.csv")

# 读取前1000行数据并导出为CSV
with open(file_path, 'r', encoding='utf-8') as infile, open(output_csv_path, 'w', newline='', encoding='utf-8-sig') as outfile:
    csv_writer = csv.writer(outfile)
    for i, line in enumerate(infile):
        if i >= 1000:
            break
        csv_writer.writerow(line.strip().split('|'))

print(f"已成功导出前1000行数据到: {output_csv_path}")

# 打印前10行数据以供查看
print("\n前10行数据预览:")
with open(output_csv_path, 'r', encoding='utf-8-sig') as csvfile:
    csv_reader = csv.reader(csvfile)
    for i, row in enumerate(csv_reader):
        if i >= 10:
            break
        print(row)


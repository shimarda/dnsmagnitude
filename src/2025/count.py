import func
import argparse
import os

def file_time(file_lst):
    file_dic = dict()

    time_list = [
        os.path.basename(f).replace('-', '').replace('.csv', '') for f in file_lst
    ]

    for time in time_list:
        month = time[4:6]
        day = time[6:8]
        hour = time[8:10]
        if day not in file_dic.keys():
            file_dic[day] = list()
        file_dic[day].append(hour)
    return file_dic

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')
    parser.add_argument('-w', help='0なら権威 1ならリゾルバ')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d
    where = args.w

    lst = func.file_lst(year, month, day, where)
    file_dic = file_time(lst)

    for day in file_dic.keys():
        dom_dic = dict()

        for hour in file_dic[day]:
            print(month+day+hour)

            df = func.open_reader_safe(year, month, day, hour, where)
            func.count_query(df, dom_dic)
        dom_dic = sorted(dom_dic.items(), key=lambda item: item[1], reverse=True)
    # csvへの書き込み
    # funcで作成
        file_path = f"/home/shimada/analysis/output-2025/{where}-{year}-{month}-{day}.csv"
        func.write_csv(dom_dic, year, month, day, where)
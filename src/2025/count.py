import func
import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-y', help='year')
    parser.add_argument('-m', help='month')
    parser.add_argument('-d', help='day')

    args = parser.parse_args()

    year = args.y
    month = args.m
    day = args.d

    lst = func.file_lst(year, month, day)
    file_dic = func.file_time(lst)

    for day in file_dic.keys():
        uni_src_set = set()
        dom_dic = dict()

        for hour in file_dic[day]:
            print(month+day+hour)

            df = func.open_reader(year, month, day, hour)
            func.count_query(df, dom_dic)
        dom_dic = sorted(dom_dic.items(), key=lambda item: item[1], reverse=True)
    # csvへの書き込み
    # funcで作成
        file_path = f"/home/shimada/analysis/output-2025/{year}-{month}-{day}.csv"
        func.write_csv(dom_dic, year, month, day)
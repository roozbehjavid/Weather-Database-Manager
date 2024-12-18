import sqlite3 as sq
from tabulate import tabulate
import re

columns = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp', '*', '']
functions = ['RANK', 'SUM', 'COUNT', 'MAX', 'MIN', 'ROW_NUMBER', 'DENSE_RANK', 'AVG']

def main():
    try:
        window_function(dbname, tablename)
    except EOFError:
        print("exiting...")

def user_input():
    select = []
    aggregate= []
    window = []

    while True:
        try:
            select_column = input("Enter columns or press enter to skip: ")
            if not select_column:
                break
            elif select_column in columns[:-2]:
                select.append(select_column)
            else:
                print("column does not exist. try another one ...")
                continue
        except EOFError:
            return "", "", ""

    while True:
        try:
            is_aggregate = input("Any aggregate function (y/n)? ").strip().lower()
            if is_aggregate == 'y':
                while True:
                    try:
                        count = int(input("how many? "))
                        break
                    except ValueError as e:
                        print(f"error:{e}")
                    except EOFError:
                        return "", "", ""
                    if count <= 0:
                        continue
                temp = []
                for _ in range(count):
                    while True:
                        u_input = input("Enter function and column (e.g., SUM(temperature)) or press Enter to stop: ").strip()
                        match = re.findall(r"(\w+)\((\w+|\*)\)", u_input)
                        if match and match[0][0].upper() in functions and match[0][1] in columns:
                            temp.append(f"{match[0][0].upper()}({match[0][1]})")
                            break
                        else:
                            print("Invalid input. Try again.")
                aggregate.append(temp)
                break
            else:
                break
        except EOFError:
            return "", "", ""

    while True:
        try:
            win_num = int(input("Number of rows using window functions or press 0 to skip: "))
            if win_num == 0:
                break
            if win_num < 0:
                print("A positive integer...")
                continue
            for _ in range(int(win_num)):
                win = []
                while True:
                    try:
                        win_input = input("enter window function and column (e.g., RANK() or SUM(humidity)): ").strip()
                        match = re.findall(r"(\w+)\((\w+|)\)", win_input)
                        if match and match[0][0].upper() in functions and match[0][1] in columns:
                            win.append(f"{match[0][0].upper()}({match[0][1]})")
                            break
                        else:
                            print("Invalid input. Try again.")
                    except EOFError:
                        return "", "", ""

                while True:
                    try:
                        is_partition = input("is there a 'partition by' in the query (yes/no)? ").lower()
                        if is_partition == "yes":
                            while True:
                                partition_column = input("partition column: ")
                                if partition_column in columns:
                                    win.append("PARTITION BY " + partition_column)
                                    break
                                else:
                                    continue
                            break
                        elif is_partition == "no":
                            break
                        else:
                            continue
                    except EOFError:
                        return "", "", ""

                while True:
                    try:
                        is_order = input("is there any 'order by' in the query (yes/no)? ").strip().lower()
                        if is_order == 'no':
                            break
                        elif is_order == 'yes':
                            while True:
                                order_column = input("order column: ")
                                if order_column in columns:
                                    win.append("ORDER BY " + order_column)
                                    break
                                else:
                                    continue
                            break
                        else:
                            print("must enter yes/no ...")
                    except EOFError:
                        return "", "", ""

                while True:
                    try:
                        direction = input("enter 'D' for DESC or 'A' for ASC: ").strip().upper()
                        if direction == 'D':
                            win.append("DESC")
                            break
                        elif direction == 'A' or "":
                            break
                        else:
                            print("must choose one ...")
                    except EOFError:
                        return "", "", ""

                while True:
                    try:
                        alias = input("alias: ").strip()
                        if not alias:
                            print("choose an alias...")
                            continue
                        else:
                            win.append(alias)
                            break
                    except EOFError:
                        return "", "", ""
                window.append(win)
            break
        except EOFError:
            return "", "", ""
        except ValueError:
            print("input must be a positive integer ...")
            continue
        
    return select, aggregate, window


def window_function(dbname, tablename):
    select, aggregate, window = user_input()
    if not(select or aggregate or window):
        print("no data provided. exiting ...")
        return
    select_clause = "SELECT " + ", ".join(select) + ", "
    if len(aggregate) > 0:
        for item in aggregate:
            aggregate_clause = ", ".join(item)
        aggregate_clause = aggregate_clause + ", "
    else:
        aggregate_clause = ""
    temp = []
    for item in window:
        string = item[0] + " OVER (" + " ".join(item[1:-1]) + ") AS " + item[-1]
        temp.append(string)
    window_clause = ", ".join(temp)
    query = f"{select_clause}{aggregate_clause}{window_clause} FROM {tablename};"
    print(f"the executed query is :{query}")

    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                cursor.execute(f"PRAGMA table_info({tablename})")
                headers = select + [item.lower() for sublist in aggregate for item in sublist] + ["window result"]
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No data found.")
        except sq.OperationalError as e:
            print(f"Error: {e}")
            return

if __name__ == "__main__":
    main()
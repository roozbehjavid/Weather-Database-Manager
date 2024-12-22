import sqlite3 as sq
from tabulate import tabulate
import re
import sys

COLUMNS = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp', '*', '']
functions = ['RANK', 'SUM', 'COUNT', 'MAX', 'MIN', 'ROW_NUMBER', 'DENSE_RANK', 'AVG']

def main():
    window_function(dbname, tablename)

def window_function(dbname, tablename):

    select, aggregate, wf_list = user_input()
    if not(select or aggregate or wf_list):
        print("no data provided. exiting ...")
        return

    select_clause = "SELECT " + ", ".join(select) + ", "
    aggregate_clause = ", ".join(aggregate) + ", " if aggregate else ""
    window_clause = ", ".join(wf_list) if wf_list else ""

    query = f"{select_clause}"
    if aggregate_clause:
        query += f"{aggregate_clause}"
    query += f"{window_clause} FROM {tablename};"

    print(f"the executed query is :{query}")

    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                headers = [desc[0] for desc in cursor.description]
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No data found.")
        except sq.OperationalError as e:
            print(f"Error: {e}")
            return

def user_input():
    select = []
    aggregate= []
    wf_list = []

    while True:
        try:
            select_column = input("Enter columns or press enter to skip: ").strip()
            if not select_column:
                break
            elif select_column in COLUMNS[:-2]:
                select.append(select_column)
            else:
                print("column does not exist. try another one ...")
                continue
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")
# IF WORKS OUT FINE FOR GO.PY, CHANGE THIS SECTION
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
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")
                    if count <= 0:
                        continue
                for _ in range(count):
                    while True:
                        u_input = input("Enter function and column (e.g., SUM(temperature)) or press Enter to stop: ").strip()
                        match = re.findall(r"(\w+)\((\w+|\*)\)", u_input)
                        if match and match[0][0].upper() in functions and match[0][1] in COLUMNS:
                            aggregate.append(f"{match[0][0].upper()}({match[0][1]})")
                            break
                        else:
                            print("Invalid input. Try again.")
                break
            else:
                break
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

    while True:
        try:
            row = int(input("Number of rows using window functions or press enter to skip: "))
            if not row:
                break
            if row < 0:
                print("A positive integer...")
                continue
            for _ in range(int(row)):
                wf_query = ""
                while True:
                    try:
                        win_input = input("enter window function and column (e.g., RANK() or SUM(humidity)): ").strip()
                        match = re.findall(r"(\w+)\((\w+|)\)", win_input)
                        if match and match[0][0].upper() in functions and match[0][1] in COLUMNS:
                            wf_query += f"{match[0][0].upper()}({match[0][1]}) OVER ("
                            break
                        else:
                            print("Invalid input. Try again.")
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")

                while True:
                    try:
                        is_partition = input("is there a 'partition by' in the query (y/n)? ").lower()
                        if is_partition == "y":
                            while True:
                                partition_column = input("partition column: ")
                                if partition_column in COLUMNS:
                                    wf_query += f"PARTITION BY {partition_column}"
                                    break
                                else:
                                    continue
                            break
                        elif is_partition == "n":
                            break
                        else:
                            continue
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")

                while True:
                    try:
                        is_order = input("is there any 'order by' in the query (y/n)? ").strip().lower()
                        if is_order == 'n':
                            break
                        elif is_order == 'y':
                            while True:
                                order_column = input("order column: ")
                                if order_column in COLUMNS:
                                    wf_query += f" ORDER BY {order_column}"
                                    break
                                else:
                                    continue
                            break
                        else:
                            print("must choose either y or n ...")
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")

                while True:
                    try:
                        direction = input("enter 'D' for DESC or 'A' for ASC: ").strip().upper()
                        if direction == 'D':
                            wf_query += f" DESC"
                            break
                        elif not direction or direction == 'A':
                            break
                        else:
                            print("must choose one ...")
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")

                while True:
                    try:
                        alias = input("alias: ").strip()
                        if not alias:
                            print("choose an alias...")
                            continue
                        else:
                            wf_query += f") AS {alias}"
                            break
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")
                wf_list.append(wf_query)
            break
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")
        except ValueError:
            print("input must be a positive integer ...")
            continue
    return select, aggregate, wf_list

if __name__ == "__main__":
    main()
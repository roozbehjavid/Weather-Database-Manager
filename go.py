import sqlite3 as sq
from tabulate import tabulate
import re
import sys

COLUMNS = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp', '1', '*']
functions = ['SUM', 'COUNT', 'MAX', 'MIN', 'AVG']
symbols = {"gt":">", "lt":"<", "eq":"=", "in":"IN", "like":"LIKE", "gte":">=", "lte":"<="}
DIRECTION = {"A":"ASC", "D":"DESC"}

def main():
    try:
        go(dbname, tablename)
    except EOFError:
        sys.exit("Operation terminated by user. Goodbye!")

def go(dbname, tablename):
    select, aggregate, group_by, having, order_by = go_input()
    if not(select or aggregate or group_by or having or order_by):
        return
    
    select_clause = "SELECT " + ", ".join(select)
    aggregate_clause = ", " + ", ".join(aggregate) if aggregate else ""
    group_by_clause = ", ".join(group_by) if group_by else ""
    having_clause = ", ".join(having) if having else ""
    order_by_clause = ", ".join(order_by) if order_by else ""

    query = f"{select_clause}"
    if aggregate_clause:
        query += f"{aggregate_clause}"
    query += f" FROM {tablename}"
    if group_by:
        query += f"\n GROUP BY {group_by_clause}"
    if having:
        query += f"\n HAVING {having_clause}"
    if order_by:
        query += f"\n ORDER BY {order_by_clause}"
    query += f";"

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

def go_input():
    select = []
    aggregate= []
    group_by = []
    having = []
    order_by = []

    while True:
        try:
            select_column = input("Enter 'select' columns or press enter to skip: ").strip()
            if not select_column:
                break
            if select_column in COLUMNS:
                select.append(select_column)
            else:
                print("column does not exist. try another one ...")
                continue
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")
            
    while True:
        try:
            agg_func_col = input("Enter function, column and its alias (e.g., SUM(temperature) sum_temp), or press Enter to stop: ").strip()
            match = re.findall(r"(\w+)\((\w+|\*)\)(\s+\w+)?", agg_func_col)
            if not match:
                break
            elif not (match[0][0].upper() in functions and match[0][1] in COLUMNS and match[0][2]):
                print("invalid input. try again ...")
                continue
            alias = match[0][2]
            clause = f"{match[0][0].upper()}({match[0][1]}) AS{match[0][2]}"
            aggregate.append(clause)
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

    while True:
        is_group = input("GROUP BY (y/n)? ").strip().lower()
        if is_group == 'y':
            while True:
                try:
                    gb_column = input("Enter 'GROUP BY' column or press enter to skip: ").strip()
                    if not gb_column:
                        break
                    if gb_column in COLUMNS and gb_column not in group_by:
                        group_by.append(gb_column)
                    else:
                        print("column does not exist. try another one ...")
                        continue
                except EOFError:
                    print('\n')
                    sys.exit("Operation terminated by user. Goodbye!")
            
            while True:
                try:
                    condition = input("Enter 'HAVING' condition or press enter to skip: ").strip()
                    if not condition:
                        break
                    match = re.findall(r"(\w+)\s+(=|>|<|>=|<=|IN|NOT IN)\s+(\w+|\d+)", condition)
                    if match:
                        matched_col, matched_rel = match[0][0], match[0][1]
                        if matched_col in (COLUMNS or aggregate) and matched_rel in symbols.values():
                            having.append(condition)
                        else:
                            print("try again ...")
                            continue
                    else:
                        print("invalid condition format. try again ...")   
                except EOFError:
                    print('\n')
                    sys.exit("Operation terminated by user. Goodbye!")
            break
        elif is_group == 'n':
            print("no GROUP BY clause. proceed to the next step ...")
            break
        else:
            continue

    while True:
        is_order = input("ORDER BY (y/n)? ").strip().lower()
        if is_order == 'y':
            while True:
                try:
                    ob_column = input("Enter 'order by' column. Type 'exit' to skip: ").strip()
                    if ob_column == 'exit':
                        break
                    direction = input("Enter direction (A for ASC and D for DESC). Type 'exit' to skip: ").strip()
                    if direction == 'exit':
                        break
                    if not (ob_column and direction):
                        print("fields cannot be empty ...\n")
                        continue
                    if not (ob_column in (COLUMNS and alias) and direction.upper() in DIRECTION):
                        print("invalid input. try again ... \n")
                        continue
                    if clause := f"{ob_column} {DIRECTION[direction.upper()]}" not in order_by:
                        order_by.append(f"{ob_column} {DIRECTION[direction.upper()]}")
                    
                except EOFError:
                    print('\n')
                    sys.exit("Operation terminated by user. Goodbye!")
            break
        elif is_order == 'n':
            print("no ORDER BY clause. proceed to the next step ...")
            break
        else:
            continue

    return select, aggregate, group_by, having, order_by

if __name__ == "__main__":
    main()
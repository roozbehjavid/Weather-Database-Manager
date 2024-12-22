import sqlite3 as sq
from tabulate import tabulate
import re
import sys
import argparse

parser = argparse.ArgumentParser(description="Weather Database Application")
parser.add_argument("--dbname", type=str, required=True, help="database name")
parser.add_argument("--apikey", type=str, required=True, help="api key")
parser.add_argument("--tablename", type=str, default= "weather", help="table name" )
args = parser.parse_args()

COLUMNS = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp', '1', '*']
WINDOW_FUNCS = ['RANK', 'ROW_NUMBER', 'DENSE_RANK']
AGG_FUNCS = ['SUM', 'COUNT', 'MAX', 'MIN', 'AVG']
symbols = {"gt":">", "lt":"<", "eq":"=", "in":"IN", "like":"LIKE", "gte":">=", "lte":"<="}
operators = {"1":'EXISTS', "2":'ANY', "3":'ALL', "4":'EXCEPT'}
conjunction = {"1":"AND", "2":"OR"}
outer_alias, inner_alias = "", ""

def main():
    nested_query(dbname, tablename)
        
def nested_query(dbname, tablename):
    global outer_alias, inner_alias
    outer_alias, inner_alias = "", ""

    select_columns, aggregate_columns, outer_alias, where_condition = outer_inputs()
    inner_select_columns, inner_alias, inner_conditions = inner_inputs()

    if not(select_columns or aggregate_columns or outer_alias or where_condition or inner_select_columns or inner_alias or inner_conditions):
        return

    select_clause = "SELECT " + ", ".join(select_columns)
    aggregate_clause = ", ".join(aggregate_columns) if aggregate_columns else ""
    where_clause = " ".join(where_condition)
    inner_select_clause = "SELECT " + ", ".join(inner_select_columns)
    inner_conditions_clause = " ".join("".join(item) for item in inner_conditions) if inner_conditions else ""

    query = f"{select_clause}"
    if aggregate_clause:
        query += f"{aggregate_clause}"
    query += f" FROM {tablename}"
    if outer_alias:
        query += f" {outer_alias}"
    query += f" WHERE {where_clause} ({inner_select_clause} FROM {tablename});"
    if inner_alias:
        query = query.replace(');', '')
        query += f" {inner_alias}"
    if inner_conditions_clause:
        query = query.replace(');', '')
        query += f" WHERE {inner_conditions_clause});"

    print(f"The query is: {query}")

    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            rows = cursor.fetchall()
            if rows:
                headers = select_columns + aggregate_columns
                print(tabulate(rows, headers= headers, tablefmt="grid"))
            else:
                print("No data found.")
        except sq.OperationalError as e:
            print(f"Error: {e}")
            return

def outer_inputs():
    global outer_alias
    select_columns = []
    aggregate_columns = []
    where_condition = []

    while True:
        try:
            outer_select_column = input("enter column or press enter to skip: ")
            if outer_select_column not in COLUMNS and outer_select_column:
                print("column does not exist. try another one ...")
                continue
            if not outer_select_column:
                break
            select_columns.append(outer_select_column)
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

    while True:
        try:
            aggregate_input = input("enter an aggregate function with a column (e.g. SUM(temperature)) or press enter to skip: ")
            if not aggregate_input:
                break
            if match := re.findall(r"(\w+)\((\w+|)\)", aggregate_input):
                func, col = match[0][0].upper(), match[0][1]
                if func in AGG_FUNCS and col in COLUMNS:
                    aggregate_columns.append(f", {func}({col})")
                    break
                else:
                    print("invalid aggregate function or column. try again ...")
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")
    
    while True:
        try:
            is_outer_alias = input("enter an alias for the table or press enter to leave it as blank: ")
            if not is_outer_alias:
                break
            if re.match(r"(\w+)",is_outer_alias):
                outer_alias = is_outer_alias
                break
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")
    
    while True:
        try:
            where_type = input("press 1 for column and 2 for operators ('EXISTS', 'ANY', and etc) or enter to skip: ")
            if not where_type:
                break
            elif where_type == '1':
                while True:
                    try:
                        column = input("enter the column: ")
                        if column not in COLUMNS:
                            continue
                        else:
                            while True:
                                try:
                                    relation = input("enter the relation(gt, lt, eq, like, in): ")
                                    if relation not in symbols:
                                        print("relation not valid. try another one...")
                                        continue
                                    else:
                                        where_condition.append(f"{column} {symbols[relation]}")
                                        break
                                except EOFError:
                                    print('\n')
                                    sys.exit("Operation terminated by user. Goodbye!")
                            break
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")
                break
            elif where_type == '2':
                while True:
                    try:
                        op = input('choose a number ("1":"EXISTS", "2":"ANY", "3":"ALL"): ').strip()
                        if not op:
                            continue
                        if op == '1':
                            where_condition.append(f"{operators[op]}")
                            break
                        if op in ['2', '3']:
                            while True:
                                get_column = input("enter a column: ")
                                if get_column not in COLUMNS:
                                    continue
                                where_condition.append(get_column)
                                get_relation = input("enter a relation: ")
                                if get_relation not in symbols:
                                    continue
                                where_condition.append(symbols[get_relation])
                                where_condition.append(operators[op])
                                break
                        break
                    except EOFError:
                        print('\n')
                        sys.exit("Operation terminated by user. Goodbye!")
                break
            else:
                print("invalid input. choose either 1 or 2. press Enter to skip")
                continue
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

    return select_columns, aggregate_columns, outer_alias, where_condition

def inner_inputs():
    global inner_alias
    inner_select_columns = []
    inner_conditions = []

    while True:
        try:
            inner_select_column = input("enter function and/or column (e.g. SUM(temperature) or 1) or press enter to skip: ")
            if not inner_select_column:
                break
            if match := re.findall(r"(\w+)(\((\w+|\*)\))?", inner_select_column):
                func, col = match[0][0], match[0][2]
                if not col and func in COLUMNS:
                    inner_select_columns.append(f"{func}")
                    break
                elif func.upper() in AGG_FUNCS and col in COLUMNS:
                    inner_select_columns.append(f"{func}({col})")
                    break
                elif not(func.upper() in AGG_FUNCS and col in COLUMNS):
                    continue
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

    more = input("more conditions (Y/N)? ").strip().upper()
    if more == 'Y':
        while True:
            try:
                is_inner_alias = input("enter an alias for the inner table or press enter to leave it as blank: ")
                if not is_inner_alias:
                    break
                if re.match(r"(\w+)",is_inner_alias):
                    inner_alias = is_inner_alias
                    break
                else:
                    continue
            except EOFError:
                print('\n')
                sys.exit("Operation terminated by user. Goodbye!")
        
        while True:
            try:
                condition_num = int(input("enter the number of conditions: "))
                if condition_num <= 0:
                    print("try a positive integer ...")
                    continue
                temp = []
                for _ in range(condition_num):
                    while True:
                        inner_condition_part = input("enter the condition: ")
                        if not condition_check(inner_condition_part):
                            continue
                        conj = input("enter '1' for 'AND' or '2' for 'OR'. press enter to skip: ")
                        if not conj:
                            temp.append(f"{inner_condition_part}")
                            break
                        if conj not in conjunction:
                            continue
                        temp.append(f"{inner_condition_part} {conjunction[conj]} ")
                        break
                inner_conditions.append(temp)
                break
            except ValueError as e:
                print(f"error: {e}")
                continue
            except EOFError:
                print('\n')
                sys.exit("Operation terminated by user. Goodbye!")
    elif more == 'N':
        inner_alias = ""
        inner_conditions = ""
    else:
        print("not enough data provided. returning to the last menu ...")
        return ""
    return inner_select_columns, inner_alias, inner_conditions

def condition_check(prompt):
    global inner_alias
    global outer_alias
    evaluation = True
    match = re.findall(r"(\w+\.)?(\w+)\s+(=|>|<|>=|<=|IN|NOT IN)\s+(\w+\.)?(\w+)", prompt)
    l = [item.strip('.') for item in match[0]]
    if l[0] not in [outer_alias, args.tablename, ''] and l[-2] not in [inner_alias, args.tablename, '']:
        evaluation = False
    if l[1] not in COLUMNS:
        evaluation = False
    if l[-1] not in COLUMNS and not l[-1].isdigit() and not l[-1].isalpha():
        evaluation = False
    if l[2] not in symbols.values():
        evaluation = False
    return evaluation

if __name__ == "__main__":
    main()
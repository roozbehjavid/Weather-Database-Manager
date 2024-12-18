import sqlite3 as sq
from tabulate import tabulate
import re
import sys

COLUMNS = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp', '1', '*']
WINDOW_FUNCS = ['RANK', 'ROW_NUMBER', 'DENSE_RANK']
AGG_FUNCS = ['SUM', 'COUNT', 'MAX', 'MIN', 'AVG']
symbols = {"gt":">", "lt":"<", "eq":"=", "in":"IN", "like":"LIKE"}
operators = {"1":'EXISTS', "2":'ANY', "3":'ALL', "4":'EXCEPT'}

def main():
    try:
        nested_query(dbname, tablename)
    except EOFError:
        print("exiting")
        
def nested_query(dbname, tablename):
    select_columns, aggregate_columns, outer_alias, where_condition = outer_inputs()
    inner_select_columns, inner_alias, inner_conditions = inner_inputs()

    if not(select_columns or aggregate_columns or outer_alias or where_condition or inner_select_columns or inner_alias or inner_conditions):
        print("No inputs provided. Exiting ...")
        return

    select_clause = "SELECT " + ", ".join(select_columns)
    aggregate_clause = ", ".join(aggregate_columns) if aggregate_columns else ""
    where_clause = "".join(where_condition)
    inner_select_clause = "SELECT " + ", ".join(inner_select_columns)
    if len(inner_conditions) > 0:
        inner_conditions_clause = " ".join("".join(item) for item in inner_conditions)
    else:
        inner_conditions_clause = ""
    
    query = (f"{select_clause} {aggregate_clause} FROM {tablename} {outer_alias}"
             f"WHERE {where_clause} ({inner_select_clause} FROM {tablename} {inner_alias} "
             f"WHERE {inner_conditions_clause});")
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
    select_columns = []
    aggregate_columns = []
    outer_alias = ""
    where_condition = []

    while True:
        try:
            outer_select_column = input("enter column or press enter to skip: ")
            if not outer_select_column:
                break
            elif outer_select_column not in COLUMNS:
                print("column does not exist. try another one ...")
                continue
            select_columns.append(outer_select_column)
        except EOFError:
            return ""

    while True:
        try:
            aggregate_input = input("enter an aggregate function with a column (separated by a space) or press enter to skip: ")
            if not aggregate_input:
                break
            if findall := re.findall(r"\w+|\*", aggregate_input):
                func, col = findall[0].upper(), findall[1]
                if func in AGG_FUNCS and col in COLUMNS:
                    aggregate_columns.append(f"{func}({col})")
                    break
                else:
                    print("invalid aggregate function or column. try again ...")
        except EOFError:
            return ""
    
    while True:
        try:
            is_outer_alias = input("enter an alias for the table or press enter to leave it as blank: ")
            if not is_outer_alias:
                break
            if re.match(r"(\w+)",is_outer_alias):
                outer_alias = is_outer_alias
                break
        except EOFError:
            return ""
    
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
                                    return ""
                            break
                    except EOFError:
                        return ""
                break
            elif where_type == '2':
                while True:
                    try:
                        op = input('enter a number ("1":"EXISTS", "2":"ANY", "3":"ALL", "4":"EXCEPT"): ').strip()
                        if not op:
                            continue
                        if op in operators:
                            where_condition.append(operators[op])
                            break
                    except EOFError:
                        return ""
                break
            else:
                print("invalid input. choose either 1 or 2. press Enter to skip")
                continue
        except EOFError:
            return ""

    return select_columns, aggregate_columns, outer_alias, where_condition

def inner_inputs():

    inner_select_columns = []
    inner_alias = ""
    inner_conditions = []

    while True:
        try:
            inner_select_column = input("enter function and/or column or press enter to skip: ")
            if not inner_select_column:
                break
            if findall := re.findall(r"\w+|\*", inner_select_column):
                if len(findall) == 2 and findall[0].upper() in AGG_FUNCS and findall[1] in COLUMNS:
                    inner_select_columns.append(f"{findall[0].upper()}({findall[1]})")
                elif len(findall) == 1 and findall[0] in COLUMNS:
                    inner_select_columns.append(f"{findall[0]}")
                elif not(findall[0].upper() in AGG_FUNCS and findall[1] in COLUMNS):
                    continue
            break
        except EOFError:
            return ""

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
                return ""
        
        while True:
            try:
                condition_num = int(input("enter the number of conditions: "))
                if condition_num <= 0:
                    print("try a positive integer ...")
                    continue
                temp = []
                for i in range(condition_num):
                    while True:
                        inner_condition_part = input("enter the condition: ")
                        conj = input("enter conjunction or simply press enter to proceed: ")
                        if inner_condition_part and conj in ["AND", "OR", ""]:
                            temp.append(f"{inner_condition_part} {conj} ")
                            break
                        else:
                            continue
                inner_conditions.append(temp)
                break
            except ValueError as e:
                print(f"error: {e}")
                continue
            except EOFError:
                return ""
    elif more == 'N':
        inner_alias = ""
        inner_conditions = ""
    else:
        print("no data provided. exiting ...")
        return ""

    return inner_select_columns, inner_alias, inner_conditions

if __name__ == "__main__":
    main()
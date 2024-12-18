import sqlite3 as sq
import view_records
from tabulate import tabulate
import sys

# Validate the column name and its expected data type
valid_columns = {
    "id": "INTEGER",
    "city_name": "TEXT",
    "temperature": "REAL",
    "weather_condition": "TEXT",
    "humidity": "INTEGER",
    "wind": "REAL",
    "timestamp": "DATETIME"
}
valid_conditions = {"eq":"=", "lt":"<", "gt":">", "lte":"<=", "gte":">=", "like":"LIKE"}
conjunction = {"1":"AND", "2":"OR"}

def main():
    try:
        multiple_condition(dbname, tablename)
    except EOFError:
        sys.exit("exiting ...")

def multiple_condition(dbname, tablename):
    conditions, values = user_inputs()
    if conditions == "" and values == "":
       return
    elif conditions == 0 and values == 0:
       return view_records.view_records(dbname, tablename)
    # Construct the query
    where_clause = ' '.join(conditions)
    query = f"SELECT * FROM {tablename} WHERE {where_clause}"

    # Query the database
    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(query, values)
            rows = cursor.fetchall()
            if rows:
                cursor.execute(f"PRAGMA table_info({tablename})")
                headers = [info[1] for info in cursor.fetchall()]
                print(tabulate(rows, headers=headers, tablefmt="grid"))
            else:
                print("No data found.")
        except sq.OperationalError:
            print(f"The table '{tablename}' does not exist in the database '{dbname}'.")
            return

def user_inputs():
    while True:
        try:
            condition_num = int(input("enter the number of conditions: "))
            if condition_num == 0:
                return 0, 0
            break
        except ValueError:
            print("enter an integer greater than zero to proceed")
            continue
        except EOFError:
            return "", ""
    
    conditions = []
    values = []

    for i in range(condition_num):
        while True:
            # Prompt the user for column, condition, and value
            try:
                column = input("Enter the column you want to query: ").strip()
                if column not in valid_columns:
                    print(f"Invalid column name. Please try again from {', '.join(valid_columns)}")
                    continue
                break
            except EOFError:
                return "", ""

        while True:
            try:
                condition = input("Enter the condition (e.g., =, <, >, LIKE): ").strip()
                if condition not in valid_conditions:
                    print("Invalid condition. Please try again.")
                    continue
                break
            except EOFError:
                return "", ""
        
        conditions.append(f"{column} {valid_conditions[condition]} ?")
        
        while True:
            try:
                value = input(f"Enter the value for {column} (expected type: {valid_columns[column]}): ").strip()
                # Cast the value based on the column type
                try:
                    if valid_columns[column] in ["INTEGER", "REAL"]:
                        value = float(value) if valid_columns[column] == "REAL" else int(value)
                    values.append(value)
                    break
                except ValueError:
                    print(f"Invalid value type. {column} expects {valid_columns[column]}.")
                    continue
            except EOFError:
                return "", ""

            # Ask if the user wants to add more conditions
        while True:
            try:
                conj = input("Enter 1 for 'AND' and 2 for 'OR'. press Enter to finish: ").strip().upper()
                if conj in conjunction:
                    conditions.append(conjunction[conj])
                    break
                elif conj == "":
                    break
                else:
                    print("Invalid input. Please enter 'AND', 'OR', or press Enter.")
            except EOFError:
                return "", ""
    return conditions, values

if __name__ == "__main__":
    main()

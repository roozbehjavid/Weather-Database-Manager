import sqlite3 as sq
from tabulate import tabulate

def main():
    view_records(dbname, tablename)

def view_records(dbname, tablename):
    records = []
    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        sql_fetch = f"SELECT * FROM {tablename};"
        try:
            cursor.execute(sql_fetch)
            rows = cursor.fetchall()
            for row in rows:
                if row not in records:
                    records.append(row)
            cursor.execute(f"PRAGMA table_info({tablename})")
            headers = [info[1] for info in cursor.fetchall()]
            display_results(records, headers)
        except sq.OperationalError:
            print(f"The table '{tablename}' does not exist in the database '{dbname}'.")

def display_results(rows, headers):
    if rows:
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print("No data found.")

if __name__ == "__main__":
    main()
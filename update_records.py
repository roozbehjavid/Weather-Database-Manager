import sqlite3 as sq
import sys

columns = ['id', 'city_name', 'temperature', 'weather_condition', 'humidity', 'wind', 'timestamp']

def main():
    try:
        update_records()
    except EOFError:
        print("\n")
        sys.exit("Operation terminated by user. Goodbye!")

def user_input():

    while True:
        try:
            row_id = int(input("enter row id: "))
            if row_id <= 0:
                raise ValueError
            break
        except ValueError:
            print("try a positive integer ...")
            continue
        except EOFError:
            print("\n")
            sys.exit("Operation terminated by user. Goodbye!")
    
    while True:
        try:
            column = input("enter the column you want to change its data: ").strip()
            if column not in columns:
                raise ValueError
            break
        except ValueError:
            print(f"Choose from {', '.join(columns)}")
            continue
        except EOFError:
            print("\n")
            sys.exit("Operation terminated by user. Goodbye!")

    while True:
        try:
            data = input("enter the new input data: ")
            if not data:
                raise ValueError
            if column in ['id', 'humidity']:
                try:
                    data = int(data)
                    break
                except ValueError:
                    print(f"{column} must be an integer. Please try again.")
                    continue
            elif column in ['temperature', 'wind']:
                try:
                    data = float(data)
                    break
                except ValueError:
                    print(f"{column} must be a number. Please try again.")
                    continue
            else:
                break
        except ValueError:
            print("data cannot be empty")
            continue
        except EOFError:
            print("\n")
            sys.exit("Operation terminated by user. Goodbye!")
            
    return row_id, column, data

def update_records(dbname, tablename):
    row_id, column, data = user_input()
    with sq.connect(dbname) as conn:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {tablename} WHERE id = ?", (row_id,))
        if not cursor.fetchone():
            print(f"No record found with ID {row_id}.")
            return
        try:
            cursor.execute(f"UPDATE {tablename} SET {column} = ? WHERE id = ?", (data, row_id))
            conn.commit()
        except EOFError:
            print("\n")
            sys.exit("Operation terminated by user. Goodbye!")


if __name__ == "__main__":
    main()
import sqlite3 as sq
import sys

def main():
    choice(dbname, tablename)

def choice(dbname, tablename):
    while True:
        try:
            user_input = int(input("choose 1 to delete records one by one and 2 to delete them all at once: "))
            if user_input == 1:
                delete_record(dbname, tablename)
                break
            elif user_input == 2:
                delete_all(dbname, tablename)
                break
        except ValueError:
            print("you can only choose either 1 or 2 ...")
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

def delete_record(dbname, tablename):
    while True:
        try:
            user_input = int(input("Enter the row ID to delete or '0' to return to the previous menu: "))
            if user_input == 0:
                print("returning to the previous menu ...")
                break
            with sq.connect(dbname) as conn:
                cursor = conn.cursor()
                try:
                    # Check if the record exists
                    cursor.execute(f"SELECT 1 FROM {tablename} WHERE id = ?", (user_input,))
                    if cursor.fetchone():
                        confirm = input(f"Are you sure you want to delete record ID {user_input}? (yes/no): ").strip().lower()
                        if confirm == 'yes':
                            cursor.execute(f"DELETE FROM {tablename} WHERE id = ?", (user_input,))
                            conn.commit()
                            print("Deletion successful.")
                        else:
                            print("Deletion cancelled.")
                    else:
                        print("No record found with the given ID.")
                except sq.OperationalError:
                    print(f"The table '{tablename}' does not exist in the database '{dbname}'.")
                    return
        except ValueError:
            print("Invalid input. Please enter an integer.")
            continue
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")


def delete_all(dbname, tablename):
    while True:
        try:
            confirm = input("are you sure you want to delete all rows (Y/N)? ").strip().upper()
            if confirm == 'Y':
                with sq.connect(dbname) as conn:
                    cursor = conn.cursor()
                    try:
                        cursor.execute(f"SELECT COUNT(*) FROM {tablename}")
                        if cursor.fetchone()[0] == 0:
                            print('the database is already empty.')
                            break
                        cursor.execute(f"DELETE FROM {tablename}")
                        conn.commit()
                        print("Deletion successful.")
                        break
                    except sq.OperationalError:
                        print(f"The table '{tablename}' does not exist in the database '{dbname}'.")
                        return
            elif confirm == 'N':
                print("Deletion cancelled.")
                break
            else:
                print("invalid input...")
        except EOFError:
            print('\n')
            sys.exit("Operation terminated by user. Goodbye!")

if __name__ == "__main__":
    main()
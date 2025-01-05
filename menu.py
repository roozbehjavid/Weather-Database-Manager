import argparse
import view_records
import insert_records
import update_records
import delete_records
import go
import advanced
import sys

parser = argparse.ArgumentParser(description="Weather Database Application")
parser.add_argument("--dbname", type=str, required=True, help="database name")
parser.add_argument("--apikey", type=str, required=True, help="api key")
parser.add_argument("--tablename", type=str, default= "weather", help="table name" )
args = parser.parse_args()


while True:
    try:
        option = int(input(
            """
            Please choose one of the following options:
            1. View Records
            2. Add a Record/Records
            3. Update a Record
            4. Delete a Record/Records
            5. Advanced Query
            6. Exit
            """
        ))
        if option not in range(1, 7):
            print("Please select a number between 1 and 6.")
        else:
            match option:
                case 1:
                    view_records.view_records(args.dbname, args.tablename)
                case 2:
                    insert_records.insert_records(args.dbname, args.apikey, args.tablename)
                case 3:
                    update_records.update_records(args.dbname, args.tablename)
                case 4:
                    delete_records.choice(args.dbname, args.tablename)
                case 5:
                    advanced.options()
                case _:
                    sys.exit("Goodbye!")
    except EOFError:
        print('\n')
        sys.exit("Operation terminated by user. Goodbye!")
    except ValueError:
        print("Invalid input. Please enter a number between 1 and 6 included.")
        continue

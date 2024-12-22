import sys
import multiple_conditions
import window_function
import nested_query
import go
import argparse

parser = argparse.ArgumentParser(description="Weather Database Application")
parser.add_argument("--dbname", type=str, required=True, help="database name")
parser.add_argument("--apikey", type=str, required=True, help="api key")
parser.add_argument("--tablename", type=str, default= "weather", help="table name" )
args = parser.parse_args()

def options():
    while True:
        try:
            option = int(input(
                """
                Please choose one of the following options:
                1. Multiple Conditions
                2. Window Function Queries
                3. Nested Queries
                4. GROUP/ORDER Queries
                5. Exit
                """
            ))
            if option not in range(1, 6):
                print("Invalid choice. Please select a number between 1 and 5.")
            else:
                if option == 1:
                    multiple_conditions.multiple_condition(args.dbname, args.tablename)
                elif option == 2:
                    window_function.window_function(args.dbname, args.tablename)
                elif option == 3:
                    nested_query.nested_query(args.dbname, args.tablename)
                elif option == 4:
                    go.go(args.dbname, args.tablename)
                elif option == 5:
                    return
                else:
                    raise ValueError

        except ValueError:
            continue
        except EOFError:
            print('\n')
            sys.exit("Goodbye!")
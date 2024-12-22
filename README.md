# Weather Database Manager

## Overview
Weather Database Manager is a Python-based application that allows users to interact with a SQLite database for weather data. The application provides functionality to:
- Insert new records.
- Update existing records.
- Delete records (individually or all at once).
- Execute several advanced queries.

The project is designed with a user-friendly CLI interface and includes features like input validation and error handling.

## Features
1. **Insert Records**: Add new rows of data to the database.
2. **Update Records**: Modify existing data based on record ID.
3. **Delete Records**:
   - Delete records one by one.
   - Delete all records at once with confirmation.
4. **Advanced Querying**: Perform dynamic and complex queries with multiple conditions.
5. **Tabular Output**: Display query results in a user-friendly table format.

## Prerequisites
1. Python 3.x
2. SQLite3:
   - Ensure you have SQLite installed on your system.
   - Use the provided `create_table.py` script to set up the database and table.
3. API Key:
   - This project uses a weather API to fetch weather data. You will need an API key to use the application.
   - Visit [Open Weather](https://openweathermap.org/) to create an account and obtain your free API key.
4. Required Python libraries:
   - `tabulate`

## Running the Application
1. To run the main menu script (`menu.py`), pass the following arguments via the command line:
- `--dbname`: The name of the SQLite database file.
- `--tablename`: The name of the table in the database (default: `weather`).
- `--apikey`: Your API key from the Open Weather API.
2. Ensure all required modules (`view_records.py`, `insert_records.py`, `update_records.py`, `delete_records.py`, `advanced_query.py`, `nested_query.py`, `go.py`, and `window_function.py`)  are in the same directory as `menu.py` before running the application.

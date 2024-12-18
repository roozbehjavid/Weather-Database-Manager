import sqlite3 as sq

with sq.connect(dbname) as conn:
    cursor = conn.cursor()
    sql_create_table = 
    f"CREATE TABLE IF NOT EXISTS {tablename} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name TEXT NOT NULL,
        temperature REAL,
        weather_condition TEXT,
        humidity INTEGER,
        wind REAL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP );"
    cursor.execute(sql_create_table)
    conn.commit()
import requests as r
import sqlite3 as sq
from datetime import datetime

def main():
    try:
        insert_records()
    except EOFError:
        print("exiting...")

def loc():
    cities = []
    while True:
        try:
            city = input("enter the city name or press enter to exit: ").strip()
            if city and city not in cities:
                cities.append(city)
            elif not city:
                break
        except EOFError:
            print('exiting...')
            break
    return cities

def insert_records(dbname, apikey, tablename):
    cities = loc()
    for city in cities:
        response = r.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={apikey}")
        if response.status_code == 200:
            data = response.json()
            inserted_data = (data['name'], float(data['main']['temp']-273.15), data['weather'][0]['description'], \
            int(data['main']['humidity']), float(data['wind']['speed']), datetime.utcfromtimestamp(data['dt']))
            with sq.connect(dbname) as conn:
                cursor = conn.cursor()
                try:
                    cursor.execute(f'INSERT INTO {tablename}(city_name, temperature, weather_condition, humidity, wind, \
                    timestamp) VALUES (?, ?, ?, ?, ?, ?)', inserted_data)
                    conn.commit()
                    print(f"Data for {city} inserted successfully.")
                except sq.OperationalError:
                    print(f"The table '{tablename}' does not exist in the database '{dbname}'.")
                    return
        else:
            print(f"The city name you entered, {city}, does not exist.")

if __name__ == "__main__":
    main()
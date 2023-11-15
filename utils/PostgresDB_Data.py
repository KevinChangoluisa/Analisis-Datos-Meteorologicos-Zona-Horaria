# -*- coding: utf-8 -*-
"""
Autor: Kevin Changoluisa
Correo electrónico: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""


from utils.PostgresConnect import PostgresConnection


class DataPostgresDB:
    def __init__(self, configDB):
        """
        Initializes an instance of the DataPostgresDB class.

        Args:
            configDB (dict): The configuration for the PostgreSQL database.
        """
        self.conn = PostgresConnection(configDB)
        self.conn.connect()

    def get_stations(self):
        """
        Gets the list of stations from the database.

        Returns:
            list: A list of dictionaries representing the stations.
        """
        query = f'''
        SELECT puobcodi, last_data_date, latitude, longitude
        FROM public.transmission_stations
        WHERE transmission_status_id=True and puobcodi LIKE 'M%';'''

        result = self.conn.execute_query(query)

        stations = [{'puobcodi': row[0], 'last_data_date': row[1], 'latitude': float(
            row[2]), 'longitude': float(row[3])} for row in result]

        return stations

    def insert_or_update_station_time_zone(self, puobcodi, update_date, time_zone_id):
        """
        Inserts or updates a transmission station in the database.

        Args:
            puobcodi (str): The station code.
            update_date (datetime): The date of the last data.
            time_zone_id (int): The ID of the transmission status.
        """
        try:
            insert_query = """
            INSERT INTO public.stations_time_zones (puobcodi, update_date, time_zone_id)
            VALUES (%s, %s, %s)
            ON CONFLICT (puobcodi)
            DO UPDATE SET
                update_date = EXCLUDED.update_date,
                time_zone_id = EXCLUDED.time_zone_id;
            """
            params = (puobcodi, update_date, time_zone_id)
            self.conn.execute_query(insert_query, params)
        except Exception as e:
            print("Error inserting or updating transmission station in the database:", e)

    def close_connection(self):
        """
        Closes the database connection.
        """
        self.conn.close()

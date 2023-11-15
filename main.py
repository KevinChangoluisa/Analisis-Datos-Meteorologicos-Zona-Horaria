# -*- coding: utf-8 -*-
"""
Author: Kevin Changoluisa
Email: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""

from utils.PostgresDB_Data import DataPostgresDB
from utils.MongoDB_Data import DataMongoDB
from utils.TemperatureAnalysis import TemperatureDataProcessor
from datetime import datetime, timedelta
import time
import configparser


config = configparser.ConfigParser()
config.read('utils/config.ini')
CONFIG_POSTGRES = config['DBPOSTGRESKEVIN']
CONFIG_MONGO = config['DBMONGOINAMHI']


class StationProcessor:
    def __init__(self, stations):
        """
        Initializes an instance of StationProcessor.
        Args:
            stations (list): A list of stations to process.
        """

        self.stations = stations

    def set_dates(self, days_ago=0):
        """
        Sets the start and end dates for data processing.

        Args:
            days_ago (int): Number of days to subtract from the current date to set
                            the start and end date. Default is 0, indicating the current date.

        Returns:
            tuple: Returns the start and end dates for processing.
        """
        reference_date = datetime.now() - timedelta(days=days_ago)
        yesterday_date = reference_date - timedelta(days=1)

        start_date = datetime(
            yesterday_date.year, yesterday_date.month, yesterday_date.day, 5, 0, 0)
        end_date = datetime(
            reference_date.year, reference_date.month, reference_date.day, 4, 59, 59)

        return start_date, end_date

    def get_and_process_data(self, puobcodi, days_ago=0):
        """
        Attempts to retrieve and process data from a station. If the data is insufficient,
        it recursively calls itself to try with the previous day.

        Args:
            puobcodi (str): The station code.
            days_ago (int): Number of days to subtract from the current date to get data.

        Returns:
            list: List of processed data or an empty list if there is not enough data.
        """
        start_date, end_date = self.set_dates(days_ago)
        result = dataMongoDB.getData(puobcodi, start_date, end_date)

        result = [item for item in result if '293161h' in item.get(
            'data', {}) and item['data']['293161h']]

        if len(result) > 20:
            result_list = temp_data_process.transform_data_list(result)
            result_data_missing = temp_data_process.predict_missing_data(
                result_list)
            time_zone = temp_data_process.determine_time_zone(
                result_data_missing)

            if time_zone > 0:
                return time_zone
            else:
                return self.get_and_process_data(puobcodi, days_ago + 1)

        elif days_ago < 7:  # Limit recursion to a maximum of 3 days ago
            return self.get_and_process_data(puobcodi, days_ago + 1)
        else:
            return []

    def process_group(self):
        """
        Processes a group of stations in a thread.
        """

        count = 0
        for station in self.stations:
            puobcodi = station['puobcodi']
            time_zone = self.get_and_process_data(puobcodi)
            if time_zone:
                stations_db.insert_or_update_station_time_zone(
                    puobcodi, datetime.now(), time_zone)
                count += 1
            else:
                stations_db.insert_or_update_station_time_zone(
                    puobcodi, datetime.now(), 0)

        print("\tStations with determined time zone: ",
              count, "/", len(self.stations))


if __name__ == '__main__':
    print('****************************************************************')
    print("\tStarting...", datetime.now())
    start_time = time.time()
    stations_db = DataPostgresDB(CONFIG_POSTGRES)
    stations = stations_db.get_stations()

    dataMongoDB = DataMongoDB(CONFIG_MONGO)
    temp_data_process = TemperatureDataProcessor()

    processor = StationProcessor(stations)
    processor.process_group()
    end_time = time.time()
    stations_db.close_connection()
    # Calculate the total execution time
    total_time = round(end_time - start_time, 3)
    print(f"\tFinished Execution Time: {total_time} seconds")
    print('****************************************************************')

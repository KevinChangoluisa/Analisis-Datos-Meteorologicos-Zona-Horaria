# -*- coding: utf-8 -*-
"""
Autor: Kevin Changoluisa
Correo electrónico: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""


import pandas as pd
from datetime import timedelta


class TemperatureDataProcessor:
    def __init__(self):
        pass

    def predict_missing_data(self, data):
        """
        Fill missing temperature values in a list of observations using linear interpolation.

        Args:
            data (list): A list of dictionaries, where each dictionary represents an observation
                         with a 'fechaTomaDato' (datetime) and 'dataValor' (float) field.

        Returns:
            list: The input list of objects with missing 'None' data values filled using linear interpolation.
        """
        # Create a pandas DataFrame from the list of objects
        df = pd.DataFrame(data)

        # Convert the 'fechaTomaDato' column to a datetime index
        df['fechaTomaDato'] = pd.to_datetime(df['fechaTomaDato'])

        df.set_index('fechaTomaDato', inplace=True)

        # Perform linear interpolation to fill missing values
        df['dataValor'].interpolate(method='linear', inplace=True)

        df['dataValor'] = df['dataValor'].round(1)

        # Sort the DataFrame by 'puobcodi', 'fechaTomaDato', and 'dataValor'
        df.sort_values(
            by=['puntoObservacion', 'fechaTomaDato', 'dataValor'], inplace=True)

        # Convert the DataFrame back to a list of objects
        data_with_predictions = df.reset_index().to_dict(orient='records')

        return data_with_predictions

    def determine_time_zone(self, data):
        """
        Determine the time zone based on the highest and lowest temperature observations.

        This function identifies the time of the highest and lowest temperature readings
        and determines the time zone by checking if these times fall into local peak
        or low temperature ranges. It considers both local time and UTC-5.

        Args:
            data (list): A list of dictionaries, each representing a temperature observation
                         with 'fechaTomaDato' (datetime) and 'dataValor' (float) fields.

        Returns:
            str: A string representing the determined time zone ('Local', 'UTC', or 'Indeterminate').
        """

        # Identify the observation with the highest temperature
        max_temp = max(data, key=lambda x: x['dataValor'])
        max_temp_time = max_temp['fechaTomaDato']

        # Identify the observation with the lowest temperature
        min_temp = min(data, key=lambda x: x['dataValor'])
        min_temp_time = min_temp['fechaTomaDato']

        # Check if the time is in the local peak temperature range
        def is_local_peak_time(time):
            return 12 <= time.hour < 16

        # Check if the time is in the local low temperature range
        def is_local_low_time(time):
            return 1 <= time.hour <= 6

        # Check if the observation times are in the local peak and low temperature ranges
        if is_local_peak_time(max_temp_time) and is_local_low_time(min_temp_time):
            return 2

        # Adjust for UTC-5 time difference and check again
        max_temp_time_adjusted = max_temp_time - timedelta(hours=5)
        min_temp_time_adjusted = min_temp_time - timedelta(hours=5)

        if is_local_peak_time(max_temp_time_adjusted) and is_local_low_time(min_temp_time_adjusted):
            return 1

        return 0

    def transform_data_list(self, array_result):
        """
        Transform a list of data points from a MongoDB query to a simplified structure.

        Args:
        - array_result (list): The original list of data points from the MongoDB query.

        Returns:
        - list: A list of dictionaries with the transformed data structure.
        """
        transformed_result = []
        countNone = 0
        for item in array_result:
            # Use next to get the first 'valor' if 'data' is not empty, else None
            data_valor = next((val['valor'] for key in item['data']
                              for val in item['data'][key]), None)

            if data_valor is None:
                countNone += 1
                if countNone > 5:
                    return False

            transformed_result.append({
                'puntoObservacion': item['puntoObservacion'],
                'fechaTomaDato': item['fechaTomaDato'],
                'dataValor': data_valor
            })
        return transformed_result

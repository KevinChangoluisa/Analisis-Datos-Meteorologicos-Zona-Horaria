# -*- coding: utf-8 -*-
"""
Autor: Kevin Changoluisa
Correo electrónico: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""


from utils.MongoConnect import MongoDBConnection


class DataMongoDB:
    def __init__(self, configDB):
        """
        Initializes an instance of the DataPostgresDB class.

        Args:
            configDB (dict): The configuration for the PostgreSQL database.
        """
        self.conn = MongoDBConnection(configDB)
        self.conn.connect()

    def getData(self, puobcodi, start_date, end_date):
        """
        Retrieves data from MongoDB based on a specified filter.

        Args:
            puobcodi (str): The station code for which data is to be retrieved.
            start_date (datetime): The start date for data retrieval.
            end_date (datetime): The end date for data retrieval.

        Returns:
            list: A list of documents that match the filter.
        """

        filter_Query = {
            "puntoObservacion": puobcodi,
            'fechaTomaDato': {
                '$gte': start_date,
                '$lte': end_date
            }}

        collection_name = "data1h"
        sorted_Query = [('fechaTomaDato', 1)]
        limit_Query = 48
        projection = {
            "_id": 0,
            "puntoObservacion": 1,
            "fechaTomaDato": 1,
            "data.293161h.valor": 1
        }
        try:
            collection = self.conn.db[collection_name]
            result = collection.find(filter_Query, projection).sort(
                sorted_Query).limit(limit_Query)
            return list(result)
        except Exception as e:
            print("Error executing the query: ", e)

    def close_connection(self):
        """
        Closes the connection to MongoDB.
        """
        self.conn.close()

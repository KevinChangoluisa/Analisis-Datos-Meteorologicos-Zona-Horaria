# -*- coding: utf-8 -*-
"""
Autor: Kevin Changoluisa
Correo electrónico: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""

from pymongo import MongoClient


class MongoDBConnection:
    def __init__(self, config):

        self.host = config['HOST']
        self.database = config['DB']
        self.user = config['USERNAME']
        self.password = config['PASSWORD']
        self.port = config['PORT']

    def connect(self):
        try:
            uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            self.client = MongoClient(uri)
            self.db = self.client[self.database]
            # print("Conexión exitosa MongoDB")
        except Exception as e:
            print("Error al conectarse a la base de datos")
            print(e)
            self.db = None

    def close(self):
        if self.client is not None:
            self.client.close()
            # print("Conexión cerrada MongoDB")

    def query_select(self, query_filter, collection_name, projection=None, limit=None, sorted_by=None):
        try:
            collection = self.db[collection_name]
            result = collection.find(
                query_filter, projection) if projection is not None else collection.find(query_filter)
            if sorted_by is not None:
                result = result.sort(sorted_by)
            if limit is not None:
                result = result.limit(limit)

            string_list = [str(item) for item in result]
            return string_list
        except Exception as e:
            print("Error al ejecutar la consulta")
            print(e)
            return None


if __name__ == '__main__':
    conn = MongoDBConnection()
    conn.connect()

    # Definir las variables para la consulta
    collection_name = "data1h"
    filter_Query = {"puntoObservacion": 'M0024'}
    sorted_Query = [('fechaTomaDato', -1)]
    limit_Query = 1
    projection = {
        "_id": 1,
        "puntoObservacion": 1,
        "medioTransmision": 1,
        "fechaTomaDato": 1,
        "data": 1
    }

    # Realizar la consulta directamente
    try:
        collection = conn.db[collection_name]
        result = conn.query_select(
            filter_Query, collection_name, projection, limit_Query, sorted_Query)
        if len(result) > 0:
            print(result[0])

    except Exception as e:
        print("Error al ejecutar la consulta")
        print(e)

    conn.close()

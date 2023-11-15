# -*- coding: utf-8 -*-
"""
Autor: Kevin Changoluisa
Correo electrónico: kchangoluisa@hotmail.com
GitHub: https://github.com/KevinChangoluisa
"""


import psycopg2


class PostgresConnection:
    def __init__(self, config):

        self.host = config['HOST']
        self.database = config['DB']
        self.user = config['USERNAME']
        self.password = config['PASSWORD']
        self.port = config['PORT']

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port
            )
            self.cursor = self.conn.cursor()
        except Exception as e:
            print("Error al conectarse a la base de datos: ", e)
            self.conn = None

    def execute_query(self, query, params=None):
        if self.conn is None:
            print("No hay conexión a la base de datos")
            return None

        try:
            if params:
                self.cursor.execute(query, params)
            else:
                self.cursor.execute(query)

            if query.strip().lower().startswith("select"):
                result = self.cursor.fetchall()
                return result
            else:
                self.conn.commit()
                return None
        except Exception as e:
            print("Error al ejecutar la consulta: ", e)
            return None

    def close(self):
        if self.conn is not None:
            self.cursor.close()
            self.conn.close()


if __name__ == '__main__':
    conn = PostgresConnection()
    conn.connect()
    query = f'''SELECT DISTINCT
                puobcodi
            FROM
                administrative.vta_estaciones_todos'''
    result = conn.execute_query(query)
    print(result)
    conn.close()

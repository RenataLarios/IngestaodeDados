import mysql.connector as connector

config = {
    "user": "ingestao", 
    "password": "ingestao", 
    "host": "eedb-011-ingestao-dados.c3myzemak5ll.us-east-1.rds.amazonaws.com", 
    "database": "ingestao_dados_fila",
    "raise_on_warnings": True
}

class MySQLConnector:
    def __init__(self) -> None:
        self.__connection = None

    def __get_connection(self):
        if self.__connection is None:
            self.__connection = connector.connect(**config)

        return self.__connection
    
    def execute(self, query, params = None):
        connection = self.__get_connection()
        cursor = connection.cursor(buffered=True)
        cursor.execute(query, params)
        connection.commit()
        cursor.close()


    def get(self, query, params = None):
        connection = self.__get_connection()
        cursor = connection.cursor()
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        cursor.close()

        return results
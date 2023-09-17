import mysql.connector as connector
import os

config = {
    "user": os.getenv("MYSQL_USER"), 
    "password": os.getenv("MYSQL_PASSWORD"),  
    "host": os.getenv("MYSQL_HOST"), 
    "database": os.getenv("MYSQL_DATABASE"), 
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
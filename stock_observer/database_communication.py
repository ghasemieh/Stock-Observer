# Connecting to the database
# importing 'mysql.connector' as mysql for convenient
import mysql.connector as mysql
from pandas import DataFrame


class mysql_connection:
    def __init__(self, table_name: str):
        # connecting to the database using 'connect()' method
        # it takes 3 required parameters 'host', 'user', 'passwd'
        db = mysql.connect(
            host="localhost",
            user="root",
            passwd="password",
            database=table_name
        )
        # creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
        self.cursor = db.cursor()

    def create_db(self, name: str) -> None:
        # creating a databse called 'datacamp'
        # 'execute()' method is used to compile a 'SQL' statement
        # below statement is used to create tha 'datacamp' database
        db = mysql.connect(
            host="localhost",
            user="root",
            passwd="password"
        )

        # creating an instance of 'cursor' class which is used to execute the 'SQL' statements in 'Python'
        cursor = db.cursor()
        cursor.execute(f"CREATE DATABASE {name}")

    def create_table(self, table_name: str) -> None:
        self.cursor.execute(f"CREATE TABLE {table_name} (name VARCHAR(255), user_name VARCHAR(255))")

    def show_table(self) -> None:
        self.cursor.execute("SHOW TABLES")
        tables = self.cursor.fetchall()  # it returns list of tables present in the database
        # showing all the tables one by one
        for table in tables:
            print(table)

    def insert(self, table_name, values) -> None:
        """
        :param table_name:
        :param values: storing values in a variable
                values = ("Hafeez", "hafeez")
        :return: None
        """
        query = f"INSERT INTO {table_name} (name, user_name) VALUES (%s, %s)"
        # executing the query with values
        self.cursor.execute(query, values)
        # to make final output we have to run the 'commit()' method of the database object
        self.db.commit()

        print(self.cursor.rowcount, "record inserted")

    def insert_many(self, table_name, values) -> None:
        """
        :param table_name:
        :param values: storing values in a variable
                values = [("Peter", "peter"), ("Amy", "amy"), ("Michael", "michael"), ("Hennah", "hennah")]
        :return: None
        """
        query = f"INSERT INTO {table_name} (name, user_name) VALUES (%s, %s)"
        # executing the query with values
        self.cursor.execute(query, values)
        # to make final output we have to run the 'commit()' method of the database object
        self.db.commit()
        print(self.cursor.rowcount, "record inserted")

    def select(self, query) -> DataFrame:
        # getting records from the table
        self.cursor.execute(query)
        # fetching all records from the 'cursor' object
        records = self.cursor.fetchall()
        data = DataFrame()
        for record in records:
            data = data.append(record)

        return data

    def delete(self, query) -> None:
        """
        :param query: query = "DELETE FROM users WHERE id = 5"
        :return: None
        """
        # executing the query
        self.cursor.execute(query)
        # final step to tell the database that we have changed the table data
        self.db.commit()

    def update(self, query) -> None:
        """
        :param query: query = "UPDATE users SET name = 'Kareem' WHERE id = 1"
        :return: None
        """
        # executing the query
        self.cursor.execute(query)
        # final step to tell the database that we have changed the table data
        self.db.commit()

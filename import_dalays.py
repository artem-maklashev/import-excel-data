import mysql.connector

from db_process import DBProcess


def main():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="402986",
        database="goldengroup",
        port=3306)
    db_process = DBProcess(connection)


if __name__ == "__main__":
    main()
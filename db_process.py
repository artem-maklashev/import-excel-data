from datetime import timedelta
import warnings
import mysql.connector
from mysqlx import IntegrityError
import pandas as pd


class DB_Process:

    def __init__(self):
        self.connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="402986",
            database="goldengroup",
            port=8000
        )   
    

    def execute_update(self, query, data):
        # Создание курсора
        cursor = self.connection.cursor()
        # Выполнение SQL-запросов
        cursor.execute(query, data)

        # Закрытие курсора и подтверждение транзакции
        self.connection.commit()
        cursor.close()

    def get_gypsum_board(self):
        with  self.connection:
            query = """SELECT gb.id, 
                        tm.name AS trade_mark, 
                        bt.name AS 'type',
                        e.name AS edge,
                        l.value AS 'length',
                        t.value AS thickness,
                        w.value AS width
                        FROM gypsum_board gb
                        JOIN trade_mark tm ON gb.trade_mark_id = tm.id
                        JOIN board_types bt ON gb.board_types_id = bt.id
                        JOIN edge e ON gb.edge_id = e.id
                        JOIN thickness t ON gb.thickness_id = t.id
                        JOIN width w ON gb.width_id = w.id
                        JOIN `length` l ON gb.length_id = l.id;"""
            warnings.filterwarnings("ignore")
            df = pd.read_sql(query, self.connection)
        return df
    
    def create_production_log_record(self, p_date, stop_time, work_time, shift, shift_tag):
        if shift_tag == 1:
            production_start = p_date + timedelta(hours=8)
        elif shift_tag == 2:
            production_start = p_date + timedelta(hours=20)
        production_finish = production_start + \
            timedelta(minutes=(work_time - stop_time))
        production_start_str = production_start.strftime("%Y-%m-%d %H:%M:%S") if production_start else None
        production_finish_str = production_finish.strftime("%Y-%m-%d %H:%M:%S") if production_finish else None
        shift_id = get_shift_id(shift)
        product_types_id = 1
        print(production_start, production_finish, shift_id, product_types_id)

        try:
            with self.connection:
                cursor = self.connection.cursor()
                query = "INSERT INTO productionlog (production_start, production_finish, shift_id, product_types_id) VALUES (%s, %s, %s, %s)"
                cursor.execute(
                    query, (production_start_str, production_finish_str, shift_id, product_types_id))
                new_id = cursor.lastrowid
                self.connection.commit()
                production_log_id = new_id
                cursor.close()
        except IntegrityError as e:
            with self.connection:
                if cursor:
                    cursor.close()
                cursor = self.connection.cursor()
                select_query = "SELECT id FROM productionlog WHERE production_start = %s AND production_finish = %s AND shift_id = %s AND product_types_id = %s;"
                cursor.execute(select_query, (production_start_str,
                            production_finish_str, shift_id, product_types_id))
                existing_id = cursor.fetchone()[0]
                production_log_id = existing_id
                cursor.close()
        return production_log_id

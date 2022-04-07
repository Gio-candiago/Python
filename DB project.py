# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 17:25:36 2022

@author: Giovanni Candiago
"""
import sqlite3
import random

#Created a class that creates a string witht he needed n. of question marks
def question_marks(data):
    question_string = '('
    for i in data:
        question_string = question_string + '?,'
    question_string = question_string[:-1]
    question_string += ')'
    return(question_string)

#Creates the tuple that will be used to create a new order in the SQL
def random_orders_tuple(iata_list):
    #random weights and volumes for the orders (assuming each unit of volume weights between 150 and 210 units of weight - in line with cargotype data)
    volume = random.randint(5, 300)
    weight = volume * random.randint(150, 210)
    arrival_destination_list = random.sample(iata_list,2)
    query_tuple = (arrival_destination_list[0][0], arrival_destination_list[1][0], volume, weight,1)
    return(query_tuple)
      

class DBManager():
    #Creating a class that opens the connection to the database
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.cursor.execute('PRAGMA foreign_keys = ON')
    
    #closing the cursor and connection
    def close_connection(self):
        self.cursor.close()
        self.conn.close()
    
    def insert_query(self, table_name, field_names, data):
        #Defining the insert query
        insert_query = f'INSERT INTO {table_name} ({field_names}) VALUES {data}'
        self.cursor.execute(insert_query)
        self.conn.commit()
    
    def query_data(self, query):
        result = self.cursor.execute(query)
        self.conn.commit()
        return result.fetchmany(30)


tupl = (12, 'bitch', 'scratch', True)
db_manager = DBManager('cfv_start.db')
#returns a tuple with all the iata codes
iata_list = db_manager.query_data('SELECT "iata_code" FROM "Airports"')

for i in range(0,9990):
    query_data = random_orders_tuple(iata_list)
    db_manager.insert_query('Orders', 'Origin_iata,Destination_iata,Weight,Volume,Order_status', query_data)




db_manager.close_connection()



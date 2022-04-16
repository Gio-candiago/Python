# -*- coding: utf-8 -*-
"""
Created on Wed Apr  6 17:25:36 2022

@author: Giovanni Candiago
"""
import sqlite3
import random

#Creates a sequence to be used in the query to insert a new plane (the plane being randomly created)
def plane(location, status):
    cargo_type = random.randint(1,13)
    query_list_1 = (cargo_type, location[0], status)
    return (query_list_1)

#Functions used to turn 2d sequence (list containing 1d tuples) to a simple list with the first (and only) element of each tuple
def unwrap_tuples(list_tuple_2D):
    unwrapped_list = list()
    for count, item in enumerate(list_tuple_2D):
        unwrapped_list.append(item[0])
    return unwrapped_list
            

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
        return result.fetchall()
    
    def insert_flights_cargos(self, iata_sequence):
        #Distributing some assigned planes for each airport (with the relevant flights)
        for item in enumerate(iata_sequence):
            query_data_1 = plane(item[1], 1)
            db_manager.insert_query('Cargos','CargoType,Location_iata_code,Status', query_data_1)
            #Selecting a random arrival iata (ensuring departure is not double chosen)
            departure_iata_tuple = item[1]
            iata_sequence_local_copy = iata_sequence.copy()
            iata_sequence_local_copy.remove(departure_iata_tuple)
            # Having each cargo complete a lot of past flights
            for other_items in enumerate(iata_sequence_local_copy):
                arrival_iata = other_items[1][0]
                data = (item[1][0],arrival_iata,1)
                db_manager.insert_query('Flights', 'Departure_iata,Arrival_iata,Status', data)
    
    #function that returns the orders contained on a flight (manifest)
    def search_manifest(self, flight_number):
        query_manifest = f'SELECT "OrderID" FROM Orders WHERE "Flight" = "{flight_number}";' 
        flight_manifest = self.query_data(query_manifest)
        flight_manifest = unwrap_tuples(flight_manifest)
        return flight_manifest
    
    #As specified by the indications, the function returns a list of the flights per route; the first list contains the
    #flights that have been scheduled, the second list the flights that have been archived
    def search_flight_for_route(self, departure, destination):
        query_flight_for_route = f'SELECT "FlightNumber", "Status"  FROM Flights WHERE "Departure_iata" = "{departure}" AND "Arrival_iata" = "{destination}";'
        flight_per_route = self.query_data(query_flight_for_route)
        #creating two separate lists to return the archieved nad scheduled flights
        archived_flights_list = list()
        scheduled_flights_list = list()
        for item in flight_per_route:
            if item[1]==0:
                scheduled_flights_list.append(item[0])
            elif item[1]==1:
                archived_flights_list.append(item[0])
        return [scheduled_flights_list, archived_flights_list]
    
    #
    def search_unassigned_orders(self):
        query_unassigned_orders = 'SELECT "OrderID" FROM Orders WHERE "Order_Status" = 0;'
        orders_list = self.query_data(query_unassigned_orders)
        orders_list = unwrap_tuples(orders_list)
        return orders_list
    
    def search_available_planes_for_airport(self, airport_code):
        query_av_planes = f'SELECT "CargoID" FROM Cargos WHERE "Status" = "0" AND "location_iata_code" = "{airport_code}";' 
        plane_list = self.query_data(query_av_planes)
        plane_list = unwrap_tuples(plane_list)
        return plane_list 
    
    def plane_specs(self, current_location):
        query = f'SELECT CargoID, CargoTypes.Payload, CargoTypes.Volume FROM Cargos INNER JOIN CargoTypes ON Cargos.CargoType=CargoTypes.CargoTypeID WHERE Cargos.Location_iata_code = "{current_location}";'
        plane_specs = self.query_data(query)
        print(plane_specs)
        return plane_specs
    
    def check_if_orders_fit(self, orders, departure):
        plane_specs = self.plane_specs(departure)
        #Retrieving volume and weights
        orders_volumes_weights = self.query_data(f'SELECT "Weight", "Volume" FROM Orders WHERE OrderID IN {orders};')
        tot_vol = 0
        tot_wei = 0
        for order in orders_volumes_weights:
            tot_wei += int(order[0])
            tot_vol += int(order[1])
        for plane in plane_specs:
            if int(plane[1])>=tot_wei and int(plane[2]) >= tot_vol:
                return plane[0]
        return None
    
    #Assume orders are passed in a tuple
    def load_orders(self, orders, departure, destination):
        orders = str(orders)
        #fits contians the CargoID if orders fit in plane, otherwise it is None
        fits = self.check_if_orders_fit(orders, departure)
        if fits is not None:
            cargoID = str(fits)
            query_load_order = f'INSERT INTO Flights (Departure_iata, Arrival_iata, Status) VALUES("{departure}","{destination}",0);'
            self.query_data(query_load_order)
            FlightNumber = self.query_data('SELECT last_insert_rowid();')[0][0]
            update_aircrafts_query = f'UPDATE Cargos SET FlightNumber="{FlightNumber}" WHERE CargoID={cargoID};'
            update_orders_query = f'UPDATE Orders SET Order_Status = 1, Flight = "{FlightNumber}" WHERE OrderID IN {orders};'
            self.query_data(update_orders_query)
            self.query_data(update_aircrafts_query)
            return cargoID
        else: 
            return "No aircrafts with desired specs available at the moment"
    
    
    #Creates the tuple that will be used to create a new order in the SQL
    def random_orders_tuple(self, iata_sequence, order_status):
        #random weights and volumes for the orders (assuming each unit of volume weights between 150 and 210 units of weight - in line with cargotype data)
        weight = random.randint(5, 300)
        volume = weight * random.randint(150, 210)
        departure_arrival_list = random.sample(iata_sequence,2)
        departure = departure_arrival_list[0][0]
        arrival = departure_arrival_list[1][0]
        flights_list = self.search_flight_for_route(departure, arrival)
        flight = random.choice(flights_list[1])
        if order_status == 1:
            query_tuple = (departure,arrival,volume,weight,flight,order_status)
        elif order_status == 0:
            query_tuple = (departure,arrival,volume,weight,order_status)
        return(query_tuple)
    
database_name = 'cfv_start.db'
db_manager = DBManager(database_name)
'''
#    THE FOLLOWING COMMENTED SECTION OF CODE HAS BEEN USED TO POPULATE THE TABLES
#returns a tuple with all the iata codes
complete_iata_list = db_manager.query_data('SELECT "iata_code" FROM "Airports"')
#Taking a random subset of airports
iata_sequence = random.sample(complete_iata_list, 10)

for i in range(0,2):
    db_manager.insert_flights_cargos(iata_sequence)
#Creating some past orders that have been Shipped (status = 1)
for i in range(0,500):
    query_data = db_manager.random_orders_tuple(iata_sequence, 1)
    db_manager.insert_query('Orders', 'Origin_iata,Destination_iata,Weight,Volume,Flight,Order_status', query_data)
#Inserting some orders that have not been shipped (Status = 0)
for i in range(0,400):
    query_data = db_manager.random_orders_tuple(iata_sequence, 0)
    db_manager.insert_query('Orders', 'Origin_iata,Destination_iata,Weight,Volume,Order_status', query_data)


print(db_manager.search_manifest(1463))

print(db_manager.load_orders( (16520,16594), 'ROL', 'MVA'))

print(db_manager.search_flight_for_route('ROL', 'MVA'))

print(db_manager.search_unassigned_orders())

print(db_manager.search_available_planes_for_airport('AZR'))

'''
db_manager.close_connection()

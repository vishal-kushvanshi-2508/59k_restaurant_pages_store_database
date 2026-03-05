
from typing import List, Tuple
import json


import mysql.connector # Must include .connector




DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : "3306",
    "database" : "grab_food_db"
}

def get_connection():
    ## here ** is unpacking DB_CONFIG dictionary.
    connection = mysql.connector.connect(**DB_CONFIG)
    ## it is protect to autocommit
    connection.autocommit = False
    return connection

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS grab_food_db;")
    connection.commit()
    connection.close()
    # "CREATE DATABASE IF NOT EXISTS database_name;"
    # pass

def create_table():
    connection = get_connection()
    cursor = connection.cursor()
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS grab_food_restaurant( 
            id INT AUTO_INCREMENT PRIMARY KEY, 
            restaurant_id VARCHAR(100) UNIQUE, 
            restaurant_name VARCHAR (300), 
            cuisine TEXT, 
            rating DECIMAL(10,2), 
            restaurant_image TEXT, 
            distance VARCHAR(500), 
            opening_time JSON );
            """)

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS food_detail( 
            id INT AUTO_INCREMENT PRIMARY KEY, 
            restaurant_id VARCHAR(50),
            category_name VARCHAR(100), 
            food_id VARCHAR(100) UNIQUE, 
            food_name VARCHAR(100) NOT NULL,  
            price DECIMAL(10,2), 
            image_url TEXT, 
            description TEXT , 
            CONSTRAINT fk_restaurant
            FOREIGN KEY (restaurant_id) REFERENCES grab_food_restaurant(restaurant_id) 
            ON DELETE CASCADE );
            """)
    # cursor.execute("CREATE DATABASE IF NOT EXISTS grab_food_db;")
    connection.commit()
    connection.close()

batch_size_length = 1000
def data_commit_batches_wise(connection, cursor, sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
    ## this is save data in database batches wise.
    batch_count = 0
    for index in range(0, len(sql_query_value), batch_size):
        batch = sql_query_value[index: index + batch_size]
        cursor.executemany(sql_query, batch)
        batch_count += 1
        connection.commit()
    return batch_count


def insert_data_in_table(list_data : list):
    connection = get_connection()
    cursor = connection.cursor()
    parent_sql = """INSERT INTO grab_food_restaurant
                                (restaurant_id, restaurant_name, cuisine, rating, restaurant_image, opening_time, distance)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE restaurant_id = restaurant_id"""

    child_sql = """INSERT IGNORE INTO food_detail
                                   (restaurant_id, category_name, food_id, food_name, price, image_url, description )
                                   VALUES (%s, %s, %s, %s, %s, %s, %s )
                                ON DUPLICATE KEY UPDATE food_id = food_id"""
    try:
        rest_values = []
        menu_values = []
        for dict_data in list_data:
            restaurant_menu_tuple = tuple(dict_data.values())
            restaurant_tuple_with_dict = tuple(restaurant_menu_tuple[0].values())
            json_opening_time = json.dumps(restaurant_tuple_with_dict[5])
            parent_tuple = restaurant_tuple_with_dict[:5] + (json_opening_time,) + restaurant_tuple_with_dict[6:]
            rest_values.append(parent_tuple)
            if len(restaurant_menu_tuple) < 2:
                continue
            # menu_list = []
            for food_data in restaurant_menu_tuple[1]:
                menu_values.append(tuple(food_data.values()))

        batch_count = data_commit_batches_wise(connection, cursor, parent_sql, rest_values)
        print("batch size  parent : ", batch_count)
        batch_count = data_commit_batches_wise(connection, cursor, child_sql, menu_values)
        print("batch size  parent : ", batch_count)
        cursor.close()
        connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        connection.rollback()
        print(f"Transaction failed, rolled back. Error: {e}")
    finally:
        connection.close()

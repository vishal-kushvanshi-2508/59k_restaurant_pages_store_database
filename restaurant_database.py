


#
from typing import List, Tuple
import json

import re
from threading import Thread

import mysql.connector # Must include .connector

from concurrent_log_handler import ConcurrentRotatingFileHandler

import logging

formatter = logging.Formatter(
    "{lineno} | {asctime} | {name} | {levelname} | {threadName} | {message}",
    style="{"
)

# -------- Logger 1 --------
logger = logging.getLogger("grab_food")
logger.setLevel(logging.DEBUG)

food_handler = ConcurrentRotatingFileHandler(
    "grab_food_logging_file.log",
    mode="a",
    # maxBytes=50 * 1024 * 1024,
    # backupCount=5,
    encoding="utf-8"
)

food_handler.setFormatter(formatter)

if not logger.handlers:
    logger.addHandler(food_handler)

# -------- Logger 2 --------
db_file = logging.getLogger("database")
db_file.setLevel(logging.DEBUG)

db_handler = ConcurrentRotatingFileHandler(
    "database_log_file.log",
    mode="a",
    # maxBytes=50 * 1024 * 1024,
    # backupCount=5,
    encoding="utf-8"
)

db_handler.setFormatter(formatter)

if not db_file.handlers:
    db_file.addHandler(db_handler)





DB_CONFIG = {
    "host" : "localhost",
    "user" : "root",
    "password" : "actowiz",
    "port" : 3306,
    "database" : "grab_food_db"
}

def get_connection():
    try:
        ## here ** is unpacking DB_CONFIG dictionary.
        connection = mysql.connector.connect(**DB_CONFIG)
        ## it is protect to autocommit
        connection.autocommit = False
        logger.info("Database connection established")
        return connection
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def create_db():
    connection = get_connection()
    # connection = mysql.connector.connect(**DB_CONFIG)
    cursor = connection.cursor()
    cursor.execute("CREATE DATABASE IF NOT EXISTS grab_food_db;")
    connection.commit()
    connection.close()

def create_table():
    connection = get_connection()
    cursor = connection.cursor()
    try:
        logger.info("Starting table creation")
        table_queries = {
            "grab_food_restaurant": """
                CREATE TABLE IF NOT EXISTS grab_food_restaurant(
                id INT AUTO_INCREMENT PRIMARY KEY,
                restaurant_id VARCHAR(100) UNIQUE,
                restaurant_name VARCHAR (300),
                cuisine TEXT,
                rating DECIMAL(10,2),
                restaurant_image TEXT,
                distance VARCHAR(500),
                opening_time JSON );
            """,
            "category_detail": """
                CREATE TABLE IF NOT EXISTS category_detail(
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    restaurant_id VARCHAR(100),
                    categories_id VARCHAR(150),
                    categories_name VARCHAR(150),
                    item_id VARCHAR(150) UNIQUE ,
                    item_name VARCHAR(150) NOT NULL,
                    image_url TEXT,
                    description TEXT ,
                    price VARCHAR(170) ,
                    INDEX idx_restaurant_id (restaurant_id),
                    CONSTRAINT fk_restaurant
                    FOREIGN KEY (restaurant_id) REFERENCES grab_food_restaurant(restaurant_id)
                    ON DELETE CASCADE  );
                """
        }
        for table_name, query in table_queries.items():
            # print("table name : ", table_name)
            # print("table name : ", query)
            query_without_enter = " ".join(query.split())
            db_file.info(
                query_without_enter
            )
            cursor.execute(query)
        connection.commit()
        logger.info("All tables checked/created successfully")
    except Exception as e:
        logger.exception("Table creation failed")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


### using thread

def fun1(sql_query, batch ):
    # retry = 3
    try:
        connection = get_connection()
        cursor = connection.cursor()
        cursor.executemany(sql_query, batch)
        connection.commit()
        query_without_enter = " ".join(sql_query.split())
        values = ", ".join(str(t_data) for t_data in batch)
        recovery_sql_query = re.sub(
            r"\(\s*(%s\s*,\s*)*%s\s*\)",
            lambda m: values,
            query_without_enter
        )
        db_file.info(
            recovery_sql_query
        )
        # connection.commit()
        if cursor:
            cursor.close()
        if connection:
            connection.close()
        return
    except Exception as e:
        logger.error(f"Batch insert failed error={e}")


## batch wise 1
from concurrent.futures import ThreadPoolExecutor

MAX_DB_THREADS = 2
batch_size_length = 1000
def data_commit_batches_wise(sql_query: str, sql_query_value: List[Tuple], batch_size: int = batch_size_length):

    batches = [
        sql_query_value[i:i+batch_size]
        for i in range(0, len(sql_query_value), batch_size)
    ]

    logger.info(f"Starting batch processing total_batches={len(batches)}")

    with ThreadPoolExecutor(max_workers=MAX_DB_THREADS) as executor:
        futures = []
        for batch in batches:
            futures.append(executor.submit(fun1, sql_query, batch))

        for f in futures:
            f.result()

    logger.info("Completed batch processing")


# ### batch wise 2
#
# batch_size_length = 200
# def data_commit_batches_wise(sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
#     ## this is save data in database batches wise.
#     threads = []
#     logger.info(
#         f"Starting batch processing total_records={len(sql_query_value)}"
#     )
#     for index in range(0, len(sql_query_value), batch_size):
#         batch = sql_query_value[index: index + batch_size]
#         thread_obj = Thread(target=fun1, args=(sql_query, batch))
#         thread_obj.start()
#         threads.append(thread_obj)
#         # thread_obj.join()
#
#     for tread_obj in threads:
#         tread_obj.join()
#     logger.info(f"Completed batch processing threads={len(threads)}")
#     return len(threads)




#### with using tread ....
# batch_size_length = 100
# def data_commit_batches_wise(sql_query : str, sql_query_value: List[Tuple], batch_size: int = batch_size_length ):
#     ## this is save data in database batches wise.
#     connection = get_connection()
#     cursor = connection.cursor()
#     try:
#         batch_count = 0
#         for index in range(0, len(sql_query_value), batch_size):
#             batch = sql_query_value[index: index + batch_size]
#             cursor.executemany(sql_query, batch)
#             batch_count += 1
#             connection.commit()
#         return batch_count
#     except Exception as e:
#         ## this exception execute when error occur in try block and rollback until last save on database .
#         connection.rollback()
#         print(f"Transaction failed, rolled back. Error: {e}")
#     finally:
#         connection.close()




def insert_data_in_table(list_data : list):
    # connection = get_connection()
    # cursor = connection.cursor()
    parent_sql = """INSERT INTO grab_food_restaurant
                                (restaurant_id, restaurant_name, cuisine, rating, restaurant_image, opening_time, distance)
                                VALUES (%s, %s, %s, %s, %s, %s, %s)
                                ON DUPLICATE KEY UPDATE restaurant_id = restaurant_id"""

    child_sql = """INSERT IGNORE INTO category_detail
                                   (restaurant_id, categories_name, item_id, item_name, price, image_url, description)
                                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                                    ON DUPLICATE KEY UPDATE item_id = item_id"""
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

        try:
            batch_count = data_commit_batches_wise(parent_sql, rest_values)
            logger.info(f"Parent batches executed count={batch_count}")
            print("batch size  parent : ", batch_count)
            batch_count = data_commit_batches_wise(child_sql, menu_values)
            logger.info(f"Child batches executed count={batch_count}")
            print("batch size  child : ", batch_count)
        except Exception as e:
            print(f"data not divide in batch . Error: ")
        # cursor.close()
        # connection.close()

    except Exception as e:
        ## this exception execute when error occur in try block and rollback until last save on database .
        # connection.rollback()
        print(f"wrong data fetch in tuple: {e}")
        # connection.rollback()
        # print(f"Transaction failed, rolled back. Error: {e}")
        logger.exception("Transaction failed. Rolling back")
    except:
        print("except error raise ")
    # finally:
    #     connection.close()









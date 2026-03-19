
import sys
import time

from restaurant_database import create_db, create_table, insert_data_in_table
from extract_data_from_zip_file import read_files_zip, extract_grab_food_data, process_chunk

DIR_PATH = "C:/Users/vishal.kushvanshi/PycharmProjects/restaurant_59k_pages_data_extract_save/PDP/PDP/"


from concurrent.futures import ThreadPoolExecutor
import os
import time

def main():
    start_time = time.time()

    create_table()
    print("table and db create")

    files = os.listdir(DIR_PATH)
    files.sort()

    num_threads = 6   # same as your 6 terminals

    chunk_size = len(files) // num_threads

    file_chunks = [
        files[i:i + chunk_size]
        for i in range(0, len(files), chunk_size)
    ]

    print(f"Total files: {len(files)}")
    print(f"Total chunks: {len(file_chunks)}")

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        executor.map(lambda chunk: process_chunk(chunk, DIR_PATH), file_chunks)

    print("Total time:", time.time() - start_time)




if __name__ == "__main__":
    # start = time.time()
    main()
    # end = time.time()
    # print("time different  : ", end - start)




# first method of code

# def main():
#     start = int(sys.argv[1])
#     end = int(sys.argv[2])
#
#     create_table()
#     print("table and db create")
#     restaurant_detail_list = []
#     for raw_dict in read_files_zip(DIR_PATH, start, end):
#         result = extract_grab_food_data(raw_dict)
#         if not result:
#             continue
#
#         restaurant_detail_list.append(result)
#
#         if len(restaurant_detail_list) >= 2000:  # large batch
#             insert_data_in_table(list_data=restaurant_detail_list)
#             restaurant_detail_list.clear()
#
#     if restaurant_detail_list:
#             insert_data_in_table(list_data=restaurant_detail_list)
#     print("extract result data ; ", type(restaurant_detail_list))
#     print("add recode  database time .. : ", time.time() - start)


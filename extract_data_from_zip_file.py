

import os
import json
import gzip

def read_files_zip(path: str, start, end):

    try:
        files = os.listdir(path)
        files.sort()
        for file in files[start:end]:
            filename = os.path.join(path, file)
            with gzip.open(filename, "rt", encoding="utf-8") as f:
                yield json.load(f)
    except Exception as e:
        print("Error in func:", read_files_zip.__name__, '\nError: ', e)


def extract_grab_food_data(single_page_data):
    ## extract from list of data of grab food pages.
    restaurant_dict = {}
    grab_food_dict = {}
    if not single_page_data.get("merchant") :
        return None
    grab_food_dict["restaurant_id"] = single_page_data.get("merchant").get("ID")
    grab_food_dict["restaurant_name"] = single_page_data.get("merchant").get("name")
    grab_food_dict["cuisine"] = single_page_data.get("merchant").get("cuisine")
    grab_food_dict["rating"] = single_page_data.get("merchant").get("rating")
    grab_food_dict["restaurant_image"] = single_page_data.get("merchant").get("photoHref")
    opening_time_dict = single_page_data.get("merchant").get("openingHours")
    grab_food_dict["opening_time"] = {
        "sunday" : opening_time_dict.get("sun"),
        "monday" : opening_time_dict.get("mon"),
        "tuesday" : opening_time_dict.get("tue"),
        "wednesday" : opening_time_dict.get("wed"),
        "thursday" : opening_time_dict.get("thu"),
        "friday" : opening_time_dict.get("fri"),
        "saturday" : opening_time_dict.get("sat")
    }
    grab_food_dict["distance"] = str (single_page_data.get("merchant").get("distanceInKm") ) + " " + "Km"
    grab_food_dict["cuisine"] = single_page_data.get("merchant").get("cuisine")

    menu_list = single_page_data.get("merchant").get("menu").get("categories", [])
    products_list = []
    if not menu_list:
        restaurant_dict["restaurant_detail"] = grab_food_dict
        return restaurant_dict
        # restaurant_list.append(Rest_Data)
        # continue
    for data in menu_list:
        item_name = data["name"]
        for food_dict in data.get("items", []):
            if food_dict.get("priceInMinorUnit"):
                price_amount = (food_dict.get("priceInMinorUnit")) / 100
                # print("not price value ")
            else:
                price_amount = 0
            item_dict = {
                "restaurant_id" : single_page_data.get("merchant").get("ID"),
                "category_name": item_name,
                "food_id" : food_dict.get("ID"),
                "food_name" : food_dict.get("name"),
                "price" : price_amount,
                "image_url" : food_dict.get("imgHref"),
                "description" : food_dict.get("description")
            }
            products_list.append(item_dict)

    restaurant_dict["restaurant_detail"] = grab_food_dict
    restaurant_dict["Menu_Items"] = products_list
    return restaurant_dict

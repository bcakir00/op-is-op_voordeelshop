from MongoDB_to_PostgreSQL import *

buids = []


def init():
    get_buids()


def get_buids():
    for entry in db["profiles"].find():
        if "buids" in entry:
            if len(entry["buids"]) > 0:
                buids.append((entry["buids"][0], str(entry["_id"])))
    buids.sort()
    print("Done with the setup of the buids.")


def link_profile_session(entry):
    low = 0
    high = len(buids) - 1

    try:
        session_id = entry["buid"][0]
        while low <= high:
                middle = (low + high) // 2
                if buids[middle][0] == session_id:
                    return buids[middle][1]
                elif buids[middle][0] > session_id:
                    high = middle - 1
                else:
                    low = middle + 1
    except:
        return -1

    return -1


def get_normalized_tables_id(search_value, search_name):
    if search_value is not None:
        search_value = search_value.replace("'", "''")
        cursor.execute(f"select _id from {search_name} where {search_name} = '{search_value}'")
    else:
        cursor.execute(f"select _id from {search_name} where {search_name} is null")
    results = cursor.fetchall()
    return results[0][0]


def get_brand_id(entry):
    try:
        return get_normalized_tables_id(entry["brand"], "brand")
    except:
        return -1


def get_category_id(entry):
    try:
        return get_normalized_tables_id(entry["category"], "category")
    except:
        return -1


def get_sub_category_id(entry):
    try:
        return get_normalized_tables_id(entry["sub_category"], "sub_category")
    except:
        return -1


def get_sub_sub_category_id(entry):
    try:
        return get_normalized_tables_id(entry["sub_sub_category"], "sub_sub_category")
    except:
        return -1


def get_color_id(entry):
    try:
        return get_normalized_tables_id(entry["color"], "color")
    except:
        return -1


def get_gender_id(entry):
    try:
        return get_normalized_tables_id(entry["gender"], "gender")
    except:
        return -1

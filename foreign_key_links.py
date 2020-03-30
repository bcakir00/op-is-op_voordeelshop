from MongoDB_to_PostgreSQL import *

buids = []


def init():
    get_buids()


def get_buids():
    for entry in db["profiles"].find():
        if "buids" in entry:
            if len(entry["buids"]) > 0:
                for buid in entry["buids"]:
                    buids.append((buid, str(entry["_id"])))
    buids.sort()
    print("Done with the setup of the buids.")


def link_buid(entry):
    low = 0
    high = len(buids) - 1

    session_id = entry["buid"][0]
    while low <= high:
        middle = (low + high) // 2
        if buids[middle][0] == session_id:
            return buids[middle][1]
        elif buids[middle][0] > session_id:
            high = middle - 1
        else:
            low = middle + 1


def get_session_duration(entry):
    start_time = entry["session_start"]
    end_time = entry["session_end"]
    duration = end_time - start_time
    return int(duration.total_seconds())


def get_normalized_tables_id(search_value, search_name):
    if search_value is not None:
        search_value = search_value.replace("'", "''")
        cursor.execute(f"select _id from {search_name} where {search_name} = '{search_value}'")
    else:
        cursor.execute(f"select _id from {search_name} where {search_name} is null")
    results = cursor.fetchall()
    return results[0][0]


def get_brand_id(entry):
    return get_normalized_tables_id(entry["brand"], "brand")


def get_category_id(entry):
    return get_normalized_tables_id(entry["category"], "category")


def get_sub_category_id(entry):
    return get_normalized_tables_id(entry["sub_category"], "sub_category")


def get_sub_sub_category_id(entry):
    return get_normalized_tables_id(entry["sub_sub_category"], "sub_sub_category")


def get_color_id(entry):
    return get_normalized_tables_id(entry["color"], "color")


def get_gender_id(entry):
    return get_normalized_tables_id(entry["gender"], "gender")


def bought_profile_id(entry):
    if entry["has_sale"]:
        return link_buid(entry)


def bought_product_id(entry):
    if entry["has_sale"]:
        profile_id = link_buid(entry)
        for product_index in range(len(entry["order"]["products"]) - 1):
            upload_values.append((profile_id, entry["order"]["products"][product_index]["id"]))
        return entry["order"]["products"][-1]["id"]


def viewed_product_id(entry):
    profile_id = str(entry["_id"])
    for product_index in range(len(entry["recommendations"]["viewed_before"]) - 1):
        upload_values.append((profile_id, entry["recommendations"]["viewed_before"][product_index]))
    return entry["recommendations"]["viewed_before"][-1]

import psycopg2
from pymongo import MongoClient
from foreign_key_links import *

cnx = psycopg2.connect("dbname=huwebshop user=postgres password=postgres")  # TODO: edit this.
cursor = cnx.cursor()

limit = -1
client = MongoClient('localhost', 27017)
db = client["huwebshop"]


def get_values(normalized, collection, values, get_fk):
    counter = 0
    normalized_list = []
    upload_values = []
    for entry in collection.find():
        upload = [counter]
        for value in values:
            if "-" in value:
                array_value = value.split("-")
                vl = entry
                try:
                    for sep in array_value:
                        vl = vl[sep]
                except KeyError:
                    vl = None
                upload.append(vl)
            elif value == "?":
                upload.append(get_fk(entry))
            else:
                if value in entry:
                    upload.append(entry[value])
                else:
                    upload.append(None)

        if not [i for i in upload if i in normalized_list] and normalized or not normalized:
            upload_values.append(tuple(upload))
            normalized_list.extend(upload)
            counter += 1

        if counter == limit and not normalized and limit != -1:
            break

    return upload_values


def set_values(table_name, upload_values, headers):
    parameters = ""
    value_string = ""
    for header in headers:
        parameters += header + ","
        value_string += "%s,"
    parameters = parameters[:-1]
    value_string = value_string[:-1]

    cursor.executemany(f"INSERT INTO {table_name}({parameters}) VALUES ({value_string})", upload_values)
    cnx.commit()


def create_table(normalized, table_name, db_name, values, headers, get_fk=None):
    collection = db[db_name]
    cursor.execute(f"DELETE FROM {table_name};")

    upload_values = get_values(normalized, collection, values, get_fk)
    set_values(table_name, upload_values, headers)

    print(f"{table_name} table uploaded using {db_name} database.")


if __name__ == "__main__":
    init()
    create_table(True, "brand", "products", ["brand"], ["_id", "brand"])
    create_table(True, "category", "products", ["category"], ["_id", "category"])
    create_table(True, "sub_category", "products", ["sub_category"], ["_id", "sub_category"])
    create_table(True, "sub_sub_category", "products", ["category"], ["_id", "sub_sub_category"])
    create_table(True, "color", "products", ["color"], ["_id", "color"])
    create_table(True, "gender", "products", ["gender"], ["_id", "gender"])
    create_table(False, "profiles", "profiles", ["recommendations-segment", "order-count"], ["_id", "recommendation_segment", "order_count"])
    create_table(False, "sessions", "sessions", ["has_sale", "user_agent-device-family", "user_agent-device-brand", "user_agent-os-familiy", "?"], ["sessions_id", "has_sale", "device_family", "device_brand", "os", "profid"], link_profile_session)
    cnx.close()

import psycopg2
from pymongo import MongoClient
from foreign_key_links import *
import bson
import csv
import os
import time

cnx = psycopg2.connect("dbname=huwebshop user=postgres password=postgres")  # TODO: edit this.
cursor = cnx.cursor()

client = MongoClient('localhost', 27017)
db = client["huwebshop"]
difference_time = time.time()
upload_values = []
counter = 0


def get_values(normalized, collection, values, get_fk):
    global difference_time, counter
    normalized_list = []

    for entry in collection.find():
        if counter % 10000 == 0:
            print("Counter: ", counter)
            print("Time difference: ", time.time() - difference_time)
            difference_time = time.time()

        upload = []
        value_index = 0
        for value in values:
            if value == "x":
                upload = [counter]
            elif "-" in value:
                array_structure = value.split("-")
                array_value = entry
                try:
                    for sep in array_structure:
                        array_value = array_value[sep]
                except KeyError:
                    array_value = None
                upload.append(array_value)
            elif value == "?":
                try:
                    upload.append(get_fk[value_index](entry))
                except:
                    upload.append(None)
                value_index += 1
            else:
                if value in entry:
                    if type(entry[value]) == bson.objectid.ObjectId:
                        upload.append(str(entry[value]))
                    else:
                        upload.append(entry[value])
                else:
                    upload.append(None)

        if not normalized:
            if -1 not in upload:
                upload_values.append(tuple(upload))
                counter += 1
        else:
            if upload[1] not in normalized_list:
                normalized_list.append(upload[1])
                upload_values.append(tuple(upload))
                counter += 1

    return upload_values


def get_path(file_name):
    # Getting the path to save the file
    script_dir = os.path.dirname(__file__)
    rel_path = f"excel_files/{file_name}.csv"
    abs_file_path = os.path.join(script_dir, rel_path)
    return abs_file_path


def create_csv_file(table_name):
    # Writing the upload values to a csv file.
    print(f"Creating the {table_name} database contents...")
    with open(get_path(table_name), 'w', newline='', encoding='utf-8') as csvout:
        writer = csv.writer(csvout)
        writer.writerow(list(upload_values[0]))
        for value in upload_values:
            writer.writerow(list(value))
    print(f"Finished creating the {table_name} database contents.\n")


def create_table(normalized, table_name, db_name, values, get_fk=None):
    global upload_values
    collection = db[db_name]
    cursor.execute(f"DELETE FROM {table_name};")

    upload_values = get_values(normalized, collection, values, get_fk)
    create_csv_file(table_name)


def upload_files():
    file_names = ["brand", "category", "sub_category", "sub_sub_category", "color", "gender",
                  "products", "profiles", "sessions", "products_bought", "viewed_products"]
    problem_files = [9, 10]

    for file_index in range(len(file_names)):
        file_name = file_names[file_index]
        # Making a placeholder table to go around the constraints because of the imperfect data.
        cursor.execute("DROP TABLE IF EXISTS placeholder CASCADE")
        cursor.execute("""CREATE TABLE placeholder (_id VARCHAR PRIMARY KEY, column1 VARCHAR, column2 VARCHAR);""")
        cnx.commit()

        try:
            cursor.execute(f"TRUNCATE {file_name} CASCADE;")
            with open(get_path(file_name)) as csvfile:
                table_name = file_name if file_index not in problem_files else "placeholder"
                cursor.copy_expert("COPY " + table_name + " FROM STDIN DELIMITER ',' CSV HEADER;", csvfile)
                cnx.commit()

            if file_index in problem_files:
                cursor.execute(f"INSERT INTO {file_name} (_id, profile_id, product_id) SELECT p._id, p.column1, "
                               f"p.column2 FROM placeholder AS p INNER JOIN products ON p.column2 = products._id")
                cnx.commit()

            print(f"Uploaded {file_name}.csv to the {file_name} table.")
        except FileNotFoundError:
            print(f"{file_name} could not be located.")

    # Deleting the temporary table and committing the uploads.
    cursor.execute("DROP TABLE IF EXISTS placeholder CASCADE")
    cnx.commit()


def create_tables():
    create_table(True, "brand", "products", ["x", "brand"])
    create_table(True, "category", "products", ["x", "category"])
    create_table(True, "sub_category", "products", ["x", "sub_category"])
    create_table(True, "sub_sub_category", "products", ["x", "sub_sub_category"])
    create_table(True, "color", "products", ["x", "color"])
    create_table(True, "gender", "products", ["x", "gender"])
    create_table(False, "profiles", "profiles", ["_id", "recommendations-segment", "order-count"])
    create_table(False, "sessions", "sessions", ["_id", "has_sale", "user_agent-device-family",
                 "user_agent-device-brand", "user_agent-os-familiy", "?", "?"], [link_buid, get_session_duration])
    create_table(False, "products", "products", ["_id", "?", "?", "?", "?", "?", "?", "price-selling_price"],
                 [get_brand_id, get_category_id, get_sub_category_id, get_sub_sub_category_id, get_color_id, get_gender_id])
    create_table(False, "viewed_products", "profiles", ["x", "_id", "?"], [viewed_product_id])
    create_table(False, "products_bought", "sessions", ["x", "?", "?"], [bought_profile_id, bought_product_id])


if __name__ == "__main__":
    # init()
    # create_tables()
    upload_files()
    cursor.close()
    cnx.close()

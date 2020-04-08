import psycopg2
import random
import string

conn = psycopg2.connect(database="huwebshop", user="postgres", password="0penN!!!")
cursor = conn.cursor()


def get_product_id(profid):
    cursor.execute(f"select product_id from viewed_products where profile_id = '{profid}' LIMIT 1;")
    viewed_products = cursor.fetchall()

    if not viewed_products:
        # print("Nieuwe websitebezoeker!", viewed_products)
        cursor.execute(f"select product_id from viewed_products LIMIT 1;")
        viewed_products = cursor.fetchall()

    return viewed_products[0][0]


def get_product_attributes(prodid):
    cursor.execute(f"select sub_sub_category_id, color_id, brand_id, gender_id from products where _id = '{prodid}';")
    attributes = cursor.fetchall()
    return list(attributes[0])


def get_similar_product(prodid, check_value):
    cursor.execute(f"select _id from products where _id != '{prodid}'and sub_sub_category_id = '{check_value[0]}'"
                   f"and color_id = '{check_value[1]}' and brand_id = '{check_value[2]}' and gender_id = '{check_value[3]}' LIMIT 4;")
    fetch_results = cursor.fetchall()
    recommended_products = [fetch_result[0] for fetch_result in fetch_results]
    return recommended_products


def do_all():
    cursor.execute("TRUNCATE product_recommendations CASCADE;")
    cursor.execute(f"select _id from profiles;")
    profile_ids = cursor.fetchall()
    counter = 0
    for profile_id in profile_ids:
        product_id = get_product_id(profile_id[0])
        attributes = get_product_attributes(product_id)
        similar_products = get_similar_product(product_id, attributes)
        for similar_product in similar_products:
            cursor.execute("INSERT INTO product_recommendations (profile_id, product_id) VALUES(%s, %s)", (profile_id, similar_product))
            if counter > 100:
                conn.commit()
                conn.close()
                exit()
            print(counter)
            counter += 1


do_all()
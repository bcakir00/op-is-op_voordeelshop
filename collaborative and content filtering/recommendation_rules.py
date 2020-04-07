import psycopg2

c = psycopg2.connect("dbname=huwebshop user=postgres password=postgres")  # TODO: edit this.
cur = c.cursor()


def insert_recommendations(max_amount):
    cur.execute(f"""select profid from sessions LIMIT {max_amount};""")
    product_ids = cur.fetchall()

    # Getting all the ids of the similar products and uploading them.
    recommendation_values = []
    counter = 0
    for pid in product_ids:
        if pid[0] is not None:
            recommendation_values.append(list(pid) + get_recommendation(pid[0].replace("'", "''")))
            counter += 1
            print(counter)

    # Converting the upload_values in the correct format
    upload_values = []
    for upload_value in recommendation_values:
        for value_index in range(1, len(upload_value)):
            upload_values.append([upload_value[0], upload_value[value_index]])

    cur.executemany(f"""INSERT INTO product_recommendations (profile_id, product_id) VALUES (%s, %s)""", upload_values)
    print(f"{counter} entries inserted into the product_recommendations table.")


def similar_products(product_id, attributes):
    cur.execute(f"""select {get_attributes_query(attributes)} from products where _id = '{product_id}';""")
    check_properties = cur.fetchall()[0]

    # Fetching all the id's of 'similar' products (similar is easily definable by changing the attributes parameter).
    conditions = get_conditions_query(attributes, check_properties)
    if None in check_properties:
        return [product_id, product_id, product_id, product_id]

    cur.execute(f"""select _id from products where _id != '{product_id}' and {conditions} LIMIT 4;""")
    matching_products = cur.fetchall()
    return [matching_product[0] for matching_product in matching_products]


def similar_profile(profile_id, attributes):
    # Check for similar profiles
    cur.execute(f"""select {get_attributes_query(attributes)} from sessions where profid = '{profile_id}';""")
    properties_to_match = list(cur.fetchall()[0])

    # Pick random profile
    conditions = get_conditions_query(attributes, properties_to_match)
    cur.execute(f"""select profid from sessions where profid != '{profile_id}' and {conditions} 
                    and has_sale = true LIMIT 1;""")
    profile = cur.fetchall()
    return get_recommendation_products(profile[0][0]) if profile else get_recommendation_products(profile)


def get_attributes_query(attributes):
    # Making the attribute in a format for the select statements.
    attribute_string = ""
    for attribute in attributes:
        attribute_string += attribute + ","
    attribute_string = attribute_string[:-1]
    return attribute_string


def get_conditions_query(attributes, info):
    cond = ""
    for index in range(len(info)):
        cond += attributes[index] + " = " + "'" + str(info[index]) + "'"
        if index < len(info) - 1:
            cond += " and "
    return cond


def get_recommendation_products(profile_id):
    cur.execute(f"""select product_id from products_bought where profile_id = '{profile_id}' LIMIT 1;""")
    bought_products_ids = cur.fetchall()

    # This is to account for the imperfect data.
    if not bought_products_ids:
        cur.execute(f"""select product_id from products_bought LIMIT 1;""")
        bought_products_ids = cur.fetchall()

    rand_product_id = bought_products_ids[0][0]
    return similar_products(rand_product_id, ["sub_sub_category_id", "gender_id"])


def get_recommendation(profile_id):
    # Check if profile id has a sale in the sessions.
    cur.execute(f"""select has_sale from sessions where profid = '{profile_id}' and has_sale = true;""")
    has_sale = cur.fetchall()

    # if has_sale choose random product with profile id and get a similar product via the database recommendation table
    # else check for a similar profile.
    return get_recommendation_products(profile_id) if has_sale else similar_profile(profile_id, ["device_family", "os"])


def get_recommendation_from_table(profile_id):
    cur.execute(f"""select * from product_recommendations where profile_id = '{profile_id}';""")
    recommendations = cur.fetchall()
    return_recommendations = [recommendation[-1] for recommendation in recommendations]
    return return_recommendations


# TODO: let this line run once and then comment it out.
insert_recommendations(100)
# [debug purpose] (has sale: 5b017d5f798e760001e9e25f) (doesn't have sale: 5c2f03060279d20001f5c17e).
# print(get_recommendation_from_table("59dce84fa56ac6edb4cd0e88"))  # or use get_recommendation() to get generate.

c.commit()
cur.close()
c.close()

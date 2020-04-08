import psycopg2
from psycopg2 import sql


def connect_to_server():
    """"make connection to server and return connection object"""
    return psycopg2.connect("dbname=nickServer user=postgres password=admin")


def create_recommendation_table(connection, cursor):
    """"Creates table where the recommendation to the user are stored."""
    cursor.execute("DROP TABLE IF EXISTS recommendations")
    cursor.execute("""CREATE TABLE recommendations
                    (profid VARCHAR,
                     prodid VARCHAR);""")
    connection.commit()


def get_all_profid(cursor):
    """"get all profile id's"""
    cursor.execute(sql.SQL("SELECT _id FROM {};").format(sql.Identifier('profiles')), [])
    return cursor.fetchall()


def get_user_has_sale(cursor, profid):
    """"get boolean value from profile if user made sale"""
    cursor.execute(sql.SQL("SELECT has_sale FROM {} WHERE profid=%s;").format(sql.Identifier('sessions')),
        [profid])

    rows = cursor.fetchall()

    for row in rows:
        if row == True:
            return True

    return False


def get_products_viewed(cursor, profid):
    """"get list of products viewed by specific user."""
    cursor.execute(sql.SQL("SELECT product_id FROM {} WHERE profile_id=%s;").
                   format(sql.Identifier('viewed_products')), [profid])
    return cursor.fetchall()


def get_product_attributes(cursor, prodid):
    """"Get brand, category, subcategory and subsubcategory for a product."""
    cursor.execute(sql.SQL("SELECT brand_id, category_id, sub_category_id, sub_sub_category_id FROM {} WHERE _id=%s;").
                   format(sql.Identifier('products')), [prodid])
    return cursor.fetchall()


def get_similar_products_attribute(cursor, attribute, attribute_value):
    """"Get all prodid that have the same value for an attribute."""
    cursor.execute(sql.SQL("SELECT _id FROM {} WHERE " + attribute + "=%s;").
                   format(sql.Identifier('products')), [attribute_value])
    return cursor.fetchall()


def get_current_profile_attributes(cursor, profid):
    """"Get os, devicefamily and devicetype from a profile."""
    cursor.execute(sql.SQL("SELECT os, device_family, device_brand FROM {} WHERE profid=%s;").
                   format(sql.Identifier('sessions')), [profid])
    return cursor.fetchone()


def get_similar_profiles_attribute(cursor, attribute, attribute_value):
    """"Get all profid that share the same attribute."""
    cursor.execute(sql.SQL("SELECT DISTINCT profid FROM {} WHERE " + attribute + "=%s;").
                   format(sql.Identifier('sessions')), [attribute_value])
    return cursor.fetchall()


def insert_recommendations_to_db(connection, cursor, profid, prodid):
    """"insert product_id's on highest point order, linked to profileID"""
    cursor.execute(sql.SQL("INSERT INTO {} (profile_id, product_id) VALUES (%s, %s);").
                   format(sql.Identifier('product_recommendations')), [profid, prodid])
    connection.commit()
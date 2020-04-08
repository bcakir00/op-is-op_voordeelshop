import db
import recommendations


def weighted_score(counters, lst, weight):
    """"Returns a list with prodid that receive a arbitrary amount of points, based on weight."""
    if counters == None:
        counters = {}


    for item in lst:
        if item in counters:
            counters[item] += weight
        else:
            counters[item] = weight

    return counters


def product_based(cursor, prodid):
    """"find similar products based on weighted points of attributes: brand, category, subcategory, subsubcategory"""
    product_attributes = db.get_product_attributes(cursor, prodid)

    brand_product_attribute = product_attributes[0][0]
    category_product_attribute = product_attributes[0][1]
    subcategory_product_attribute = product_attributes[0][2]
    subsubcategory_product_attribute = product_attributes[0][3]

    brand_attribute_list = db.get_similar_products_attribute(cursor, 'brand_id', brand_product_attribute)
    category_attribute_list = db.get_similar_products_attribute(cursor, 'sub_category_id',
                                                               category_product_attribute)
    subcategory_attribute_list = db.get_similar_products_attribute(cursor, 'sub_category_id',
                                                                  subcategory_product_attribute)
    subsubcategory_attribute_list = db.get_similar_products_attribute(cursor, 'sub_sub_category_id',
                                                                     subsubcategory_product_attribute)

    product_counters = weighted_score(None, brand_attribute_list, 3)
    product_counters = weighted_score(product_counters, category_attribute_list, 1)
    product_counters = weighted_score(product_counters, subcategory_attribute_list, 2)
    product_counters = weighted_score(product_counters, subsubcategory_attribute_list, 4)

    return product_counters


def profile_based(cursor, profid):
    """"find similar profiles based on weighted points of attributes: os, device, devicetype"""
    profile_attributes = db.get_current_profile_attributes(cursor, profid)

    os_profile_attribute = profile_attributes[0]
    devicefamily_profile_attribute = profile_attributes[1]
    devicetype_profile_attribute = profile_attributes[2]

    os_attribute_list = db.get_similar_profiles_attribute(cursor, 'os', os_profile_attribute)
    devicefamily_attribute_list = db.get_similar_profiles_attribute(cursor, 'device_family',
                                                                    devicefamily_profile_attribute)
    devicetype_attribute_list = db.get_similar_profiles_attribute(cursor, 'device_brand', devicetype_profile_attribute)

    profile_counters = weighted_score(None, os_attribute_list, 4)
    profile_counters = weighted_score(profile_counters, devicefamily_attribute_list, 2)
    profile_counters = weighted_score(profile_counters, devicetype_attribute_list, 1)

    return profile_counters


def get_top_similar_profiles(lst, amount):
    top_profiles = []

    for x in range(amount):
        tmp_best_profile = ''
        tmp_best_score = 0

        for profid in lst:
            if lst[profid] > tmp_best_score:
                tmp_best_profile = profid
                tmp_best_score = lst[profid]

        lst.pop(profid)
        top_profiles.append(tmp_best_profile)

    return top_profiles


def calculate_weighted_results():
    """"Calculate best product to be recommended by combining view en profile recommendations"""
    pass
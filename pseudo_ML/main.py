#make connection to server

#get data from all profiles if user made sale
#if sale, get recommendations from similar profiles
#else, get recommendations from viewed products.

    #view_based_recommendation
        #-find similar products based on weights of attributes: brand, category, subcategory, subsubcategory

    #profile-similarity_based_recommendation
        #find similar profiles based on weights of viewed-products, os, device, devicetype that has a sale

#insert productItems on highest point order, linked to profileID

#terminate connection to server


import db
import recommendations


connection = db.connect_to_server()
cursor = connection.cursor()
#db.create_recommendation_table(connection, cursor)

profid_list = db.get_all_profid(cursor)

for profid in profid_list:
    print("making recommendations for profile id " + str(profid[0]))

    recommended_list = []
    has_sale = db.get_user_has_sale(cursor, profid)

    if has_sale:
        profile_based_recommended_list = recommendations.profile_based(cursor, profid)
        top_profiles = recommendations.get_top_similar_profiles(profile_based_recommended_list, 5)

        for profid in top_profiles:
            recommended_list += db.get_products_viewed(cursor, profid)
    else:
        prodid_viewed = db.get_products_viewed(cursor, profid)

        for viewed_prodid in prodid_viewed:
           recommended_list = recommendations.product_based(cursor, viewed_prodid)

    for recommendation in recommended_list:
        db.insert_recommendations_to_db(connection, cursor, profid, recommendation)
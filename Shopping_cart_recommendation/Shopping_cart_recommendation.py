import psycopg2
import random

con = psycopg2.connect(database="huwebshop", user="postgres", password="GP2020")
cur = con.cursor()


def getAllProfiles():
    # Gets all profiles
    allprofiles = []

    cur.execute("""SELECT _id FROM profiles""")
    profiles = cur.fetchall()

    for profile in profiles:
        allprofiles.append(profile[0])

    return allprofiles


# Gets list with profiles that have made previous purchases
def getBuyers():
    # Gets profiles where sales is not null
    buyerProfiles = {}
    buyerProfileList = []

    cur.execute("""SELECT profile_id, product_id FROM products_bought""")
    a = cur.fetchall()

    for x in a:
        buyerProfiles[x[0]] = x[1]


    cur.execute("""SELECT profile_id FROM products_bought""")
    b = cur.fetchall()

    for x in b:
        buyerProfileList.append((x[0]))

    return buyerProfileList


# Gives a list with all profiles with no previous purchases
def notBuyers(all, buyers):
    nonBuyer = []
    for x in all:
        if x not in buyers:
            nonBuyer.append(x)
    return nonBuyer


# Gets recommendation for profiles WITH previous purchases
def hasSale():
    profileId = []
    productId = []
    tempList= []
    salesPrice = []
    maxPrice = []
    available = []


    cur.execute("""SELECT profile_id FROM products_bought""")
    a = cur.fetchall()

    for x in a:
        profileId.append(x[0])

    cur.execute("""SELECT product_id FROM products_bought""")
    b = cur.fetchall()

    for y in b:
        productId.append(y[0])

    for z in productId:
        cur.execute(f"""SELECT price FROM products
                      WHERE _id = '{z}'""")
        prod = cur.fetchone()
        tempList.append(prod[0])

    for x in tempList:
        salesPrice.append(str(x))


    # Get max price for recommendations
    for x in salesPrice:
        if x == '1.39' or x == '12.98':
            x = float(x)
            x = x * 100
            x = int(x)
            x = x + 500
        else:
            x = int(x)
            x = x + 500
        maxPrice.append(x)


    """---------------------"""


    prodMPrice = {}

    for x in range(0, len(productId)):
        prodMPrice[productId[x]] = maxPrice[x]

    print(prodMPrice)
    allProductsList = []
    cur.execute("""SELECT _id FROM products
                WHERE category_id='9'
                OR category_id='10'
                OR category_id='12'
                OR category_id='13'
                OR category_id='15'""")
    allproducts = cur.fetchall()

    for y in allproducts:
        allProductsList.append(y[0])

    print(allProductsList)


# Gets recommendations for profiles WITHOUT previous purchases
def noSales(profileId):
    priceList = []
    other = []
    c = []
    four = []

    cur.execute("""SELECT _id FROM products
                WHERE price <= '500'""")
    prices = cur.fetchall()
    for x in prices:
        priceList.append(x[0])

    cur.execute("""SELECT _id FROM products
                WHERE category_id ='9'
                OR category_id='10'
                OR category_id='12'
                OR category_id='13'
                OR category_id='15'""")
    category = cur.fetchall()
    for x in category:
        other.append(x[0])

    for x in range(0, len(priceList)):
        if priceList[x] in other:
            c.append(priceList[x])

    for x in range(0, 4):
        a = random.randint(0, len(c))
        four.append(c[a])

    count = 0
    for x in profileId:
        count += 1
        print(count)
        for y in range(0, 4):
            cur.execute("""INSERT INTO test
                        VALUES(%s, %s, 1)
                        ON CONFLICT DO NOTHING;""", (x, four[y]))

    cur.execute(f"INSERT INTO product_recommendations (profile_id, product_id) SELECT t.test, "
                   f"t.test2 FROM test AS t INNER JOIN products ON t.test2 = products._id")

# getAllProfiles()
# hasSale()
noSales(notBuyers(getAllProfiles(), getBuyers()))

con.commit()
cur.close()
con.close()
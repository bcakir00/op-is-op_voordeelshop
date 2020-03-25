import psycopg2

c = psycopg2.connect("dbname=huwebshop user=postgres password=postgres")  # TODO: edit this.
cur = c.cursor()


# Creating all the tables that have to be normalised.
cur.execute("DROP TABLE IF EXISTS brand CASCADE")
cur.execute("DROP TABLE IF EXISTS category CASCADE")
cur.execute("DROP TABLE IF EXISTS sub_category CASCADE")
cur.execute("DROP TABLE IF EXISTS sub_sub_category CASCADE")
cur.execute("DROP TABLE IF EXISTS color CASCADE")
cur.execute("DROP TABLE IF EXISTS gender CASCADE")

cur.execute("""CREATE TABLE brand(_id INT PRIMARY KEY, brand VARCHAR);""")
cur.execute("""CREATE TABLE category(_id INT PRIMARY KEY, category VARCHAR);""")
cur.execute("""CREATE TABLE sub_category(_id INT PRIMARY KEY, sub_category VARCHAR);""")
cur.execute("""CREATE TABLE sub_sub_category(_id INT PRIMARY KEY, sub_sub_category VARCHAR);""")
cur.execute("""CREATE TABLE color(_id INT PRIMARY KEY, color VARCHAR);""")
cur.execute("""CREATE TABLE gender(_id INT PRIMARY KEY, gender VARCHAR);""")

# Creating the rest of the tables.
cur.execute("DROP TABLE IF EXISTS profiles CASCADE")
cur.execute("DROP TABLE IF EXISTS sessions CASCADE")
cur.execute("DROP TABLE IF EXISTS products CASCADE")
cur.execute("DROP TABLE IF EXISTS viewed_products CASCADE")
cur.execute("DROP TABLE IF EXISTS products_bought CASCADE")
cur.execute("DROP TABLE IF EXISTS previously_recommended CASCADE")

cur.execute("""CREATE TABLE profiles
                (_id VARCHAR PRIMARY KEY, 
                recommendation_segment VARCHAR,
                order_count INT);""")

cur.execute("""CREATE TABLE sessions
                (sessions_id VARCHAR PRIMARY KEY, 
                has_sale BOOLEAN,
                device_family VARCHAR,
                device_brand VARCHAR,
                os VARCHAR,
                profid VARCHAR,
                FOREIGN KEY (profid) REFERENCES profiles (_id));""")

cur.execute("""CREATE TABLE products
                (_id VARCHAR PRIMARY KEY, 
                brand_id INT,
                category_id INT,
                sub_category_id INT,
                sub_sub_category_id INT,
                color_id INT,
                gender_id INT,
                price DECIMAL,
                FOREIGN KEY (brand_id) REFERENCES brand (_id),
                FOREIGN KEY (category_id) REFERENCES category (_id),
                FOREIGN KEY (sub_category_id) REFERENCES sub_category (_id),
                FOREIGN KEY (sub_sub_category_id) REFERENCES sub_sub_category (_id),
                FOREIGN KEY (color_id) REFERENCES color (_id),
                FOREIGN KEY (gender_id) REFERENCES gender (_id));""")

cur.execute("""CREATE TABLE viewed_products
                (_id INT PRIMARY KEY, 
                product_id VARCHAR,
                profile_id VARCHAR,
                FOREIGN KEY (product_id) REFERENCES products (_id),
                FOREIGN KEY (profile_id) REFERENCES profiles (_id));""")

cur.execute("""CREATE TABLE products_bought
                (_id INT PRIMARY KEY, 
                product_id VARCHAR,
                profile_id VARCHAR,
                FOREIGN KEY (product_id) REFERENCES products (_id),
                FOREIGN KEY (profile_id) REFERENCES profiles (_id));""")

cur.execute("""CREATE TABLE previously_recommended
                (_id INT PRIMARY KEY, 
                product_id VARCHAR,
                profile_id VARCHAR,
                FOREIGN KEY (product_id) REFERENCES products (_id),
                FOREIGN KEY (profile_id) REFERENCES profiles (_id));""")

cur.execute("""CREATE TABLE product_recommendations
                (_id INT PRIMARY KEY,
                profile_id VARCHAR,
                product_id VARCHAR,
                FOREIGN KEY (profile_id) REFERENCES profiles (_id),
                FOREIGN KEY (product_id) REFERENCES products (_id));""")

c.commit()
cur.close()
c.close()

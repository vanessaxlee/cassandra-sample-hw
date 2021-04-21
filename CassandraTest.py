from cassandra.cluster import Cluster
import pandas as pd
import random
import time
import datetime
import csv


# generate a random date between given start and end dates
def randomDate(start, end):
    frmt = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


# generate data for items in carts and product logs and export to csv files
def data_generator():
    # create items in carts data with 1000 carts/customers,
    # each with a cart_id, item_id, random timestamp, and quantity of the item
    cart_id = []
    item_id = []
    time = []
    quantity = []

    # for each cart (1 to 1000)
    for i in range(1000):
        items_so_far = []
        # generate a random number of product types in cart
        num_items = random.randint(1, 10)
        for j in range(num_items):
            # generate a random product id for random product to put in cart
            it_id = random.randint(1, 50)
            # generate new product id if already in cart
            while it_id in items_so_far:
                it_id = random.randint(1, 50)

            cart_id.append(i + 1)
            item_id.append(it_id)
            items_so_far.append(it_id)
            time.append(randomDate("01-02-2021 00:00:00", "10-02-2021 00:00:00"))
            # generate a random quantity from 1 to 5 for product
            quantity.append(random.randint(1, 5))

    items_carts = {'cart_id': cart_id, 'item_id': item_id, 'time': time, 'quantity': quantity}
    df = pd.DataFrame(items_carts, columns=['cart_id', 'item_id', 'time', 'quantity'])
    df.to_csv('items_carts.csv', index=False)


    # create product log data, with product id's and prices
    product_id = []
    price = []

    # generate product id's from 1 to 50
    for i in range(50):
        # generate a random price from 1 to 10
        p = random.randint(1, 10)

        product_id.append(i + 1)
        price.append(p)

    products = {'product_id': product_id, 'price': price}
    df = pd.DataFrame(products, columns=['product_id', 'price'])
    df.to_csv('products.csv', index=False)

# create keyspace in Cassandra by connecting to given Cluster session,
# and create tables for items in carts and product log
def create_keyspace_and_tables(session):
    # create and use keyspace named 'shoppingcart'
    session.execute('DROP KEYSPACE IF EXISTS shoppingcart;')
    session.execute(
        'CREATE KEYSPACE shoppingcart WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\':3};')
    session.execute('USE shoppingcart;')

    # create the item_carts table
    session.execute('DROP TABLE IF EXISTS shoppingcart.item_carts;')
    session.execute(
        'CREATE TABLE item_carts(cart_id int, item_id int, time timestamp, quantity int, price int,'
        + 'PRIMARY KEY ((cart_id, item_id)));')
    create_index = "CREATE INDEX find_item ON shoppingcart.item_carts (item_id);"
    session.execute(create_index)
    create_sec_index = "CREATE INDEX find_cart ON shoppingcart.item_carts (cart_id);"
    session.execute(create_sec_index)

    # create the products table
    session.execute('DROP TABLE IF EXISTS shoppingcart.products;')
    session.execute('CREATE TABLE products (product_id int, price int,'
                    + 'PRIMARY KEY (product_id));')

    print("Created!")

# insert data into items in carts table, combining data from product log dataset and items in carts dataset
def insert_item_carts(session,product_data_filepath, cart_data_filepath):
    session.execute('USE shoppingcart;')

    products = pd.read_csv(product_data_filepath, header=0)

    with open(cart_data_filepath, "r") as item_carts:
        next(item_carts)

        now = datetime.datetime.now()
        for carts in item_carts:
            columns = carts.split(",")

            cart_id = columns[0]
            cart_id = int(float(cart_id))

            item_id = columns[1]
            item_id = int(float(item_id))

            time = datetime.datetime.strptime(columns[2], "%Y-%m-%d %H:%M:%S").date()

            quantity = columns[3]
            quantity = int(float(quantity))

            # find price of current product using products.csv data that has been inserted into a dataframe
            price = products[products['product_id'] == item_id]['price'].values[0]

            prepared = session.prepare("""
             INSERT INTO item_carts (cart_id, item_id, time, quantity, price)
             VALUES (?, ?, ?, ?, ?)
             """)

            session.execute(prepared, [cart_id, item_id, time, quantity, price])

    # closing the file
    item_carts.close()

    later = datetime.datetime.now()
    time_insert = later - now
    print("item carts insert speed = ", time_insert)

# insert data into product log table from given filepath to csv file
def insert_products(session,filepath):
    session.execute('USE shoppingcart;')

    with open(filepath, "r") as products:
        next(products)
        now = datetime.datetime.now()
        for product in products:
            columns = product.split(",")

            product_id = columns[0]
            product_id = int(float(product_id))

            price = columns[1]
            price = int(float(price))

            prepared = session.prepare("""
             INSERT INTO products (product_id, price)
             VALUES (?, ?)
             """)

            session.execute(prepared, [product_id, price])

    # closing the file
    products.close()

    later = datetime.datetime.now()
    time_insert = later - now
    print("products insert speed = ", time_insert)

# find number of carts that contains product with given product id after connecting to given Cluster session
def find_num_carts(session, product_id):
    p_id = str(product_id)
    query2 = "SELECT COUNT(*) FROM item_carts WHERE item_id = " + p_id + ";"

    session.execute('USE shoppingcart;')
    now = datetime.datetime.now()
    count = session.execute(query2)
    later = datetime.datetime.now()

    time_insert = later - now
    print("number of carts with product #", product_id, ": ", count.one().count)
    print("find number of carts with given product read speed = ", time_insert)

# find total price for cart with given cart id after connecting to given Cluster session
def find_total(session, cart_id):
    query = "SELECT sum (price) FROM shoppingcart.item_carts where cart_id = " + str(cart_id) + ";"
    session.execute('USE shoppingcart;')

    now = datetime.datetime.now()
    price = session.execute(query)
    later = datetime.datetime.now()

    time_insert = later - now
    print("total for cart ", cart_id, ": ", price.one().system_sum_price)
    print("find total for cart read speed = ", time_insert)


# connect to Cassandra, generate datasets, insert data into tables in Cassandra, and run queries to find
# number of carts with a given product in it and the total price for items in a given cart
def main():
    cluster = Cluster()
    session = cluster.connect()
    # generate random shopping cart data
    data_generator()

    # create keyspace and tables
    create_keyspace_and_tables(session)

    # insert data into item carts table
    insert_item_carts(session, 'products.csv', 'items_carts.csv')

    # insert data into products table
    insert_products(session, 'products.csv')
    print("Finished!")

    # Q1 - Find how many people have item 3 in their carts
    find_num_carts(session, 3)

    # Q2 -  Find total for cart 80
    find_total(session, 80)

    session.shutdown()


main()

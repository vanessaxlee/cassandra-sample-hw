from cassandra.cluster import Cluster
import pandas as pd
import random
import time
import datetime
import csv


def randomDate(start, end):
    frmt = '%d-%m-%Y %H:%M:%S'

    stime = time.mktime(time.strptime(start, frmt))
    etime = time.mktime(time.strptime(end, frmt))

    ptime = stime + random.random() * (etime - stime)
    dt = datetime.datetime.fromtimestamp(time.mktime(time.localtime(ptime)))
    return dt


def data_generator():
    # create items_carts table with 1000 random customers
    cart_id = []
    item_id = []
    time = []
    quantity = []

    for i in range(1000):
        items_so_far = []
        num_items = random.randint(1, 10)
        for j in range(num_items):
            it_id = random.randint(1, 50)
            while it_id in items_so_far:
                it_id = random.randint(1, 50)

            cart_id.append(i + 1)
            item_id.append(it_id)
            items_so_far.append(it_id)
            time.append(randomDate("01-02-2021 00:00:00", "10-02-2021 00:00:00"))
            quantity.append(random.randint(1, 5))

    items_carts = {'cart_id': cart_id, 'item_id': item_id, 'time': time, 'quantity': quantity}
    df = pd.DataFrame(items_carts, columns=['cart_id', 'item_id', 'time', 'quantity'])
    df.to_csv('items_carts.csv', index=False)

    # products_table
    product_id = []
    price = []
    for i in range(50):
        p = random.randint(1, 10)

        product_id.append(i + 1)
        price.append(p)

    products = {'product_id': product_id, 'price': price}
    df = pd.DataFrame(products, columns=['product_id', 'price'])
    df.to_csv('products.csv', index=False)


def create_keyspace_and_tables(session):
    # create and use keyspace named 'shoppingcart'
    session.execute('DROP KEYSPACE IF EXISTS shoppingcart;')
    session.execute(
        'CREATE KEYSPACE IF NOT EXISTS shoppingcart WITH replication = {\'class\':\'SimpleStrategy\', \'replication_factor\':3};')
    session.execute('USE shoppingcart;')

    # create the item_carts table
    session.execute('DROP TABLE IF EXISTS shoppingcart.item_carts;')
    session.execute(
        'CREATE TABLE item_carts(cart_id int, item_id int, time timestamp, quantity int, price int,'
        + 'PRIMARY KEY ((cart_id, item_id)));')

    # create the products table
    session.execute('DROP TABLE IF EXISTS shoppingcart.products;')
    session.execute('CREATE TABLE products (product_id int, price int,'
                    + 'PRIMARY KEY (product_id));')

    print("Created!")


def insert_item_carts(session, product_data_filepath, cart_data_filepath):
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


def insert_products(session, filepath):
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


def find_num_carts(session, product_id):
    p_id = str(product_id)
    #query1 = "CREATE INDEX find_item ON shoppingcart.item_carts (item_id);"
    query2 = "SELECT COUNT(*) FROM item_carts WHERE item_id = " + p_id + ";"

    session.execute('USE shoppingcart;')
    #session.execute(query1)
    count = session.execute(query2)

    print("number of carts with product #", product_id, ": ", count.one().count)


def find_total(session, cart_id):
    # find all the items in this cart
    # find the prices for all items
    # sum them
    query = "SELECT sum (price) FROM shoppingcart.item_carts where cart_id = " + str(cart_id) + ";"
    #query2 = "CREATE INDEX find_cart ON shoppingcart.item_carts (cart_id);"
    session.execute('USE shoppingcart;')
    #session.execute(query2)
    price = session.execute(query)
    print("total for cart ", cart_id, ": ", price.one().system_sum_price)


def main():
    cluster = Cluster()
    session = cluster.connect()
    # generate random shopping cart data
    #data_generator()

    # create keyspace and tables
    #create_keyspace_and_tables(session)

    # insert data into item carts table
    #insert_item_carts(session, 'products.csv', 'items_carts.csv')

    # insert data into products table
    #insert_products(session, 'products.csv')
    print("Finished!")

    # Q1 - Find how many people have a given item in their carts. Find read speed
    find_num_carts(session, 3)

    # Q2 -  Find total for a person’s cart
    find_total(session, 80)

    # modify the data

    session.shutdown()


main()

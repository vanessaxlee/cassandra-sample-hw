DROP DATABASE IF EXISTS cassandra;
CREATE DATABASE cassandra;
USE cassandra;

DROP TABLE IF EXISTS products;
CREATE TABLE IF NOT EXISTS products (
    product_id INT PRIMARY KEY,
    price INT NOT NULL
);

DROP TABLE IF EXISTS items_cart;
CREATE TABLE IF NOT EXISTS items_cart (
    cart_id INT,
	item_id INT NOT NULL,
    time DATETIME NOT NULL,
    quantity INT NOT NULL, CONSTRAINT fk_product FOREIGN KEY (item_id) REFERENCES products(product_id),
    CONSTRAINT pk_items_cart PRIMARY KEY (cart_id, item_id)
);

SELECT COUNT(*) FROM items_cart WHERE item_id = 3;

SELECT SUM(t2.price) FROM (SELECT products.price AS price FROM items_cart
JOIN products ON (items_cart.item_id = products.product_id) WHERE items_cart.cart_id = 80) AS t2;

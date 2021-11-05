# Cassandra Sample HW


## Problem Description
The purpose of this homework assignment is to compare the read and write speeds between SQL and CQL, Cassandra’s querying language. You will be modeling a database for online shopping with two tables: products and carts. There are three components to this assignment. You need to (1) generate datasets that satisfy the requirements given to you below, (2) write insert statements and record the write speeds for SQL and CQL, and (3) write queries that answer the questions below and record the read speeds for SQL and CQL.

## Part 1: Generate Data
Generate two datasets, one representing a product log with all products and their price, one stores all the shopping carts, the products in it and relevant information such as quantity of the product. For example, you datasets may contain the following columns:
- Shopping Carts
  - cart ID (int)
  - item ID (int)
  - timestamp (timestamp)
  - quantity (int)
  - primary key (cart ID, item ID)
- Product Log 
  - item ID (int)
  - price (int)

You may use any language you prefer to generate the datasets. Make sure to document any assumptions you make for the data model here, and in both SQL and Cassandra.

## Part 2: Comparing read and write speeds of mySQL and Cassandra
Queries to support:
	Query 1: Find how many people have the product with item id 3 in their carts.
    Query 2: Find the total price for all items in cart with cart id 80.

### MySQL
1) Insert product log data into a MySQL table. What is the write speed?
2) Insert items in cart data into a MySQL table. What is the write speed?
3) Run Query 1 in mySQL. What is the read speed?
4) Run Query 2 in mySQL. What is the read speed?

### Cassandra
**Make sure to think about what is needed to support queries before modeling/creating tables.**
1) Insert data into a Cassandra table to support Query 1. What is the write speed? What is the read speed?
2) Insert data into a Cassandra table to support Query 2. What is the write speed? What is the read speed?

### Optional Comparison
1) Is the write speed for inserting data generally faster in mySQL or Cassandra?
2) Is the read speed for querying data generally faster in mySQL or Cassandra?


## Resources
Install Python Cassandra driver via terminal ([instructions](https://docs.datastax.com/en/developer/python-driver/3.25/installation/)).

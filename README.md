# Cassandra Sample HW


## Problem Description
The purpose of this homework assignment is to compare the read and write speeds between SQL and CQL, Cassandra’s querying language. You will be modeling a database for online shopping with two tables: products and carts. There are three components to this assignment. You need to (1) generate datasets that satisfy the requirements given to you below, (2) write insert statements and record the write speeds for SQL and CQL, and (3) write queries that answer the questions below and record the read speeds for SQL and CQL.

## Part 1: Generate Data
Generate two datasets, one representing a product log with all products and their price, one stores all the shopping carts, the products in it and relevant information such as quantity of the product. For example, you datasets may contain the following columns:
Shopping Carts
Cart id (int)
Item id (int)
Timestamp (timestamp)
Quantity (int)
Primary key (cart id, item id)
Product Log 
Item id (int)
Price (int)

You may use any language you prefer to generate the datasets. Make sure to document any assumptions you make for the data model here, and in both SQL and Cassandra.

## Part 2: Comparing Read and Write Speeds of MySQL and Cassandra
Queries to support:
* Query 1: Find the total price for all items in cart with cart ID = 80.
* Query 2: Find how many people have the product with item ID = 3 in their carts.

### MySQL
Insert data into a MySQL table to support query.

1. What is the write speed? What is the read speed?

Insert data into a MySQL table to support query. 

2. What is the write speed? What is the read speed?
### Cassandra
Make sure to think about what is needed to support queries before modeling/creating tables.
Insert data into a Cassandra table to support query.

1. What is the write speed? What is the read speed?

Insert data into a Cassandra table to support query.

2. What is the write speed? What is the read speed?
### Comparison
Is the write speed for inserting data generally faster in mySQL or Cassandra?
Is the read speed for querying data generally faster in mySQL or Cassandra?

## Resources
Install Python Cassandra driver via terminal ([instructions](https://docs.datastax.com/en/developer/python-driver/3.25/installation/)).

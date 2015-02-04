# import psycopg2
# conn = psycopg2.connect("dbname=DistributedDatabaseSystem user=postgres")
# # Open a cursor to perform database operations
# cur = conn.cursor()

# # Execute a command: this creates a new table
# #cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

# # Pass data to fill a query placeholders and let Psycopg perform
# # the correct conversion (no more SQL injections!)
# cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
# (100, "abc'def"))

# # Query the database and obtain data as Python objects
# cur.execute("SELECT * FROM test;")
# rows=cur.fetchone()

# print rows[0]
# # Make the changes to the database persistent
# conn.commit()

# # Close communication with the database
# cur.close()
# conn.close()
# print "h"

import Interface

#Load/Create DB
Interface.create_db("udaidistr")

con = Interface.getopenconnection(dbname='udaidistr')
cur = con.cursor()
cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")
cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
(100, "abc'def"))
cur.execute("SELECT * FROM test;")
row= cur.fetchone()[0]
print row

# print Interface.cur

#Connect to the DB
# conn=Interface.getopenconnection('postgres','1234','disdis')

# Open a cursor to perform database operations
# cur = conn.cursor()

#If the table exists
# cur.execute("CREATE TABLE " + Interface.RATINGS_TABLE_NAME+" (UserID integer, MovieID integer, Rating float);")
# cur.execute("SELECT * FROM"+Interface.RATINGS_TABLE_NAME)
# rows=cur.fetchone()
# print rows[0]

# cur.execute("CREATE TABLE test (user_id integer, num integer, data varchar);")


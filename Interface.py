#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE_NAME = 'Ratings'
MAX_RATING = 5


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")








def loadratings(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    #Drop the table if its there
    cur.execute("DROP TABLE IF EXISTS "+RATINGS_TABLE_NAME+";")

    # Create The Ratings Table if it doesnt exists
    cur.execute("CREATE TABLE IF NOT EXISTS "+ RATINGS_TABLE_NAME +" (UserID integer, MovieID integer, Rating float);")

    # Insert the ratings
    with open(ratingsfilepath) as RatingsFile:
        for line in RatingsFile:
            splitArr=line.split("::")
            cur.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(splitArr[0],splitArr[1],splitArr[2]))
    print "Ratings Loaded Successfully. Check Database Now!"
    # rangepartition(ratingstablename, 5, openconnection)
    # rangeinsert(ratingstablename,999,999,3.99, openconnection)
    # roundrobinpartition(ratingstablename, 10, openconnection)
    # roundrobininsert(ratingstablename,9992,999,3.99, openconnection)
    # deletepartitionsandexit(openconnection)








def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    #Delete The tables if they exist:
    cur.execute("DROP TABLE IF EXISTS rptablenholder;")
    for i in range(1,numberofpartitions+1):
        cur.execute("DROP TABLE IF EXISTS rangepartitiontable%s;",[i])


    cur.execute("CREATE TABLE IF NOT EXISTS rptablenholder (N integer);")
    cur.execute("INSERT INTO rptablenholder (N) VALUES (%s)",[numberofpartitions])

    cur.execute("SELECT * FROM Ratings;")
    allResults=cur.fetchall()
    
    for result in allResults:
        currRat = float(5.0/numberofpartitions)
        count=1;
    
        inserted=False
        while currRat<=MAX_RATING:
            if result[2]<=currRat:
                cur.execute("CREATE TABLE IF NOT EXISTS rangepartitiontable%s (UserID integer, MovieID integer, Rating float);",[count])
                cur.execute("INSERT INTO rangepartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(count,result[0],result[1],result[2]))
                inserted=True
                break
            else:
                count+=1
                currRat=currRat + float(5.0/numberofpartitions)
        if not inserted:
            cur.execute("CREATE TABLE IF NOT EXISTS rangepartitiontable%s (UserID integer, MovieID integer, Rating float);",[MAX_RATING])
            cur.execute("INSERT INTO rangepartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(MAX_RATING,result[0],result[1],result[2]))

    print "RangePartition Execution Completed Successfully. Check Database Now!"

   




def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()

    #Delete The tables if they exist:
    cur.execute("DROP TABLE IF EXISTS rrtablestartcountholder;")
    for i in range(1,numberofpartitions+1):
        cur.execute("DROP TABLE IF EXISTS roundrobinpartitiontable%s;",[i])

    cur.execute("SELECT * FROM Ratings;")
    allResults=cur.fetchall()
    
    count=0;
    for result in allResults:
        curCount=(count%numberofpartitions)+1
        cur.execute("CREATE TABLE IF NOT EXISTS roundrobinpartitiontable%s (UserID integer, MovieID integer, Rating float);",[curCount])
        cur.execute("INSERT INTO roundrobinpartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(curCount,result[0],result[1],result[2]))
        count+=1

    cur.execute("CREATE TABLE IF NOT EXISTS rrtablestartcountholder (Last integer, N integer);")
    cur.execute("INSERT INTO rrtablestartcountholder (Last, N) VALUES (%s, %s)",[count, numberofpartitions])
    print "RoundRobin Partition Execution Completed Successfully. Check Database Now!"
            
           






def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT N FROM rrtablestartcountholder")
    n= cur.fetchall()
    numberofpartitions = n[-1][0]

    cur.execute("SELECT Last FROM rrtablestartcountholder")
    last= cur.fetchall()
    count = last[-1][0]

    curCount=(count%numberofpartitions)+1
    cur.execute("CREATE TABLE IF NOT EXISTS roundrobinpartitiontable%s (UserID integer, MovieID integer, Rating float);",[curCount])
    cur.execute("INSERT INTO roundrobinpartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(curCount,userid,itemid,rating))

    cur.execute("CREATE TABLE IF NOT EXISTS rrtablestartcountholder (Last integer, N integer);")
    cur.execute("INSERT INTO rrtablestartcountholder (Last, N) VALUES (%s, %s)",[count, numberofpartitions])
    print "RoundRobin Insertion Execution Completed Successfully. Check Database Now!"






def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT N FROM rptablenholder")
    n= cur.fetchall()
    numberofpartitions = n[-1][0]
    currRat = float(5.0/numberofpartitions)
    count=1;

    inserted=False
    while currRat<=MAX_RATING:
        if rating<=currRat:
            cur.execute("CREATE TABLE IF NOT EXISTS rangepartitiontable%s (UserID integer, MovieID integer, Rating float);",[count])
            cur.execute("INSERT INTO rangepartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(count,userid,itemid,rating))
            inserted=True
            break
        else:
            count+=1
            currRat=currRat + float(5.0/numberofpartitions)
    if not inserted:
        cur.execute("CREATE TABLE IF NOT EXISTS rangepartitiontable%s (UserID integer, MovieID integer, Rating float);",[MAX_RATING])
        cur.execute("INSERT INTO rangepartitiontable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(MAX_RATING,userid,itemid,rating))
    print "Range Insert Complete Successfully. Check Database Now!"







def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'rptablenholder';")
    if int(cur.fetchone()[0]):
        cur.execute("SELECT N FROM rptablenholder")
        n= cur.fetchall()
        numberofpartitions = n[-1][0]
        for i in range(1,numberofpartitions+1):
            cur.execute("DROP TABLE IF EXISTS rangepartitiontable%s;",[i])
        cur.execute("DROP TABLE IF EXISTS rptablenholder;")

    cur.execute("SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = 'rrtablestartcountholder';")
    if int(cur.fetchone()[0]):
        cur.execute("SELECT N FROM rrtablestartcountholder")
        n= cur.fetchall()
        numberofpartitions = n[-1][0]
        for i in range(1,numberofpartitions+1):
            cur.execute("DROP TABLE IF EXISTS roundrobinpartitiontable%s;",[i])
        cur.execute("DROP TABLE IF EXISTS rrtablestartcountholder;")


    print "Partitions Deleted Successfully. Check Database Now!"




def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()



# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to
    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection() as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings(RATINGS_TABLE_NAME, 'test_data.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

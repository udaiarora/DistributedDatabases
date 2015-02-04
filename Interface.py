#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

RATINGS_TABLE_NAME = 'Ratings'
MAX_RATING = 5


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingsfilepath, openconnection):
    cur = openconnection.cursor()
    # Create The Ratings Table if it doesnt exists
    cur.execute("CREATE TABLE IF NOT EXISTS "+ RATINGS_TABLE_NAME +" (UserID integer, MovieID integer, Rating float);")
    # Insert the ratings
    with open(ratingsfilepath) as RatingsFile:
        for line in RatingsFile:
            splitArr=line.split("::")
            cur.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(splitArr[0],splitArr[1],splitArr[2]))
    

    rangepartition(RATINGS_TABLE_NAME, 5, openconnection)
    # cur.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(24, 55, 2.5))
    # cur.execute("SELECT * FROM Ratings;")
    # row= cur.fetchone()[0]
    # print row


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS RangePartitionTableNHolder (N integer);")
    cur.execute("INSERT INTO RangePartitionTableNHolder (N) VALUES (%s)",[numberofpartitions])

    cur.execute("SELECT * FROM Ratings;")
    allResults=cur.fetchall()
    
    for result in allResults:
        currRat = float(5.0/numberofpartitions)
        count=0;
    
        inserted=False
        while currRat<=MAX_RATING:
            if result[2]<=currRat:
                cur.execute("CREATE TABLE IF NOT EXISTS RangePartitionTable%s (UserID integer, MovieID integer, Rating float);",[count])
                cur.execute("INSERT INTO RangePartitionTable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(count,result[0],result[1],result[2]))
                inserted=True
                break
            else:
                count+=1
                currRat=currRat + float(5.0/numberofpartitions)
        if not inserted:
            cur.execute("CREATE TABLE IF NOT EXISTS RangePartitionTable%s (UserID integer, MovieID integer, Rating float);",[MAX_RATING])
            cur.execute("INSERT INTO RangePartitionTable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(MAX_RATING,result[0],result[1],result[2]))

    print "RangePartition Execution Completed"
    rangeinsert(RATINGS_TABLE_NAME,112233,4,0.5,openconnection)

   
def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT N FROM RangePartitionTableNHolder")
    n= cur.fetchall()
    numberofpartitions = n[-1][0]
    currRat = float(5.0/numberofpartitions)
    count=0;

    inserted=False
    while currRat<=MAX_RATING:
        if rating<=currRat:
            cur.execute("CREATE TABLE IF NOT EXISTS RangePartitionTable%s (UserID integer, MovieID integer, Rating float);",[count])
            cur.execute("INSERT INTO RangePartitionTable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(count,userid,itemid,rating))
            inserted=True
            break
        else:
            count+=1
            currRat=currRat + float(5.0/numberofpartitions)
    if not inserted:
        cur.execute("CREATE TABLE IF NOT EXISTS RangePartitionTable%s (UserID integer, MovieID integer, Rating float);",[MAX_RATING])
        cur.execute("INSERT INTO RangePartitionTable%s (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(MAX_RATING,userid,itemid,rating))
    print "Range Insert Complete"



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


if __name__ == '__main__':
    try:
        create_db('dds_assgn1')

        with getopenconnection() as con:
            loadratings('ratings.dat', con)
            
    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2
import threading

DATABASE_NAME = 'dds_assgn1'
RATINGS_TABLE_NAME = 'ratings'
MAX_RATING = 5


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")



def startProg(ratingstablename, ratingsfilepath, openconnection):
    cur = openconnection.cursor()

    #Drop the table if its there
    cur.execute("DROP TABLE IF EXISTS "+RATINGS_TABLE_NAME+";")

    # Create The Ratings Table if it doesnt exists
    cur.execute("CREATE TABLE IF NOT EXISTS "+ RATINGS_TABLE_NAME +" (UserID integer, MovieID integer, Rating float);")

    # Insert the ratings
    # with open(ratingsfilepath) as RatingsFile:
    #     for line in RatingsFile:
    #         splitArr=line.split("::")
    #         cur.execute("INSERT INTO Ratings (UserID, MovieID, Rating) VALUES (%s, %s, %s)",(splitArr[0],splitArr[1],splitArr[2]))
    # print "Ratings Loaded Successfully. Check Database Now!"
    


    # ParallelSort("t1", "c1", "SortedResults", openconnection);
    # ParallelJoin("t1","t2","c1","c2","JoinedResults",openconnection);

# Library Function taken from http://code.activestate.com/recipes/52293/
def fields(cursor):
    '''
    This fuction takes a DB API 2.0 cursor object that has been executed and returns a dictionary of the field names and column numbers.  Field names are the key, column numbers are the value.
    This lets you do a simple cursor_row[field_dict[fieldname]] to get the value of the column.
    Returns dictionary
    '''
    results = {}
    column = 0
    for d in cursor.description:
        results[d[0]] = column
        column = column + 1

    return results      




def rangepartition(tablename, numberofpartitions, SortingColumnName, minval, maxval, opprefix, openconnection):
    cur = openconnection.cursor()

    for i in range(1, numberofpartitions+1):
        cur.execute("DROP TABLE IF EXISTS "+opprefix+"rangepartitiontable%s;",[i])
        cur.execute("CREATE TABLE "+opprefix+"rangepartitiontable%s AS SELECT * FROM "+tablename+" where 1=0",[i])

    cur.execute("SELECT * FROM "+tablename+";")
    allResults=cur.fetchall();

    incr=(maxval-minval)/numberofpartitions
    ind= fields(cur)[SortingColumnName]
    
    for result in allResults:
        currRat = minval + incr
        count=1
        # inserted=False
        while currRat<=maxval:
            if result[ind]<=currRat:
                cur.execute("INSERT INTO "+opprefix+"rangepartitiontable%s VALUES %s",(count,result))
                # inserted=True
                break
            else:
                count+=1
                currRat=currRat + incr
            # if not inserted:
            #     cur.execute("INSERT INTO rangepartitiontable%s VALUES %s",(numberofpartitions,result))

    print "RangePartition Execution Completed Successfully."






l=threading.Lock()
        
def parsort(i, Table, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM rangepartitiontable%s ORDER BY "+SortingColumnName,[i])
    data = cur.fetchall()
    
    l.acquire()
    for row in data:
        # print row[0],row[1],row[2]
        cur.execute("INSERT INTO "+OutputTable+"  VALUES %s",[row])
        openconnection.commit()

    l.release()

    


def ParallelSort(Table, SortingColumnName, OutputTable, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS tempiptable")
    cur.execute("CREATE TABLE tempiptable AS SELECT * FROM "+Table+" where TRUE")
    cur.execute("ALTER TABLE tempiptable ADD COLUMN TupleOrder BIGSERIAL")

    cur.execute("DROP TABLE IF EXISTS "+OutputTable)
    cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM tempiptable where 1=0")

    cur.execute("SELECT MAX("+SortingColumnName+") FROM tempiptable")
    maxval=cur.fetchall()[0][0];
    cur.execute("SELECT MIN("+SortingColumnName+") FROM tempiptable")
    minval=cur.fetchall()[0][0];

    rangepartition("tempiptable", 5, SortingColumnName, minval, maxval, "", openconnection)

    threads = []
    for i in range(1, 6):
        t = threading.Thread(target=parsort, args=(i, Table, SortingColumnName, OutputTable, openconnection,))
        threads.append(t)
        t.start()
    print "Parallel Sort Done. Check SortedReults Table for results"




l2=threading.Lock()
        
def parjoin(i, pre1, pre2, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

    cur = openconnection.cursor()
    cur.execute("SELECT * from "+pre1+"rangepartitiontable%s JOIN "+pre2+"rangepartitiontable%s ON "+pre1+"rangepartitiontable%s."+Table1JoinColumn+" = "+pre2+"rangepartitiontable%s."+Table2JoinColumn+"",[i,i,i,i])
    data = cur.fetchall()

    l2.acquire()
    for row in data:
        # print row[0],row[1],row[2],row[3]
        cur.execute("INSERT INTO "+OutputTable+"  VALUES %s",[row])
        openconnection.commit()

    l2.release()




def ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cur = openconnection.cursor()

    cur.execute("DROP TABLE IF EXISTS "+OutputTable)
    cur.execute("CREATE TABLE "+OutputTable+" AS SELECT * FROM "+InputTable1+" JOIN "+InputTable2+" ON 1=0")


    cur.execute("SELECT MAX("+Table1JoinColumn+") FROM "+InputTable1+"")
    maxval1=cur.fetchall()[0][0];
    cur.execute("SELECT MIN("+Table1JoinColumn+") FROM "+InputTable1+"")
    minval1=cur.fetchall()[0][0];

    cur.execute("SELECT MAX("+Table2JoinColumn+") FROM "+InputTable2+"")
    maxval2=cur.fetchall()[0][0];
    cur.execute("SELECT MIN("+Table2JoinColumn+") FROM "+InputTable2+"")
    minval2=cur.fetchall()[0][0];

    minval=min(minval1,minval2);
    maxval=max(maxval1,maxval2);

    rangepartition(InputTable1, 5, Table1JoinColumn, minval, maxval, "one", openconnection)
    rangepartition(InputTable2, 5, Table2JoinColumn, minval, maxval, "two", openconnection)

    threads = []
    for i in range(1, 6):
        t = threading.Thread(target=parjoin, args=(i, "one", "two", InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection,))
        threads.append(t)
        t.start()

    print "Parallel Join Done. Check JoinedReults Table for results"
    








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
            startProg(RATINGS_TABLE_NAME, 'test_data.dat', con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail

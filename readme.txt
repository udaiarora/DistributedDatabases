All the tests cases are passing from the test file. Here are the prefixes fro the partition tables

RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'rangepartitiontable'
RROBIN_TABLE_PREFIX = 'roundrobinpartitiontable'

Note that postfix of the partition table names start from 1. So that means if you give no. of partition = 5, the range partition tables that will be created are:
rangepartitiontable1
rangepartitiontable2
rangepartitiontable3
rangepartitiontable4
rangepartitiontable5



The Auxilary table names are:
rptablenholder
rrtablestartcountholder
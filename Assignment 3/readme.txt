/*===========================================
=            Running Insructions            =
===========================================*/

The program does 2 operations:
ParallelSort(Table, SortingColumnName, OutputTable, openconnection)
ParallelJoin(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):

Uncomment Line 37 and Line 38 and edit the arguments to match the input table. 

For example, if the input table name for me is t1 and c1 is the column on which sorting has to be performed and the outputa table is called SortedResults, then the following command will be used on line 37:
ParallelSort("t1", "c1", "SortedResults", openconnection);

Similarly, in the command on line 38 (paralleljoin), t1 is table 1, t2 is table 2, c1 is column in table 1 and c2 is column in table 2 on which the join has to be performed. JoinedResults is the output table:
ParallelJoin("t1","t2","c1","c2","JoinedResults",openconnection)


To run the Interface.py file, type the following command in command line:
python Interface.py



/*==================================
=          Algorithms Used         =
==================================*/

For parallel sort, first an empty output table is created. The minimum and maximum values in the table's sorting column are then founded and passed as an argument to the function which RANGEPARTITIONS the table into 5 parts. After dynamically range partitioning the input, 5 threads are spawned. Each of these threads calls a subroutine parsort, which sorts the individual partition and puts the result into the output table. Threads are used to lock the writing of the output so that it's ensured that thread 1 writes before thread 2 and so on. This in turns ensures that the results in the final output are sorted as a whole too.

The parallel join algorithm is based on the same lines. We begin by calling the range parition function on both the input tables. In this algorithm however, the min/max values used for partitioning are not just for that table, but across both tables. After the partitioning, 5 threads are spawned which joins the individual partition into the output table in the parjoin subroutine. The locking mechanism used here is similar to parallel sort.
# concurrencyControl
CS 609 Databases Project: implementing concurrency control tactics including MVCC and 2PL 

Multiversion Concurrency Control (MVCC) handles the schedule of concurrent operations on the same data items by creating multiple versions of the data. Timestamps on each transaction and version help determine the appropriate version to use for each operation.

Strong Strict Two Phase Locking with Wound Wait (2PL) handles the schedule by granting locks to individual transactions for data items. The two types of locks are read locks, shared between all transactions, and write locks, exclusive to the locking transaction. The strong strict specification means transactions hold any aquired locks until the transaction commits or aborts. Wound Wait is the method for handling lock conflicts using transaction timestamps: if the requesting transaction is younger than the transaction holding the lock, it is blocked and waits for the lock, but if it is older it aborts and restarts.

The outputed schedules include starts S, read operations R, write operations W, aborts A, commits C, and blocks B (only in 2PL). The number following the operation type is the transaction number. For R and W operations, in 2PL the letter in parentheses following the transaction number is the data item, and in MVCC the parentheses contain the data item identifier, followed by the version number. For MVCC writes, when a new version is created the version it is based on is also listed. The final summary includes the number of restarts for MVCC and 2PL, as well as the number of versions created for MVCC.

Example schedules to test (must be written individually in a file called schedule.txt stored in the same folder as the program):
S1;R1(Y);S2;R2(X);W2(Y);R1(X);W1(X);;

S1;R1(X);W1(X);S2;R2(X);S3;R3(Y);W3(X);W3(Y);W2(X);S4;W4(X);;

S2;R2(X);W2(X);S1;R1(X);S3;R3(Y);W3(Y);W1(X);R3(Y);R3(X);W2(X);W2(Y);;

S1;R1(X);S4;R4(X);S2;R2(Y);R2(X);W4(X);W2(Y);;

S1;R1(X);S2;R2(X);W1(X);W2(X);;

S1;W1(X);S2;R2(X);S3;W3(X);W2(X);S4;W4(X);;

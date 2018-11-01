Oracle FS (aka orcl) Hadoop Plugin!
=====================

## Why ? ## 

There are cases where installing an HDFS cluster (or even NFS) is beyond the scope, needs capacity or reach of a project. Still you need a DFS to run those (small?) spark jobs you might have.

Is this that crazy? Apparently not, Oracle has this thing called  the "Oracle Database File System" :-) 

Quoting:
"
The Oracle Database has been commonly used to store files closely associated with database applications including CAD, medical images, invoice images, documents, etc. The SQL standard data type, BLOB (and CLOB) is used by applications to store files in the database. The Oracle Database provides much better security, availability, robustness, transactions, and scalability than traditional file systems. When files are stored in the database, they are backed up, synchronized to the disaster recovery site using Data Guard, and recovered along with the relational data in the database. This has made storing files in the database an appealing option for many applications.

In Oracle Database 11g, Oracle introduced Oracle SecureFiles LOBs. SecureFiles LOBs provide high performance storage for files, comparable to the performance of traditional file systems. SecureFiles LOBs support advanced features of compression, deduplication and encryption to files. Because SecureFiles LOBs maintain backward compatibility to BLOB (and CLOB), applications written against BLOBs continue to transparantly work against SecureFiles LOBs, even with the previously mentioned features.

Database File System (DBFS) leverages the features of the database to store files, and the strengths of the database in efficiently managing relational data, to implement a standard file system interface for files stored in the database. With this interface, storing files in the database is no longer limited to programs specifically written to use BLOB and CLOB programmatic interfaces. Files in the database can now be transparently accessed using any operating system (OS) program that acts on files. For example, ETL (Extract, Transform and Load) tools can transparently store staging files in the database.
"

No, Orclfs is not using DBFS, it is much simpler, customizable and targeted only to hadoop.

## Quality/Feature support ##
The code is kept simple, the implementation is using the hadoop contracts for FileSystem testing.
Apart from concatenation (and permisions which are stored but not checked) everything else should be supported.
The coverage is ~90%, missing only exception branches (ie when the db is down..)

## How to build ##
If you don't already have an oracle db then maybe you will not need this module. If you still want to build and test it, here is how, using docker and Oracle XE.

 -- install docker
 -- run `systemctl start docker`
 -- run `docker pull sath89/oracle-12c` (or any other xe image)
 -- run `docker run -d -p 8080:8080 -p 1521:1521 sath89/oracle-12c`
 -- keep an not of the id and
 -- run `docker logs -f <id>`
 
The specific image will start a db and initialize it. 

Next start sqldeveloper and as sys run the following:
 `CREATE USER "DFS" IDENTIFIED BY "DFS";`

 `GRANT "CONNECT" TO "DFS" ;`
 
 `GRANT "RESOURCE" TO "DFS" ;`
 
 `GRANT "AUTHENTICATEDUSER" TO "DFS" ;`

 `GRANT CREATE ANY VIEW TO "DFS" ;`


 `ALTER USER "DFS"`
 
 ` DEFAULT TABLESPACE "USERS"`
 
 ` TEMPORARY TABLESPACE "TEMP" `
 
 ` ACCOUNT UNLOCK ;`

 `ALTER USER "DFS" QUOTA UNLIMITED ON "USERS";`
 
 You are ready to connect as user DFS and run the create_tables.sql
 

 To build, you will need to install localy (download from oracle and copy to your local m2) the following artifacts:
        com.oracle:ojdbc8:12.1.0.2
        com.oracle:ucp:12.1.0.2
        
 The same artifacts (together with one you will build) need to be available in your hadoop classpath


## Using ##
 See the unit tests for examples and inspiration


## Publishing , deployment , and continuous integration ##

Not planned. Orclfs depends on oracle artifacts which are not freely available... And in any case, you will most certainly want to modify how you connect to the database (via wallets? ssl?)
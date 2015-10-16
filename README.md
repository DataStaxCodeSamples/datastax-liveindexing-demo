Live Indexing Demo
====================

Please note this is for DSE Search implementations 

To create the a single node cluster with replication factor of 1 for standard localhost setup, run the following

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaSetup"
    
Once the schema has been created we need to create the Solr cores that are needed to use DSE Search.

The commands that need to be run are in the src/main/resources/commands.txt file.   

This test will insert a number of transactions (100 by default) into Cassandra and then immediately try to access the last one inserted using the solr query. This will give an indication of the time needed to index the transactions. To change the no of transactions use the command line argument -DnoOfTransactions

To run the test

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.creditcard.Main"

To remove the tables and the schema, run the following.

    mvn clean compile exec:java -Dexec.mainClass="com.datastax.demo.SchemaTeardown"
    
    
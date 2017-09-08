# Reading SMF Data with Apache Spark and MDS 

This spark application allows a system programmer to quickly search through
large numbers of SMF30 records, grouping them by either Job Name or by RACF 
User ID. This was used as part of a finger pointing use case for a runaway
job. 

## How to run
Compile the code with Maven and pass it to Spark using the spark-submit command.
Pass Spark the MDS JDBC driver using the --jars command line option. Certain 
Configurations may require you to pass the driver in using the --driver-class-path
command line option as well. 


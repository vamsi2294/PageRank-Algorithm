Instructions:
-MapReduce jobs were executed in Cloudera. 
All the input files are moved from local file system to the input folder in Hadoop file system.
Command: Hadoop fs -put /<local file path>/ /<input HDFS FilePath>/

GraphGenerator: -To run it must be provided with two arguments:input path and output path.
Command: Hadoop jar <Jar name> GraphGenerator /<file path>/input /<filepath>/output

Files:
-Output folders are deleted for the graph generator and pagerank mapReduce tasks.

To Rerun the program:
-command: hadoop fs -rm -r <filepath>/output/sort
	

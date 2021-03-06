# HDFS
## By Jinwei Xiong and Praveen Sakthivel 

# Motivation
In many environments, a lot of data needs to be readily available (fast access) and resilient to disk and system
failures. Keeping in mind the large amount of data that needs to be stored (petabytes and exabytes):
1. No one disk can store all of it.
2. Keeping a complete file on one disk may bottleneck access speeds.
3. The expected number of disk failures per day in a large datacenter is not negligible, even with SSDs.
To cater to all these needs, HDFS was created. Its design is based on the Google File System (GFS), which was
initially created to store data for Google search but found use in non-production environments as well. HDFS is
used for big data storage throughout the world, notably by Facebook and Twitter. It:
1. Divides all its files into small chunks.
2. Stores $ReplicationFactor number of copies of each file chunk on different DataNodes.
3. Keeps all metadata (information about the files) in a fail-safe and highly available (low downtime) master node
called the NameNode.

## Required Dependencies
- Program uses the Protobuf-3.11.4 Library (Already included in package)
- com.google.code.gson:gson:2.8.6

## Server Configuration
- 2 Configuration files (nn_config.txt and dn_config.txt) are located in the src directory
- The nn_config file must be configured for all classes
- The dn_config file only must be configured for datanode classes
- Config files are already prepopulated. Modify as desired
- DataNode names must be unique

#### NameNode Config Layout
```
servername:[SERVERNAME]
IP:[IP ADDRESS]
Port:[PORT NUMBER]
MAX_NUM_FD:[MAX NUMBER OF FILE DESCRIPTORS]
MAX_NUM_BLOCKS:[MAX NUMBER OF BLOCKS]
HEARTBEATTIME(ms):[TIME FOR HEARTBEAT IN MILLISECONDS]
BLOCKSIZE:[SIZE OF DATA BLOCKS IN BYTES]
```

#### DataNode Config Layout
```
servername:[SERVERNAME]
IP:[IP ADDRESS]
Port:[PORT NUMBER]
HEARTBEATTIME(ms):[TIME FOR HEARTBEAT IN MILLISECONDS]
```
## How to Build:
To Build Everything
```
make
```
To Build Only One Class
```
make [namenode/datanode/client]
```
To Clean
```
make clean
```

## How to Run
All commands must be executed inside the main folder (or paths should be updated to reflect execution location)

To run the NameNode
```
java -classpath ./bin/:./com/google/protobuf-java-3.11.4.jar ds.hdfs.NameNode
```

To run the DataNode
```
java -classpath ./bin/:./com/google/protobuf-java-3.11.4.jar ds.hdfs.DataNode
```

To run the client
```
java -classpath ./bin/:./com/google/protobuf-java-3.11.4.jar ds.hdfs.Client
```
# References
1. HDFS: https://storageconference.us/2010/Papers/MSST/Shvachko.pdf
2. HDFS Architecture Guide: https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html
3. The Google File System: https://static.googleusercontent.com/media/research.google.com/en//archive/gfssosp2003.pdf

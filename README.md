# HDFS
## By Praveen Sakthivel and Jinwei Xiong

## Required Dependencies
- Program uses the Protobuf-3.11.4 Library (Already included in package)

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


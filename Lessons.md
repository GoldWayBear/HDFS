# HDFS
## By Praveen Sakthivel and Jinwei Xiong

## Division of the Work:
Class| Author
------------ | -------------
NameNode| Jinwei
DataNode| Praveen
Client| Praveen
Makefile| Jinwei
README | Praveen
Lessons | Both
Debugging | Both

## Design Overview

#### NameNode

#### DataNode
The DataNode design revolved around the idea that all blocks would be stored as individual files with the blocknumber.

##### On Startup
On Startup the DataNode looks for a file called ChunksRecord in the bin directory. This file is just a a serialized version of the BlockReport Message. If the DataNode has been run before, this file will contain the latest BlockReport sent. It will read all the blocks stored in the report and add it to the integer arraylist storedchunks (A list of all blocks stored on the node). If the file is not present, then the datanode is new. A blank ChunkRecords is created and an empty arraylist storedchunks is created. Afterwards two threads are spinned off that repeatedly execute the Heartbeat and BlockReport functions at a specified time interval.

##### WriteBlock
The Write function takes in a protobuf Message WriteBlockRequest. This just contains the block number and the data of the block in bytes. The write function opens a file in the block directory with a name as the block number and writes the data to it. It assumes that it is receiving a block of proper size. Afterwards it acquires a write lock and writes the block number to an integer arraylist stored chunks (A list of all blocks stored on the node). If no exceptions are thrown, a response message with status 1 is returned, otherwise a message with a status of -1 is returned

##### ReadBlock
The read function taken in a protobuf Message ReadBlockRequest. This message just contains the blocknumber. The function than opens a file with the name as the blocknumber in the block directory and reads that data. Again it assumes the block written was of proper size and reads all the data. A response message is returned containing a byte array of the data and a status code of 1. If an exception was thrown the message will only contain a status code of -1.

##### BlockReport
This function creates the protobuf Blockreport message. This just contains the DataNodes server information (Name,IP,Port) and the list of all blocks on the machine. A read lock is acquired for the storedchunks arraylist. Once the lock is acquired the arraylist is written into the messsage as an array of ints. After the message is serialized, it is first sent to the NameNode and then it is saved to disk as file ChunksRecord to serve as a persistent record of the blocks stored on the DataNode. If the NameNode returns a message with status code of -1, the function continues to resend the message until a status code of 1 is returned.

##### Heartbeat
This function creates and sends a protobuf Heartbeat message. This just contains the DataNodes server information (Name,IP,Port). If the NameNode returns a message with status code of -1, the function continues to resend the message until a status code of 1 is returned.

#### Client

##### DNStub
The DN stub was modified to include a timeout. This to prevent the client from hanging on the rare case where the the client requests DataNode information from the NameNode right as or very soon after the DataNode crashes. In this case not enough time would have occured for the NameNode to remove the dead DataNode and it would have returned the DataNodes info. Without the timeout, the client would hang attempting to establish a connection to the DataNode

##### Put
The Put function creates and OpenFileMessage and sets the filename and the type as Write Only. It sends this to the NameNodes openFile Function. It receives back a file descriptor for the file. The function then opens the file locally. On startup the client would have read the blocksize from the NameNode config and has it stored as variable block_size. It creates a byte array of block_size to serve as buffer. It then loops and reads block_size chunks of the file one at a time. For each block read, the function calls the NameNode assignBlock function, inputting its filedescriptor in a protobuf message as a parameter. It gets back a unique blocknumber assigned for this file and the corrresponding datanodes for this file. It then loops through the all the DataNodes, connecting to them and writing the read block by calling the writeBlock function with the read bytes and the blocknumber as parameters. This ends the loop. If any exceptions are thrown the function exits and an errror message is writtnen to the client

##### Get
The Put function creates and OpenFileMessage and sets the filename and the type as Read Only. It sends this to the NameNodes openFile Function. It receives back a file descriptor for the file and an array of integers containing all block numbers for this file. The array is in order of how the blocks comprise the file. The function loops for each blocknumber and calls the NameNodes getBlockLocation function inputting the block number. It then gets a list of all active DataNodes containing the block. If this list is zero, this file unreadable because all servers containing a portion of this file are down. This is then written to the client. If there are datanodes, the function opens the file locally and  loops for each DataNode and attempts to read the block from the DataNode by calling the readBlock function with the blocknumber as a serialized parameter. If the read is not successful, the function continues onto the next datanode. If it cannot be successfully read from any datanode an error message is written and the function returns. If the reads are successful, that block is then written to the local file locally and the function moves on the next block in the loop.

##### List
This function is very simple. It calls the NameNodes List function and sends an empty protobuf message as parameter. It gets back a list of all the files stored on HDFS. If then pretty prints this to the user. If there was an error somewhere in the process, an error message is written and the function returns.



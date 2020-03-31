all: compile

compile:
	javac -d bin -classpath ".:./bin/protobuf-java-3.11.4.jar" ./src/ds/hdfs/*.java
	@echo "All java files compiled, HDFS can run now!"

namenode:
	@echo "Name Node running..."
	java -classpath ".:./bin/protobuf-java-3.11.4.jar:./bin" ds.hdfs.NameNode

datanode:
	@echo "Data Node running..."
	java -classpath ".:./bin/protobuf-java-3.11.4.jar:./bin" ds.hdfs.DataNode

datanode1:
	@echo "Data Node1 running..."
	java -classpath ".:./bin/protobuf-java-3.11.4.jar:./bin" ds.hdfs.DataNode1

client:
	@echo "Data Node running..."
	java -classpath ".:./bin/protobuf-java-3.11.4.jar:./bin" ds.hdfs.Client

clean:
	@echo "Clean all files and directories ..."
	rm -rf ./bin/get ./bin/blocks ./bin/blocks1 ./bin/ds
	rm -f ./bin/nn_files_proto ./bin/dn_blocks_proto ./bin/dn_blocks_proto1

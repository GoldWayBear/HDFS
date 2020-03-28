//Written By Shaleen Garg
package ds.hdfs;
import java.rmi.registry.Registry;
import java.rmi.registry.LocateRegistry;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.*;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.io.FileInputStream;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.io.File;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.*;
import java.nio.charset.Charset;

import ds.hdfs.IDataNode.*;
import ds.hdfs.HdfsProto.*;

public class DataNode implements IDataNode
{

    protected INameNode NNStub;
    protected String MyIP;
    protected int MyPort;
    protected String MyName;
    protected int heartbeattime;


    //Disk Saved version of StoredChunks
    private String ChunksRecord = "ChunksRecord";
    //List of all blocks stored on the machine
    private List<BlockReportRequest.Block> StoredChunks;
    //Read Write Lock for the arraylist of stored chunks in memory
    private ReentrantReadWriteLock rrwl;
    //Lock for the file containing stored chunks in persistent storage (Only used for writes)
    private ReentrantReadWriteLock filelock;

    private static String NN_ConfigFile = "./src/nn_config.txt";
    private static String DN_ConfigFile = "./src/dn_config.txt";

    public DataNode()
    {
        try {
            File chunkrecords = new File(ChunksRecord);
            StoredChunks = new ArrayList<BlockReportRequest.Block>();
            rrwl = new ReentrantReadWriteLock(true);
            filelock = new ReentrantReadWriteLock(true);
            //Check if there are already blocks saved on this machine
            if (chunkrecords.isFile() && chunkrecords.canRead()) {
                byte[] blockbytes = Files.readAllBytes(Paths.get(ChunksRecord));
                if(blockbytes.length == 0){
                    return;
                }
                BlockReportRequest blocks = BlockReportRequest.parseFrom(blockbytes);
                StoredChunks.addAll(blocks.getBlockList());
                BlockReport();
            }
            else {
                //First time DataNode is booting up on this machine, create blank ChunkFile
                chunkrecords.createNewFile();
            }
        }
        catch(Exception e){
            System.err.println("Error at " + this.getClass() + e.toString());
            e.printStackTrace();
        }

        //Constructor
    }

    public byte[] readBlock(byte[] Inp)
    {
        //Set up response and request objects
        ReadBlockRequest request = null;
        ReadBlockResponse.Builder response = ReadBlockResponse.newBuilder();
        try
        {
            //Parse request and retrieve block number
            request = ReadBlockRequest.parseFrom(Inp);
            int blocknum = request.getBlocknumber();
            //Retrieve data and convert to bytestring to package in response object
            byte[] block = Files.readAllBytes(Paths.get("/blocks/"+blocknum));
            response.setData(ByteString.copyFrom(block));
            response.setStatus(1);
        }
        catch(Exception e)
        {
            System.out.println("Error at readBlock");
            response.setStatus(-1);
        }

        return response.build().toByteArray();
    }

    public byte[] writeBlock(byte[] Inp)
    {
        //Set up response and request objects
        WriteBlockRequest request = null;
        WriteBlockResponse.Builder response = WriteBlockResponse.newBuilder();
        try
        {
            //Parse request and retrieve block number and data
            request = WriteBlockRequest.parseFrom(Inp);
            int blocknum = request.getBlocknumber();
            byte[] block = request.getData().toByteArray();
            File out = new File("/blocks/"+blocknum);
            OutputStream os = new FileOutputStream(out);
            os.write(block);
            os.close();
            //Acquire lock and Add Block Number to list of blocks currently stored on this node
            rrwl.writeLock().lock();
            BlockReportRequest.Block.Builder newblock = BlockReportRequest.Block.newBuilder();
            newblock.setBlocknumber(blocknum);
            StoredChunks.add(newblock.build());
            response.setStatus(1);

        }
        catch(Exception e)
        {
            System.out.println("Error at writeBlock ");
            response.setStatus(-1);
        }
        finally{
            rrwl.writeLock().unlock();
        }

        return response.build().toByteArray();
    }

    //Sends Block Report to NameNode and writes current blocklist to persistent storage
    public void BlockReport()
    {
        int status = -1;
        //Continue sending requests until the NameNode returns an acknowledgment(positive status code)
        while(status < 0) {
            BlockReportRequest.Builder request = BlockReportRequest.newBuilder();
            //Acquire lock to add chunkslist to NameNode message
            rrwl.readLock().lock();
            request.addAllBlock(StoredChunks);
            rrwl.readLock().unlock();
            try {
                //Write message to file and send out to NameNode
                byte[] data = request.build().toByteArray();
                byte[] out = NNStub.blockReport(data);
                BlockReportResponse response = BlockReportResponse.parseFrom(out);
                status = response.getStatus();
                File f = new File(ChunksRecord);
                filelock.writeLock().lock();
                OutputStream os = new FileOutputStream(f,false);
                os.write(data);
                os.close();
            }
            catch(Exception e){
                e.printStackTrace();
            }
            finally{
                filelock.writeLock().unlock();
            }
        }
    }

    public void BindServer(String Name, String IP, int Port)
    {
        try
        {
            IDataNode stub = (IDataNode) UnicastRemoteObject.exportObject(this, 0);
            System.setProperty("java.rmi.server.hostname", IP);
            Registry registry = LocateRegistry.getRegistry(Port);
            registry.rebind(Name, stub);
            System.out.println("\nDataNode connected to RMIregistry\n");
        }catch(Exception e){
            System.err.println("Server Exception: " + e.toString());
            e.printStackTrace();
        }
    }

    public INameNode GetNNStub(String Name, String IP, int Port)
    {
        while(true)
        {
            try
            {
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                INameNode stub = (INameNode) registry.lookup(Name);
                System.out.println("NameNode Found!");
                return stub;
            }catch(Exception e){
                System.out.println("NameNode still not Found");
                continue;
            }
        }
    }

    private void heartbeat(){
        try {
                int status = -1;
                //Continiue sending Heartbeat until the NameNode returns acknowledgment(positive status code)
                while(status < 0){
                    HeartBeatRequest.Builder request = HeartBeatRequest.newBuilder();
                    DataNodeInfo.Builder dn = DataNodeInfo.newBuilder();
                    dn.setServername(MyName);
                    dn.setIpaddr(MyIP);
                    dn.setPortnum(MyPort);
                    request.setDatanode(dn.build());
                    byte[] out = NNStub.heartBeat(request.build().toByteArray());
                    HeartBeatResponse response = HeartBeatResponse.parseFrom(out);
                    status = response.getStatus();
                }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws  IOException
    {
        DataNode Me = new DataNode();
        //Collect configuration for this DataNode
        BufferedReader in = null;
        in = new BufferedReader(new FileReader(DN_ConfigFile));
        String line = null;
        ArrayList<String> config = new ArrayList<String>();
        while((line = in.readLine()) != null){
            config.add(line);
        }
        Me.MyName = config.get(0).split(":")[1];
        Me.MyIP = config.get(1).split(":")[1];
        Me.MyPort = Integer.parseInt(config.get(2).split(":")[1]);
        Me.heartbeattime = Integer.parseInt(config.get(3).split(":")[1]);
        //Collect the configuration for the NameNode
        in = new BufferedReader(new FileReader(NN_ConfigFile));
        line = null;
        config = new ArrayList<String>();
        while((line = in.readLine()) != null){
            config.add(line);
        }
        in.close();
        String name = config.get(0).split(":")[1];
        String ip = config.get(1).split(":")[1];
        int port = Integer.parseInt(config.get(2).split(":")[1]);
        //Create NameNode Stub and Register server with RMI
        Me.NNStub = Me.GetNNStub(name,ip,port);
        Me.BindServer(Me.MyName,Me.MyIP,Me.MyPort);
        //Spin off two threads that loop sending Block Reports and Heartbeats
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    Me.heartbeat();
                    //Sleep 5 seconds before sending heartbeat again
                    try{ Thread.sleep(Me.heartbeattime);}
                    catch(Exception e){e.printStackTrace();}
                }
            }
        });
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    Me.BlockReport();
                    //Sleep 5 seconds before sending BlockReport again
                    try{ Thread.sleep(Me.heartbeattime);}
                    catch(Exception e){e.printStackTrace();}
                }
            }
        });
    }
}

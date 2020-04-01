package ds.hdfs;
import java.net.UnknownHostException;
import java.rmi.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.Registry;
import java.rmi.RemoteException;
import java.util.*;
import java.io.*;
import ds.hdfs.HdfsProto.*;
import com.google.protobuf.ByteString;
//import ds.hdfs.INameNode;

public class Client
{
    //Variables Required
    public INameNode NNStub; //Name Node stub
    public IDataNode DNStub; //Data Node stub

    private static String NN_ConfigFile = "./src/nn_config.txt";
    private int block_size; //size of block
    private int heartbeat;

    public Client()
    {
        //Get the Name Node Stub
        //nn_details contain NN details in the format Server;IP;Port
    }

    public IDataNode GetDNStub(String Name, String IP, int Port) throws Exception {
        //get time at start
        long time = System.currentTimeMillis();
        while(true)
        {
            try{
                Registry registry = LocateRegistry.getRegistry(IP, Port);
                IDataNode stub = (IDataNode) registry.lookup(Name);
                return stub;
            }catch(Exception e){
                //If DataNode has not connected in the duration of one heartbeat,
                // it is possible the node went down right when we requested its info
                //So the namenode has not yet removed it from use. This will time us out from connecting to it
                if ((System.currentTimeMillis() - time) >= heartbeat){
                    throw new Exception("Connection timed out");
                }
                continue;
            }
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
                return stub;
            }catch(Exception e){
                continue;
            }
        }
    }

    public void PutFile(String Filename) //Put File
    {
        System.out.println("Going to put file " + Filename);
        BufferedInputStream bis;
        byte[] block = new byte[block_size];
        try{
            //Open file in NameNode and retrieve File Descriptor
            OpenFileRequest.Builder request = OpenFileRequest.newBuilder();
            request.setFilename(Filename);
            request.setFlag(OpenFileRequest.Flag.O_WRONLY);
            byte[] output = NNStub.openFile(request.build().toByteArray());
            OpenFileResponse response = OpenFileResponse.parseFrom(output);
            int fd = response.getFiledescriptor();
            File f = new File(Filename);
            bis = new BufferedInputStream(new FileInputStream(f));
            //Read [block_size] chunks of the file and keep looping until reached EOF
            while(bis.read(block,0,block_size) != -1){
                //For each read block, get an assigned Block and DataNodes from NameNode
                AssignBlockRequest.Builder req = AssignBlockRequest.newBuilder();
                AssignBlockResponse resp = null;
                req.setFiledescriptor(fd);
                output = NNStub.assignBlock(req.build().toByteArray());
                resp = AssignBlockResponse.parseFrom(output);
                int blocknum  = resp.getBlocknumber();
                //For each DataNode in the response message write the block
                for(DataNodeInfo dn: resp.getDatanodeList()){
                    if(dn.getServername().length() == 0){ continue;}
                    //Get DN, if connection times out alert user and continue to next node
                    try {
                        DNStub = GetDNStub(dn.getServername(), dn.getIpaddr(), dn.getPortnum());
                    }
                    catch(Exception e){
                        System.out.println("Unable to reach DataNode " +dn.getServername() + ". Moving to next one");
                        continue;
                    }
                    WriteBlockRequest.Builder writerequest = WriteBlockRequest.newBuilder();
                    WriteBlockResponse writerresponse = null;
                    writerequest.setBlocknumber(blocknum);
                    writerequest.setData(ByteString.copyFrom(block));
                    output = DNStub.writeBlock(writerequest.build().toByteArray());
                    writerresponse = WriteBlockResponse.parseFrom(output);
                    //If one DataNode fails to write the block throw error (Is this the proper behavior?)
                    if (writerresponse.getStatus() < 0){
                        throw new Exception("Error Writing to DataNode: " + dn.getServername());
                    }
                }
            }
            //Close File locally and in File System
            bis.close();
            CloseFileRequest.Builder closerequest = CloseFileRequest.newBuilder();
            closerequest.setFiledescriptor(fd);
            output = NNStub.closeFile(closerequest.build().toByteArray());
            CloseFileResponse closeresponse = CloseFileResponse.parseFrom(output);
            //Make sure file was closed properly
            if(closeresponse.getStatus() < 0){ throw new Exception("Retrieved File but could not close File in File System");}
        }catch(Exception e){
            System.err.println("Error Writing File"+ e.toString());
            e.printStackTrace();
            return;
        }
    }

    public void GetFile(String FileName)
    {
        try {
            OpenFileRequest.Builder request = OpenFileRequest.newBuilder();
            request.setFilename(FileName);
            request.setFlag(OpenFileRequest.Flag.O_RDONLY);
            //Send OpenFile Request and get FileDescriptor and block numbers
            byte[] output = NNStub.openFile(request.build().toByteArray());
            OpenFileResponse response = OpenFileResponse.parseFrom(output);
            int fd = response.getFiledescriptor();
            List<Integer> blocks = response.getBlocknumberList();
            //Open the local file
            File out = new File(FileName);
            OutputStream os = new FileOutputStream(out);
            //Loop through each block
            for(int block: blocks){
                //For each block get the datanodes for them
                BlockLocationsRequest.Builder locationrequest = BlockLocationsRequest.newBuilder();
                locationrequest.setBlocknumber(block);
                output = NNStub.getBlockLocations(locationrequest.build().toByteArray());
                BlockLocationsResponse locationresponse = BlockLocationsResponse.parseFrom(output);
                if(locationresponse.getDatanodeCount() == 0){
                    System.out.println("Error: All DataNodes containing a portion of this file are down");
                    break;
                }
                //Loop throught the datanodes
                for(int i = 0; i < locationresponse.getDatanodeCount(); i++){
                    try {
                        //Get the data for the block from the DataNode
                        DataNodeInfo dn = locationresponse.getDatanode(i);
                        if(dn.getServername().length() == 0){ continue;}
                        //Get DN, if connection times out alert user and continue to next node
                        try {
                            DNStub = GetDNStub(dn.getServername(), dn.getIpaddr(), dn.getPortnum());
                        }
                        catch(Exception e){
                            //If gone through all datanodes, throw an exception
                            if(i == (locationresponse.getDatanodeCount() - 1)){
                                System.out.println("Error: All DataNodes for this file are down");
                                throw new Exception();
                            }
                            continue;
                        }
                        //Create the request and read the block from the datanode
                        ReadBlockRequest.Builder readrequest = ReadBlockRequest.newBuilder();
                        readrequest.setBlocknumber(block);
                        output = DNStub.readBlock(readrequest.build().toByteArray());
                        ReadBlockResponse readresponse = ReadBlockResponse.parseFrom(output);
                        //If DataNode returned invalid response throw an error
                        if(readresponse.getStatus() < 0){ throw new Exception(); }
                        //Response is ok, write the block and move onto the next block
                        os.write(readresponse.getData().toByteArray());
                        break;
                    }
                    //Catches if one datanode was not able to retrieve block
                    catch(Exception e){
                        //If looped through all datanodes for this block without successfully getting the block throw an error
                        if(i == (locationresponse.getDatanodeCount() - 1)){
                            System.out.println("Unable to retrieve a portion of this file");
                            return;
                        }
                        else{ //try the next DataNode
                            continue;
                        }
                    }

                }

            }
            //Close Local and FileSystem files
            os.close();
            CloseFileRequest.Builder closerequest = CloseFileRequest.newBuilder();
            closerequest.setFiledescriptor(fd);
            output = NNStub.closeFile(closerequest.build().toByteArray());
            CloseFileResponse closeresponse = CloseFileResponse.parseFrom(output);
            //Make sure file was closed properly
            if(closeresponse.getStatus() < 0){ throw new Exception("Could not close File in File System");}
        }
        catch(Exception e){
            System.err.println("Error retreiving File: "+ e.toString());
            e.printStackTrace();
            return;
        }

    }

    public void List()
    {
        try {
            //Build A listfilerequest message and parse response as listfileresponse message
            ListFileRequest.Builder request = ListFileRequest.newBuilder();
            request.setDir("");
            byte[] out = NNStub.list(request.build().toByteArray());
            ListFileResponse response = ListFileResponse.parseFrom(out);
            //If Response is good, print results otherwise throw exception
            if(response.getStatus() == -1){throw new Exception("Received Bad Response from NameNode");}
            System.out.println("Directory:");
            for (String file: response.getFilenameList()){
                System.out.println("\t- "+file);
            }
        }
        catch(Exception e){
            System.err.println("Error at list "+ e.toString());
            e.printStackTrace();
        }

    }

    public static void main(String[] args) throws RemoteException, UnknownHostException, IOException
    {
        // To read config file and Connect to NameNode
        BufferedReader in = null;
        in = new BufferedReader(new FileReader(NN_ConfigFile));
        String line = null;
        ArrayList<String> config = new ArrayList<String>();
        while((line = in.readLine()) != null){
            config.add(line);
        }
        in.close();
        String name = config.get(0).split(":")[1];
        String ip = config.get(1).split(":")[1];
        int port = Integer.parseInt(config.get(2).split(":")[1]);
        //Intitalize the Client
        Client Me = new Client();
        Me.heartbeat = Integer.parseInt(config.get(5).split(":")[1]);
        Me.block_size = Integer.parseInt(config.get(6).split(":")[1]);
        Me.NNStub = Me.GetNNStub(name,ip,port);
        System.out.println("Welcome to HDFS!!");
        Scanner Scan = new Scanner(System.in);
        while(true)
        {
            //Scanner, prompt and then call the functions according to the command
            System.out.print("$> "); //Prompt
            String Command = Scan.nextLine();
            String[] Split_Commands = Command.split(" ");

            if(Split_Commands[0].equals("help"))
            {
                System.out.println("The following are the Supported Commands");
                System.out.println("1. put filename ## To put a file in HDFS");
                System.out.println("2. get filename ## To get a file in HDFS"); System.out.println("2. list ## To get the list of files in HDFS");
            }
            else if(Split_Commands[0].equals("put"))  // put Filename
            {
                //Put file into HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.PutFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("get"))
            {
                //Get file from HDFS
                String Filename;
                try{
                    Filename = Split_Commands[1];
                    Me.GetFile(Filename);
                }catch(ArrayIndexOutOfBoundsException e){
                    System.out.println("Please type 'help' for instructions");
                    continue;
                }
            }
            else if(Split_Commands[0].equals("list"))
            {
                System.out.println("List request");
                //Get list of files in HDFS
                Me.List();
            }
            else
            {
                System.out.println("Please type 'help' for instructions");
            }
        }
    }
}

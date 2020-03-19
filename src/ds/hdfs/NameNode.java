package ds.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.*;

//import ds.hdfs.hdfsformat.*;
import ds.hdfs.HdfsProto.*;

public class NameNode implements INameNode{

	protected Registry serverRegistry;

	// Using nn_config.txt as the default Name Node config file.
	private static String NN_ConfigFile = "nn_config.txt";
	// using nn_files_proto to store all files info in system
	// with Google Protocol Buffer
	private static String NN_Files_Proto = "nn_files_proto";

	//Max number for file descriptor
	private int FD_MAX_NUM = 16384;//2^13
	//Max number for block number
	private int BLOCKS_MAX_NUM = 32768;//2^15	
	//Block size
	private int BLOCK_SZIE = 64;//Bytes
	//bitmap for fd
	byte[] bitmap_fd = null;//new byte[FD_MAX_NUM/8];
	//bitmap for blocks
	byte[] bitmap_blocks = null;//new byte[BLOCKS_MAX_NUM/8];
	
	//records of all files opened, pair with <file Descriptor, file name>.
	//a file can be opened by multi reading users.
	private HashMap<Integer,String> openFileList = null;// new HashMap<Integer, String>();
	//A list of File metadata, pair with <file name, FileInfo>
	private HashMap<String, FileInfo> fileTable =  null;//new HashMap<String, FileInfo>();
	
	/*
	//records of the block info that links to the Data Node, pair with <blocknumber, HashMap<String, DataNodeTs>>.
	private HashMap<Integer,  ArrayList<DataNodeTS>> blockTable = new HashMap<Integer,  ArrayList<DataNodeTS>>();
	//records of the Data Node 
	private List<DataNode> datanodeList = new ArrayList<DataNode>();
	*/
	
	//records of the block info that links to the Data Node, pair with <blocknumber, HashMap<String, DataNodeTs>>.
	private HashMap<Integer,  ArrayList<DataNode>> blockTable = null;//new HashMap<Integer,  ArrayList<DataNode>>();
	//records of the active Data Nodes 
	private List<DataNodeActive> datanodeListActive = null;//new ArrayList<DataNodeActive>();

	String  ip_addr = null;
	int 	port_num = 0;
	String  name_node = null;
	
	public NameNode(String addr,int p, String nn,
					int max_num_fd, int max_num_blocks,int blocksize){
		this.ip_addr = addr;
		this.port_num = p;
		this.name_node = nn;
		
		if(max_num_fd > 0) this.FD_MAX_NUM = max_num_fd; 
		if(max_num_blocks > 0) this.BLOCKS_MAX_NUM = max_num_blocks;	
		if(blocksize > 0) this.BLOCK_SZIE = blocksize;
		bitmap_fd = new byte[this.FD_MAX_NUM/8];
		bitmap_blocks = new byte[this.BLOCKS_MAX_NUM/8];
		
		openFileList = new HashMap<Integer, String>();
		fileTable =  new HashMap<String, FileInfo>();
		blockTable = new HashMap<Integer,  ArrayList<DataNode>>();
		datanodeListActive = new ArrayList<DataNodeActive>();
		
		getFileMetaData();
	}

	public static class DataNodeActive{
		//String uid;
		DataNode dnode;
		long ts;
		public DataNodeActive(DataNode dnode, long ts){
			this.dnode = dnode;
			this.ts = ts;
		}
	}
	
	public static class DataNode{
		String ip;
		int port;
		String serverName;
		public DataNode(String addr,int p,String sname){
			ip = addr;
			port = p;
			serverName = sname;
		}
	}

	class FileInfo{
		String filename;
		int filehandle;
		boolean writemode;
		ArrayList<Integer> Chunks;
		public FileInfo(String name, int handle, boolean option){
			filename = name;
			filehandle = handle;
			writemode = option;
			Chunks = new ArrayList<Integer>();
		}
	}

	/* Method to open a file given file name with read-write flag*/
	boolean findInFilelist(int fd){
		return openFileList.containsKey(fd);
	}

	public void printFilelist(){
		for(Map.Entry<Integer, String> entry: openFileList.entrySet()) {
			int fd = (int)entry.getKey();
			System.out.println("File descriptor: "+fd+" File name: "+ entry.getValue());
		}
	}

	
	public boolean getBitmap(byte[] bitmap, int pos){
		int index = pos/8;
		int offset = pos&7; //pos%8;
		byte b = (byte) (bitmap[index] & (1 << offset));
		if (b != 0)
			return true;
		else
			return false;
	}
	public void setBitmap(byte[] bitmap, int pos){
		//bitmap[pos/8] |= (1<<(pos%8));
		bitmap[pos/8] |= 1 << (pos & 7);
	}

	void unsetBitmap(byte[] bitmap, int pos) {
	    bitmap[pos/8] &= ~(1 << (pos & 7));
	}

	public int getAvailNo(byte[] bitmap) {
		int avail_no = -1;
		int size = bitmap.length*8;
		for(int pos_in_bitmap = 0; pos_in_bitmap < size; pos_in_bitmap++){
			if(!getBitmap(bitmap, pos_in_bitmap)){
				avail_no = pos_in_bitmap;
				break;
			}
		}
		// Update the bitmap
		if(avail_no != -1){
			//set the bitmap
			setBitmap(bitmap, avail_no);
		}
		return avail_no;
	}


	public byte[] openFile(byte[] inp) throws RemoteException {
		//Open file request
		OpenFileRequest request = null;
		//Open file Response
		OpenFileResponse.Builder response = OpenFileResponse.newBuilder();
		//file Descriptor for response
		int fd = -1;

		try{
			//Get request parameter from open file request
			request = OpenFileRequest.parseFrom(inp);
			String file_name = request.getFilename();
			ds.hdfs.HdfsProto.OpenFileRequest.Flag file_rw_flag = request.getFlag();
			
			//check if openFileList is empty
			if(openFileList == null) {
				openFileList = new HashMap<Integer,String>();
			}else{
				//check if this file is already in the Open List,
				//and if open flag is for writing. If true, return null.
				if(openFileList.containsValue(file_name) && file_rw_flag != ds.hdfs.HdfsProto.OpenFileRequest.Flag.O_RDONLY){
						response.setFiledescriptor(-1);
						response.setDatanode(null);
						return response.build().toByteArray();	
				//If for reading, check if file exists in system 
				}else if(!fileTable.containsValue(file_name) && file_rw_flag == ds.hdfs.HdfsProto.OpenFileRequest.Flag.O_RDONLY) {
					response.setFiledescriptor(-1);
					response.setDatanode(null);
					return response.build().toByteArray();					
				}
			}
			//this is an valid new openFile request
			fd = getAvailNo(bitmap_fd);
			openFileList.put(fd, file_name);

			//If this is a request to create a new file, put it into fileTable			
			if(file_rw_flag != ds.hdfs.HdfsProto.OpenFileRequest.Flag.O_RDONLY) {
				if(!fileTable.containsKey(file_name)) {
					FileInfo fi = new FileInfo(file_name, fd, true);
					fileTable.put(file_name, fi);					
				}
			}

			response.setFiledescriptor(fd);
			response.setDatanode(null);			
		}catch (Exception e){
			System.err.println("Error at " + this.getClass() + e.toString());
			e.printStackTrace();
			response.setFiledescriptor(-1);
			response.setDatanode(null);
		}
		return response.build().toByteArray();
	}

	public synchronized byte[] closeFile(byte[] inp ) throws RemoteException{
		CloseFileRequest request = null;
		CloseFileResponse.Builder response = CloseFileResponse.newBuilder();
		try{
			request = CloseFileRequest.parseFrom(inp);
			int fd = request.getFiledescriptor();
			if(openFileList.containsKey(fd)) {
				String filename = openFileList.get(fd);
				FileInfo fi = fileTable.get(filename);	
				//if new file, write the fileTable back to local
				if(fi.writemode) {
					writeFileTableToLocal(fi);					
				}
				
				openFileList.remove(fd);
				response.setStatus(0);
			}else {
				//this file doesn't exist
				response.setStatus(-1);
			}						
		}
		catch(Exception e){
			System.err.println("Error at closefileRequest " + e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}

		return response.build().toByteArray();
	}

	public byte[] getBlockLocations(byte[] inp ) throws RemoteException	{
		BlockLocationsRequest request = null;
		BlockLocationsResponse.Builder response = BlockLocationsResponse.newBuilder();
		try{
			request = BlockLocationsRequest.parseFrom(inp);
			int blocknumber = request.getBlocknumber();
			ArrayList<DataNode> dnode_in_block = blockTable.get(blocknumber);
			for(DataNode dnode:dnode_in_block) {
				DataNodeInfo.Builder dinfo = DataNodeInfo.newBuilder();
				dinfo.setServername(dnode.serverName);
				dinfo.setIpaddr(dnode.ip);
				dinfo.setPortnum(dnode.port);
				response.addDatanode(dinfo);
			}
			response.setStatus(0);			
			
		}catch(Exception e){
			System.err.println("Error at getBlockLocations "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}


	public byte[] assignBlock(byte[] inp ) throws RemoteException{
		AssignBlockRequest request = null;
		AssignBlockResponse.Builder response = AssignBlockResponse.newBuilder();
		try{
			//If there are more than 2 Data Nodes, pick 2 randomly from the list of Data Nodes.
            int fd = request.getFiledescriptor();
			int block_num = getAvailNo(bitmap_blocks);
            response.setBlocknumber(block_num);
            
			int dn_list_size = datanodeListActive.size(); 
			ArrayList<DataNode> dnodes_in_block = new ArrayList<DataNode>();//for block table new item
			DataNode dnode = null;
            if(dn_list_size > 2) {
            	int r_num = getRandomElement(dn_list_size, 0);
            	DataNodeActive dnodeTS = datanodeListActive.get(r_num);
            	dnode = dnodeTS.dnode;
            	dnodes_in_block.add(dnode);
            	addAssignBlockDatanodeInfo(response, dnode);
            	r_num = getRandomElement(dn_list_size, r_num);
            	dnodeTS = datanodeListActive.get(r_num);
            	dnode = dnodeTS.dnode;
            	dnodes_in_block.add(dnode);
            	addAssignBlockDatanodeInfo(response, dnode);            	
            }else if (dn_list_size == 2){
            	DataNodeActive dnodeTS = datanodeListActive.get(0);
            	dnode = dnodeTS.dnode;
            	dnodes_in_block.add(dnode);
            	addAssignBlockDatanodeInfo(response, dnode);
            	dnodeTS = datanodeListActive.get(1);
            	dnode = dnodeTS.dnode;
            	dnodes_in_block.add(dnode);            	
            	addAssignBlockDatanodeInfo(response, dnode);            	            	
            }else if (dn_list_size == 1) {
            	DataNodeActive dnodeTS = datanodeListActive.get(0);
            	dnode = dnodeTS.dnode;
            	dnodes_in_block.add(dnode);            	
            	addAssignBlockDatanodeInfo(response, dnode);            	
            }else {
    			response.setStatus(-1);            	            	
            }            
            response.setStatus(0);
            
            //update File table with new block
            String filename = openFileList.get(fd);
            FileInfo fi = fileTable.get(filename);
            System.out.println(fileTable.get(filename).Chunks.size());
            ArrayList<Integer> chunks = fi.Chunks;
            chunks.add(block_num);
            fileTable.put(filename, fi);
            System.out.println(fileTable.get(filename).Chunks.size());

            //add a new block into block table
            blockTable.put(block_num, dnodes_in_block);
            
		}
		catch(Exception e)
		{
			System.err.println("Error at AssignBlock "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}

		return response.build().toByteArray();
	}

	public void addAssignBlockDatanodeInfo(AssignBlockResponse.Builder response, DataNode dnode) {
		//AssignBlockResponse.DataNode.Builder datanode = AssignBlockResponse.DataNode.newBuilder();
		DataNodeInfo.Builder datanode = DataNodeInfo.newBuilder();
		
		datanode.setServername(dnode.serverName);
		datanode.setIpaddr(dnode.ip);
		datanode.setPortnum(dnode.port);
		response.addDatanode(datanode);
	}
	
    public int getRandomElement(int size, int fixed){ 
        Random rand = new Random(); 
        int r_num = 0;
        while(true) {
        	r_num = rand.nextInt(size);
        	if(fixed != 0 && r_num != fixed) {
        		break;
        	}else if(fixed == 0){
        		break;
        	}
        } 
        return r_num;
    } 
    
	public byte[] list(byte[] inp ) throws RemoteException{
		ListFileRequest request = null;
		ListFileResponse.Builder response = ListFileResponse.newBuilder(); 
		//FileListInNameNode file_list = null;
		try{
			request = ListFileRequest.parseFrom(inp);
			String dir = request.getDir();
			
			/*
            FileInputStream f_input = new FileInputStream(NN_Files_Proto);
            file_list = FileListInNameNode.parseFrom(f_input);
            f_input.close();			
            if(file_list != null){                
                for(FileInfoInNameNode fileinfo: file_list.getFileList()){
                	response.addFilename(fileinfo.getFilename());
                }
            }
            */
			for(Map.Entry<String,FileInfo> entry: fileTable.entrySet()) {
				String filename = (String)entry.getKey();
				response.addFilename(filename);				
			}
			response.setStatus(0);            
		}catch(Exception e){
			
			System.err.println("Error at list "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}

	// input: blocks info of the coming data node &
	// update the blockTable and datanodeList
	public synchronized byte[] blockReport(byte[] inp ) throws RemoteException{
		BlockReportRequest request = null;
		BlockReportResponse.Builder response = BlockReportResponse.newBuilder();		
		try{
			request = BlockReportRequest.parseFrom(inp);
			
			for(BlockReportRequest.Block block: request.getBlockList()) {
				int block_num = block.getBlocknumber();
				DataNodeInfo dnodeInfo = block.getDatanode();
				String uid = dnodeInfo.getServername();
				String ipaddr = dnodeInfo.getIpaddr();
				int port = dnodeInfo.getPortnum();			
				DataNode dnode = new DataNode(ipaddr,port,uid);
				
				if(blockTable.containsKey(block_num)) {
					ArrayList<DataNode> dnlst_this_block = blockTable.get(block_num);
					if(dnlst_this_block == null) {
						//there is a new DataNode for this block number, new an arraylist
						dnlst_this_block = new ArrayList<DataNode>();
						/*
						tm_s = new Date().getTime();
						dnodeTS = new DataNodeTS(uid, tm_s);
						*/
						dnlst_this_block.add(dnode);
					}else {
						if(containsUidInBlockTable(dnlst_this_block, uid, true)) {
							/*
							tm_s = new Date().getTime();
							dnodeTS = new DataNodeTS(uid, tm_s);
							*/
							dnlst_this_block.add(dnode);							
						}else {
							/*
							tm_s = new Date().getTime();
							dnodeTS = new DataNodeTS(uid, tm_s);
							*/
							dnlst_this_block.add(dnode);														
						}
					}
				}else {
					ArrayList<DataNode> dnlst_this_block = new ArrayList<DataNode>();
					/*
					tm_s = new Date().getTime();
					dnodeTS = new DataNodeTS(uid, tm_s);
					*/
					dnlst_this_block.add(dnode);
					blockTable.put(block_num, dnlst_this_block);
				}
				
				if(containsUidInActiveDatanodeList(datanodeListActive, uid, true)) {
					long tm_s = new Date().getTime();
					DataNodeActive dnodeTS = new DataNodeActive(dnode, tm_s);
					datanodeListActive.add(dnodeTS);
				}else {
					long tm_s = new Date().getTime();
					DataNodeActive dnodeTS = new DataNodeActive(dnode, tm_s);
					datanodeListActive.add(dnodeTS);					
				}
			}
			response.setStatus(0);
			
		}catch(Exception e){
			System.err.println("Error at blockReport "+ e.toString());
			e.printStackTrace();
			response.setStatus(-1);
		}
		return response.build().toByteArray();
	}
	
	public boolean containsUidInActiveDatanodeList(List<DataNodeActive> list, String uid, boolean flag_remove) {
		for(int i = 0; i < list.size(); i++) {
			DataNodeActive dnodeTS = list.get(i);
			if(dnodeTS.dnode.serverName.equals(uid)) {
				if(flag_remove) {
					list.remove(i);
				}
				return true;
			}
		}
		return false;
	}

	public boolean containsUidInBlockTable(List<DataNode> list, String uid, boolean flag_remove) {
		for(int i = 0; i < list.size(); i++) {
			DataNode dnode = list.get(i);
			if(dnode.serverName.equals(uid)) {
				if(flag_remove) {
					list.remove(i);
				}
				return true;
			}
		}
		return false;
	}
	
	public synchronized byte[] heartBeat(byte[] inp ) throws RemoteException{
	    HeartBeatRequest request = null;
	    HeartBeatResponse.Builder response = HeartBeatResponse.newBuilder(); 	    
        try{
        	request = HeartBeatRequest.parseFrom(inp);
        	DataNodeInfo dnodeInfo = request.getDatanode();
			String uid = dnodeInfo.getServername();
			String ipaddr = dnodeInfo.getIpaddr();
			int port = dnodeInfo.getPortnum();			
			DataNode dnode = new DataNode(ipaddr,port,uid);
        	
			if(containsUidInActiveDatanodeList(datanodeListActive, uid, true)){
				//update this Data node
				long tm_s = new Date().getTime();
				DataNodeActive dnodeTS = new DataNodeActive(dnode, tm_s);
				datanodeListActive.add(dnodeTS);
			}else {
				//add a new Data node
				long tm_s = new Date().getTime();
				DataNodeActive dnodeTS = new DataNodeActive(dnode, tm_s);
				datanodeListActive.add(dnodeTS);					
			}
			response.setStatus(0);

        } catch (Exception e) {
            System.out.println("Parsing heart beat request problem?? " + e.getMessage());
            e.printStackTrace();
            response.setStatus(-1);
        }
		return response.build().toByteArray();
	}

	public void printMsg(String msg){
		System.out.println(msg);
	}

	public static void main(String[] args) throws InterruptedException, NumberFormatException, IOException
	{
        // Parsing config file
        BufferedReader input = null;
        input = new BufferedReader(new FileReader(NN_ConfigFile));
        String strline = null;

    	//suppose there is only one Name Node
    	ArrayList<String> arr = new ArrayList<String>();
        while((strline = input.readLine()) != null) {
        	/*
        	String[] arr_str = strline.split(";");
        	if(arr_str != null) {
        		name_node = arr_str[0];
        		ip_addr = arr_str[1];
        		port_num = Integer.parseInt(arr_str[2]);
        	}
        	*/
        	arr.add(strline);
        }
        
    	String  name_node = arr.get(0).split(":")[1];    	
    	String  ip_addr = arr.get(1).split(":")[1];
    	int 	port_num = Integer.parseInt(arr.get(2).split(":")[1]);
    	int max_num_fd = Integer.parseInt(arr.get(3).split(":")[1]);
    	int max_num_blocks = Integer.parseInt(arr.get(4).split(":")[1]);
    	int block_size = Integer.parseInt(arr.get(5).split(":")[1]);
        
        NameNode thisNN = new NameNode(ip_addr, port_num, name_node,
        		max_num_fd,max_num_blocks,block_size);
        
        // create the URL to contact the rmiregistry
        String url = "//"+ip_addr+":" + port_num + "/HDFSNameNode";
        System.out.println("binding " + url);
        // register it with rmiregistry
        Naming.rebind(url, thisNN);
	}

    private  void getFileMetaData() {
        int blocknumber = 0;    
		FileListInNameNode file_list = null;
		try{					
            FileInputStream f_input = new FileInputStream(NN_Files_Proto);
            //get file list
            file_list = FileListInNameNode.parseFrom(f_input);
            f_input.close();
			
            if(file_list != null){ 
            	//get file info
                for(FileInfoInNameNode fileinfo: file_list.getFileList()){                	
                	String filename = fileinfo.getFilename();
        			ArrayList<Integer> chunks = new ArrayList<Integer>();
        			//get all block numbers
                	for(FileInfoInNameNode.Block block:fileinfo.getBlockList()) {
                		blocknumber = block.getBlocknumber();
                		chunks.add(blocknumber);
                		//no need for Data Node info for this structure
                		//for(DataNodeInfo dnodeInfo : block.getDatanodeList()){}
                	}
                	boolean writemode = fileinfo.getWritemode();
                	FileInfo fi = new FileInfo(filename, 0, writemode);
                	fi.Chunks = chunks;
                	fileTable.put(filename, fi);
                }
            }
		}catch(Exception e){
			System.err.println("Error at FileTable "+ e.toString());
			e.printStackTrace();
		}		
    }
    
    //After a written file closed, new file or updated file info should be recorded   
    public synchronized void  writeFileTableToLocal(FileInfo fi) {
		FileListInNameNode file_list = null;
		try{					
            file_list = FileListInNameNode.parseFrom(new FileInputStream(NN_Files_Proto));			
        	String filename = fi.filename;
    		int fd = fi.filehandle;
    		boolean writemode = fi.writemode;
    		ArrayList<Integer> chunks = fi.Chunks;
        	
    		FileInfoInNameNode.Builder fileinfo = FileInfoInNameNode.newBuilder();
    		//add file name 
    		fileinfo.setFilename(filename);
    		//add all block numbers
    		for (int blocknumber: chunks) {
        		FileInfoInNameNode.Block.Builder block = FileInfoInNameNode.Block.newBuilder();
    			block.setBlocknumber(blocknumber);
    			//add all data nodes that link to this block number
    			 ArrayList<DataNode> datalist = blockTable.get(blocknumber);
    			 for(DataNode dnode : datalist) {
    				 DataNodeInfo.Builder dnodeInfo = DataNodeInfo.newBuilder();
    				 dnodeInfo.setServername(dnode.serverName);
    				 dnodeInfo.setIpaddr(dnode.ip);
    				 dnodeInfo.setPortnum(dnode.port);
    				 block.addDatanode(dnodeInfo);    				 
    			 }
    			 fileinfo.addBlock(block);
    		}
    		fileinfo.setWritemode(writemode);
    		FileListInNameNode.Builder renewed_file_list = FileListInNameNode.newBuilder();
    		renewed_file_list = FileListInNameNode.newBuilder(file_list);
    		renewed_file_list.addFile(fileinfo);
    		
            renewed_file_list.build().writeTo(new FileOutputStream(NN_Files_Proto));
		}catch(Exception e){
			System.err.println("Error at FileTable "+ e.toString());
			e.printStackTrace();
		}		
    }
}

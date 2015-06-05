package ict.org.apache.hadoop.hbase.performance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutSpeedTestNew {
	private Configuration conf = null;
	private String tableName = "_putTest";
    int threadN = 0;
    private String filePath="";
    
    public PutSpeedTestNew(String filePath,String tableName,int threadNum){
    	this.filePath = filePath;
    	this.tableName = tableName;
    	this.threadN  = threadNum;
    }
	  
	public void process() throws IOException{
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
		conf =  HBaseConfiguration.create();

	    for (int i = 0; i < this.threadN; i++) {
	        Thread d = new Thread(String.valueOf(i)) {
	          public void run() {
	            try {
	              executePut();
	            } catch (IOException e) {
					e.printStackTrace();
				} finally {
	              threadN--;
	              if(threadN==0){
	            	  System.out.println("No thread existing!");
	              }
	            }
	          }
	        };
	        d.start();
	      }
	}
	
	public void createTable() throws IOException{
		conf =  HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor htd = new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor("f1"));
//		htd.addFamily(new HColumnDescriptor("f2"));
		byte[][] splitkey = { Bytes.toBytes("1"),
		        Bytes.toBytes("2"), Bytes.toBytes("3"), Bytes.toBytes("4"),
		        Bytes.toBytes("5"), Bytes.toBytes("6"), Bytes.toBytes("7"),
		        Bytes.toBytes("8"), Bytes.toBytes("9")};
		admin.createTable(htd, splitkey);
		System.out.println("Create Sucess!");
	}
	
	public void deleteTable() throws IOException{
		conf =  HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		System.out.println("Delete Sucess!");
	}

	private void executePut() throws IOException{
		HTable table = new HTable(conf,tableName);
//		
		table.setAutoFlush(false);
		table.setWriteBufferSize(15*1024*1024);
		
		String id = Thread.currentThread().getName();
		File file = new File(this.filePath+id+".txt");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
		while(true){
			String element = br.readLine();
			if(element==null){
				break;
			}
			String elements[] = element.split(" "); 
			Put p = new Put(Bytes.toBytes(elements[0]));
			p.setWriteToWAL(false);
			byte[] family1 = Bytes.toBytes("f1");		
			for(int i=1;i<elements.length;i++){
				p.add(family1,Bytes.toBytes("c"+i) , Bytes.toBytes(elements[i]));
			}
			try {
				table.put(p);
			} catch (IOException e) {
				e.printStackTrace();
			}
			synchronized (PutSpeedTestNew.class) {
				putLines++;
	        }
		}
	}
	
	static long putLines = 0;
    static long time = System.currentTimeMillis();
	static long putLast = 0;
	static long startT = System.currentTimeMillis();

	public static Thread output = new Thread() {
	    public void run() {
	      while (true) {
	        try {
	          sleep(4000);
	        } catch (InterruptedException e) {
	          e.printStackTrace();
	        }
	        System.out.println("Start Time:" + startT + "\tNow:"
	            + System.currentTimeMillis() + "\tTotal :" + putLines
	            + "\tCurrent put Speed:" + ((putLines - putLast) * 1000)
	            / (System.currentTimeMillis() - time) + "\tTotal put Speed:"
	            + (putLines * 1000) / (System.currentTimeMillis() - startT)
	            + " r/s");
	        time = System.currentTimeMillis();
	        putLast = putLines;
	      }
	    }
	};
	
	public static void main(String args[]) throws IOException{
//		String filePath="/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/3000w/";
//		String filePath = "/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/4000w/";
		String filePath = "/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/5000w/";
		
		String tableName = "_putTest";
		int threadNum = 32;
		
		PutSpeedTestNew test = new PutSpeedTestNew(filePath,tableName,threadNum);
		test.deleteTable();
		test.createTable();
		test.process();
	}
}

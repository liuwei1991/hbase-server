package ict.org.apache.hadoop.hbase.performance;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class PutSpeedTest {
	private Configuration conf = null;
	private HBaseAdmin admin = null;
//	private HTable table = null;
	private String tableName = "_putTest";
    int threadN = 0;
    Random r = new Random();
	  
	public void process() throws IOException{
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();
		conf =  HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);

	    for (int i = 0; i < 2; i++) {
	        Thread d = new Thread(String.valueOf(i)) {
	          public void run() {
	            try {
	              executePut();
	            } catch (IOException e) {
					// TODO Auto-generated catch block
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
	        threadN++;
	      }
	}

	private void executePut() throws IOException{
		HTable table = new HTable(conf,tableName);
		table.setAutoFlush(false);
		table.setWriteBufferSize(10*1024*1024);
		byte[] threadName = Bytes.toBytes(Thread.currentThread().getName());
		while(true){
			byte[] rowKey = Bytes.add(threadName, Bytes.toBytes(r.nextInt()));
			Put p = new Put(rowKey);
			
			p.setWriteToWAL(false);
			byte[] family1 = Bytes.toBytes("f1");
			byte[] family2 = Bytes.toBytes("f2");
			byte[] value = Bytes.toBytes("abcdefghijklmn");
			p.add(family1,Bytes.toBytes("c1") ,value );
//			p.add(family1,Bytes.toBytes("c2") ,value );
//			p.add(family1,Bytes.toBytes("c3") ,value );
//			p.add(family1,Bytes.toBytes("c4") ,value );
//			p.add(family1,Bytes.toBytes("c5") ,value );
//			p.add(family2,Bytes.toBytes("c6") ,value );
//			p.add(family2,Bytes.toBytes("c7") ,value );
//			p.add(family2,Bytes.toBytes("c8") ,value );
//			p.add(family2,Bytes.toBytes("c9") ,value );
//			p.add(family2,Bytes.toBytes("c10") ,value );
			
			try {
				table.put(p);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			synchronized (PutSpeedTest.class) {
				putLines++;
	        }
		}
	}
	
	public void createTable() throws IOException{
		conf =  HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		HTableDescriptor htd = new HTableDescriptor(tableName);
		htd.addFamily(new HColumnDescriptor("f1"));
//		htd.addFamily(new HColumnDescriptor("f2"));
		byte[][] splitkey = { Bytes.toBytes("0"), Bytes.toBytes("1"),
		        Bytes.toBytes("2"), Bytes.toBytes("3"), Bytes.toBytes("4"),
		        Bytes.toBytes("5"), Bytes.toBytes("6"), Bytes.toBytes("7"),
		        Bytes.toBytes("8"), Bytes.toBytes("9")};
		admin.createTable(htd, splitkey);
		System.out.println("Create Sucess!");
	}
	
	public void deleteTable() throws IOException{
		conf =  HBaseConfiguration.create();
		admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
		}
		System.out.println("Delete Sucess!");
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
	          // TODO Auto-generated catch block
	          e.printStackTrace();
	        }
	        System.out.println("Start Time:" + startT + "\tNow:"
	            + System.currentTimeMillis() + "\tTotal :" + putLines
	            + "\tCurrent Speed:" + ((putLines - putLast) * 1000)
	            / (System.currentTimeMillis() - time) + "\tTotal Speed:"
	            + (putLines * 1000) / (System.currentTimeMillis() - startT)
	            + " r/s");
	        time = System.currentTimeMillis();
	        putLast = putLines;
	      }
	    }
	};
	
	public static void main(String args[]) throws IOException{
		
		PutSpeedTest test = new PutSpeedTest();
		test.createTable();
//		if(args[0].equalsIgnoreCase("create")){
//			test.createTable();
//		}else if(args[0].equalsIgnoreCase("delete")){
//			test.deleteTable();
//		}else{
//			test.process();
//		}
		
//		String a = "NOT lIke ";
//		a = a .replaceAll(" (l|L)(i|I)(k|K)(e|E) ", " like ");
//		System.out.println(a);
	}

}

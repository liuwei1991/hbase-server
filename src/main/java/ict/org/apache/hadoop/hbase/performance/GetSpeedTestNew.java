package ict.org.apache.hadoop.hbase.performance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class GetSpeedTestNew {
	private Configuration conf = null;
	private String tableName = "_putTest";
    int threadN = 0;
    private String filePath="";
    
    public GetSpeedTestNew(String filePath,String tableName,int threadNum){
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

	private void executePut() throws IOException{
		HTable table = new HTable(conf,tableName);
		String id = Thread.currentThread().getName();
		File file = new File(this.filePath+id+".txt");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr,CommonVariable.BUFFERED_READER_SIZE);
		byte[] family1 = Bytes.toBytes("f1");
		
		while(true){
			String element = br.readLine();
			if(element==null){
				break;
			}
			String elements[] = element.split(" "); 
			Get get = new Get(Bytes.toBytes(elements[0]));

			get.addColumn(family1, Bytes.toBytes("c"+1));
//			for(int i=1;i<elements.length;i++){
//				get.addColumn(family1, Bytes.toBytes("c"+i));
//			}
			
			try {
				table.get(get);
			} catch (IOException e) {
				e.printStackTrace();
			}
			synchronized (GetSpeedTestNew.class) {
				getLines++;
	        }
		}
	}
	
	static long getLines = 0;
    static long time = System.currentTimeMillis();
	static long getLast = 0;
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
	            + System.currentTimeMillis() + "\tTotal :" + getLines
	            + "\tCurrent get Speed:" + ((getLines - getLast) * 1000)
	            / (System.currentTimeMillis() - time) + "\tTotal get Speed:"
	            + (getLines * 1000) / (System.currentTimeMillis() - startT)
	            + " r/s");
	        time = System.currentTimeMillis();
	        getLast = getLines;
	      }
	    }
	};
	
	public static void main(String args[]) throws IOException{
//		String filePath="/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/3000w/";
//		String filePath = "/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/4000w/";
		String filePath = "/ares/TestData/tpch/thread=32/keylen=32 columnNum=8/5000w/";
		
		String tableName = "_putTest";
		int threadNum = 32;
		
		GetSpeedTestNew get = new GetSpeedTestNew(filePath,tableName,threadNum);
		get.process();
	}

}

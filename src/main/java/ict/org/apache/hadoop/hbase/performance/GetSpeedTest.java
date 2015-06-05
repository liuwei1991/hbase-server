package ict.org.apache.hadoop.hbase.performance;

import java.io.IOException;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.client.Result;

public class GetSpeedTest {
	private static final Log LOG = LogFactory.getLog(ScanSpeedTest.class.getName());
	private String tableName;
	private static long rowNum = -1;
	private static long getNum=0;

	public void process(String tableName,String rowNum) throws RuntimeException{
		try {
			this.rowNum = Long.valueOf(rowNum);
			this.tableName = tableName;
			this.resolve();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void resolve() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		table = new HTable(conf,tableName);		

		HRegionInfo[] regions = table.getRegionLocations().keySet().toArray(new HRegionInfo[0]);
		table.close();
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(regions.length,regions.length,10,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(),
				new ThreadPoolExecutor.CallerRunsPolicy());
		for(int i=0;i<regions.length;i++){
			threadPool.execute(new GetSpeedTestRunable(conf,regions[i]));
		}
		while(true){
			if(threadPool.getActiveCount()==0){
				break;
			}
		}
		threadPool.shutdown();
	}
	
	static long time = System.currentTimeMillis();
	static long startTime = time;
	static long lastNum = 0;
	public static Thread output = new Thread() {
	    public void run() {
	      while (true) {
	        try {
	          sleep(6000);
	        } catch (InterruptedException e) {
	          // TODO Auto-generated catch block
	          e.printStackTrace();
	        }
	        long current = getNum;
	        System.out.println("Start Time:" + time + "\tNow:"
	            + System.currentTimeMillis() + "\tTotal :" + current
	            + "\tCurrent Speed:" + ((current - lastNum) * 1000)
	            / (System.currentTimeMillis() - time) + "\tTotal Speed:"
	            + (current * 1000) / (System.currentTimeMillis() - startTime)
	            + " r/s");
	        time = System.currentTimeMillis();
	        lastNum = current;
	      }
	    }
	};
	static {
		output.setPriority(Thread.MAX_PRIORITY);
	    output.start();
	}
	public class GetSpeedTestRunable implements Runnable{
		Configuration conf = null;
		private HRegionInfo region = null;
		
		public GetSpeedTestRunable(Configuration conf,HRegionInfo region) throws IOException{
			this.conf = conf;
			this.region = region;
		}
		
		@Override
		public void run() {
			HTable table = null;
			try {
				table = new HTable(conf,tableName);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return;
			}
			byte[] startKey = region.getStartKey();
			byte[] endKey = region.getEndKey();
			long start = 0;
			long inteveral = 0;
			if(startKey==null|| Bytes.toString(startKey).length()==0){
				start = 0;
				inteveral = Long.valueOf(Bytes.toString(endKey));
			}else if(endKey == null || Bytes.toString(endKey).length()==0){
				start = Long.valueOf(Bytes.toString(startKey));
				inteveral = rowNum - start;
			}else{
				start = Long.valueOf(Bytes.toString(startKey));
				inteveral = Long.valueOf(Bytes.toString(endKey)) - Long.valueOf(Bytes.toString(startKey));
			}
			
			byte[] rowKey = null;
			Random r = new Random();
			int length = String.valueOf(rowNum).length()-1;
			
			try {
				while(true){
					long key = start+(r.nextLong()%inteveral);
					if (key<start){
						key+=inteveral;
					}
					rowKey = Bytes.toBytes(String.format("%0"+length+"d", key));
					Get get = new Get(rowKey);
					get.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("c1"));
					Result result;
					try {
						result = table.get(get);
						if(result.isEmpty()){
							throw new IOException("no result!");
						}
						synchronized (GetSpeedTestRunable.class) {
							getNum++;
						}
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
				}
			}finally{
				try {
					table.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}
	public static void main(String[] args) throws NumberFormatException, Exception{
		GetSpeedTest test = new GetSpeedTest();
		test.process(args[0],args[1]);
	}
}

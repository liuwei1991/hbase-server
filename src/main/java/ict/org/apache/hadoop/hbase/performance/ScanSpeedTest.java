package ict.org.apache.hadoop.hbase.performance;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;

public class ScanSpeedTest{
	private static final Log LOG = LogFactory.getLog(ScanSpeedTest.class.getName());
	private String tableName;
	private static long scanNum = 0;
	
	public void process(String tableName,byte[] startRow,byte[] endRow) throws RuntimeException{
		try {
			this.tableName = tableName;
			this.resolve(startRow,endRow);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private void resolve(byte[] startRow,byte[] endRow) throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		table = new HTable(conf,tableName);
		HRegionInfo[] regions = table.getRegionLocations().keySet().toArray(new HRegionInfo[0]) ;
		table.close();
		int threadNum = regions.length;
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadNum,threadNum,10,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(),
				new ThreadPoolExecutor.CallerRunsPolicy());
		ScanSpeedTestRunable tr[] = new ScanSpeedTestRunable[regions.length];
		for(int i=0;i<regions.length;i++){
			tr[i] = new ScanSpeedTestRunable(conf,regions[i],startRow,endRow);
			threadPool.execute(tr[i]);
		}
		
		int finished = 0;
		int error = 0;
		
		while(true){
			finished = 0;
			for(int i=0;i<tr.length;i++){
				if(tr[i].isFinished()){
					finished++;
				}
				if(tr[i].isError()){
					error++;
				}
			}
			LOG.info("Finished threads:"+finished+",total threads:"+tr.length);
			if(finished==tr.length || error!=0){
				break;
			}
			Threads.sleep(18000);
		}
		System.currentTimeMillis();
		threadPool.shutdown();
		if(error!=0){
			throw new Exception("execute error!");
		}
	}
	
	public static Scan getScan(){
		Scan scan = new Scan();
		scan.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("c1"));
		return scan;
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
	        long current = scanNum;
	        System.out.println("Start Time:" + time + "\tNow:"
	            + System.currentTimeMillis() + "\tTotal:" + current
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
	public class ScanSpeedTestRunable implements Runnable{
		Configuration conf = null;
		private HRegionInfo region = null;
		private boolean finish = false;
		private boolean isError = false;
		
		public ScanSpeedTestRunable(Configuration conf,HRegionInfo region,byte[] startRow,byte[] endRow) throws IOException{
			this.conf = conf;
			this.region = region;
		}
		
		@Override
		public void run() {
			try {
				HTable table = new HTable(conf,tableName);
				table.setAutoFlush(false);
				table.setWriteBufferSize(10*1024*1024);
				Scan scan = getScan();
				scan.setStartRow(region.getStartKey());
				scan.setStopRow(region.getEndKey());
				
//				if(Bytes.compareTo(startRow, region.getEndKey())>0){
//					table.close();
//					this.finish = true;
//					return;
//				}else if(Bytes.compareTo(endRow, region.getStartKey())<0){
//					table.close();
//					this.finish = true;
//					return;
//				}else if(Bytes.compareTo(startRow, region.getStartKey())>=0 && Bytes.compareTo(endRow, region.getEndKey())>=0){
//					scan.setStartRow(startRow);
//					scan.setStopRow(region.getEndKey());
//				}else if(Bytes.compareTo(startRow, region.getStartKey())>=0 && Bytes.compareTo(endRow, region.getEndKey())<=0){
//					scan.setStartRow(startRow);
//					scan.setStopRow(endRow);
//				}else if(Bytes.compareTo(startRow, region.getStartKey())<=0 && Bytes.compareTo(endRow, region.getEndKey())>=0){
//					scan.setStartRow(region.getStartKey());
//					scan.setStopRow(region.getEndKey());
//				}else {
//					scan.setStartRow(region.getStartKey());
//					scan.setStopRow(endRow);
//				}
				
				ResultScanner rs = table.getScanner(scan);
				while(rs.next()!=null){
					synchronized (ScanSpeedTestRunable.class) {
						scanNum++;
					}
				}
				
				table.close();
				this.finish = true;
			} catch (IOException e) {
				this.isError = true;
				e.printStackTrace();
			}
		}

		/**
		 * Check if the thread has finished.
		 * @return true if the thread has been finished. false if hasn't been finished.
		 */
		public boolean isFinished(){
			return this.finish;
		}

		/**
		 * Check if the thread has encounter errors while executing. 
		 * @return true if this thread has encounter some errors, false if no error occur.
		 */
		public boolean isError(){
			return this.isError;
		}
	}
	
	public static void main(String[] args) throws NumberFormatException, Exception{
		ScanSpeedTest test = new ScanSpeedTest();
		test.process(args[0],Bytes.toBytes(args[1]),Bytes.toBytes(args[2]));
	}
}

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

public class ScanSpeedTestNew {
	private static final Log LOG = LogFactory.getLog(ScanSpeedTest.class.getName());
	private String tableName;
	private static long scanNum = 0;
	
	public void process(String tableName,boolean singleThread) throws RuntimeException{
		try {
			this.tableName = tableName;
			if(singleThread){
				this.singleThreadScan();
			}else{
				this.resolve();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	private void singleThreadScan() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		Thread t = new Thread(new ScanSpeedTestRunable(conf));
		t.start();
	}

	private void resolve() throws Exception{
		Configuration conf = HBaseConfiguration.create();
		HTable table = null;
		table = new HTable(conf,tableName);
		HRegionInfo[] regions = table.getRegionLocations().keySet().toArray(new HRegionInfo[0]) ;
		table.close();
		int threadNum = regions.length;
		System.out.println("thread numbers:"+threadNum);
		ThreadPoolExecutor threadPool = new ThreadPoolExecutor(threadNum,threadNum,10,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>(),
				new ThreadPoolExecutor.CallerRunsPolicy());
		ScanSpeedTestRunable tr[] = new ScanSpeedTestRunable[regions.length];
		for(int i=0;i<regions.length;i++){
			tr[i] = new ScanSpeedTestRunable(conf,regions[i]);
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
	          sleep(3000);
	        } catch (InterruptedException e) {
	          e.printStackTrace();
	        }
	        long current = scanNum;
	        System.out.println("Start Time:" + time + "\tNow:"
	            + System.currentTimeMillis() + "\tTotal:" + current
	            + "\tCurrent scan Speed:" + ((current - lastNum) * 1000)
	            / (System.currentTimeMillis() - time) + "\tTotal scan Speed:"
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
		
		public ScanSpeedTestRunable(Configuration conf){
			this.conf = conf;
		}
		
		public ScanSpeedTestRunable(Configuration conf,HRegionInfo region) throws IOException{
			this.conf = conf;
			this.region = region;
		}
		
		@Override
		public void run() {
			try {
				HTable table = new HTable(conf,tableName);
//				table.setAutoFlush(false);
//				table.setWriteBufferSize(10*1024*1024);
				Scan scan = getScan();
				scan.setCaching(2000);
				if(this.region!=null){
					scan.setStartRow(region.getStartKey());
					scan.setStopRow(region.getEndKey());
				}
				
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
	
	public static void main(String[] args) throws Exception{
		ScanSpeedTestNew test = new ScanSpeedTestNew();
		String tableName = "_putTest";
		boolean single = false;
		
		test.process(tableName,single);
	}
}

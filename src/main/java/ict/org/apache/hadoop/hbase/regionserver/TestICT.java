package ict.org.apache.hadoop.hbase.regionserver;

import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.KeyValueSkipListSet;
import org.apache.hadoop.hbase.util.Bytes;

public class TestICT {
	Comparator c = new Comparator<String>() {
		@Override
		public int compare(String o1, String o2) {
			if (o1.compareTo(o2) < 0) {
				return -1;
			} else if (o1.compareTo(o2) == 0) {
				return 0;
			}
			return 1;
		}
	};
	private ICTKeyValueSkipListSet ict = new ICTKeyValueSkipListSet(c ,  8);;
	private String tableName = "_putTest";
    int threadN = 0;
    Random r = new Random();
   
	public void process() throws IOException{
		output.setPriority(Thread.MAX_PRIORITY);
		output.start();

	    for (int i = 0; i < 10; i++) {
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
//		byte[] threadName = Bytes.toBytes(Thread.currentThread().getName());
		String threadName = Thread.currentThread().getName();
		while(true){
			String rowKey = threadName+Bytes.toBytes(r.nextInt());
//			Put p = new Put(rowKey);
//			p.setWriteToWAL(false);
			byte[] family1 = Bytes.toBytes("f1");
//			byte[] family2 = Bytes.toBytes("f2");
			byte[] column1 = Bytes.toBytes("c1");
			byte[] value = Bytes.toBytes("abcdefghijklmn");
//			p.add(family1,Bytes.toBytes("c2") ,value );
//			p.add(family1,Bytes.toBytes("c3") ,value );
//			p.add(family1,Bytes.toBytes("c4") ,value );
//			p.add(family1,Bytes.toBytes("c5") ,value );
//			p.add(family2,Bytes.toBytes("c6") ,value );
//			p.add(family2,Bytes.toBytes("c7") ,value );
//			p.add(family2,Bytes.toBytes("c8") ,value );
//			p.add(family2,Bytes.toBytes("c9") ,value );
//			p.add(family2,Bytes.toBytes("c10") ,value );
//			ict.add(rowKey,column1,value);
//			ict.add(new KeyValue(Bytes.toBytes(rowKey),family1,column1, value));
			ict.add(rowKey, family1, column1, value, 8, 8);
			synchronized (TestICT.class) {
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
		 
		TestICT test = new TestICT();
		test.process();
	}
}

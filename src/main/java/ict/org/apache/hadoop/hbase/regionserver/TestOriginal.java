package ict.org.apache.hadoop.hbase.regionserver;

import ict.org.apache.hadoop.hbase.performance.PutSpeedTest;

import java.io.IOException;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.util.Bytes;

public class TestOriginal {

	private String tableName = "_putTest";
	int threadN = 0;
	Random r = new Random();
	private OriginalKeyValueSkipListSet original = new OriginalKeyValueSkipListSet(
			new ConcurrentSkipListMap<KeyValue, KeyValue>(KeyValue.COMPARATOR));

	public void process() throws IOException {
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
						if (threadN == 0) {
							System.out.println("No thread existing!");
						}
					}
				}
			};
			d.start();
			threadN++;
		}
	}

	private void executePut() throws IOException {
		byte[] threadName = Bytes.toBytes(Thread.currentThread().getName());
		while (true) {
			byte[] rowKey = Bytes.add(threadName, Bytes.toBytes(r.nextInt()));
			// Put p = new Put(rowKey);
			// p.setWriteToWAL(false);
			byte[] family1 = Bytes.toBytes("f1");
			// byte[] family2 = Bytes.toBytes("f2");
			byte[] column1 = Bytes.toBytes("c1");
			byte[] value = Bytes.toBytes("abcdefghijklmn");
			// p.add(family1,Bytes.toBytes("c2") ,value );
			// p.add(family1,Bytes.toBytes("c3") ,value );
			// p.add(family1,Bytes.toBytes("c4") ,value );
			// p.add(family1,Bytes.toBytes("c5") ,value );
			// p.add(family2,Bytes.toBytes("c6") ,value );
			// p.add(family2,Bytes.toBytes("c7") ,value );
			// p.add(family2,Bytes.toBytes("c8") ,value );
			// p.add(family2,Bytes.toBytes("c9") ,value );
			// p.add(family2,Bytes.toBytes("c10") ,value );

			original.add(new KeyValue(rowKey, family1,column1, value));
			synchronized (TestOriginal.class) {
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
						/ (System.currentTimeMillis() - time)
						+ "\tTotal Speed:" + (putLines * 1000)
						/ (System.currentTimeMillis() - startT) + " r/s");
				time = System.currentTimeMillis();
				putLast = putLines;
			}
		}
	};

	public static void main(String args[]) throws IOException {

		TestOriginal test = new TestOriginal();
		test.process();

		// String a = "NOT lIke ";
		// a = a .replaceAll(" (l|L)(i|I)(k|K)(e|E) ", " like ");
		// System.out.println(a);
	}

}

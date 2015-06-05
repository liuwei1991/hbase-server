package ict.org.apache.hadoop.hbase.performance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class WriteSpeedTest {
  int seed = 10;
  int maxThreadN = 99;
  int threadN = 0;
  boolean buildTable = false;
  public String path = "";
  public String tn = "zhizhen";
  public String tn2 = "zhizhen2";
  public String tn3 = "zhizhen3";
  public String c = "c";
  public static long index = 100000000;
  public String dataspliter = ",";
  private static long writeMax = 441500000;

  static byte[][] splitK;
  static {
    splitK = new byte[100][];
    for (int i = 0; i < 100; i++) {
      splitK[i] = Bytes.toBytes(String.format("%02d", i));
    }
  }

  public void process(File f) {
    for (File ff : f.listFiles()) {
      if (ff.isDirectory()) {
        process(ff);
      } else if (ff.isFile()) {
        if (!buildTable) {
          buildTable(ff);
        }
        processFile(ff);

      } else {
        System.out.println(ff);
      }
    }
  }

  public void processFile(File f) {
    synchronized (this) {
      while (threadN >= maxThreadN) {
        try {
          Thread.currentThread().sleep(3000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
    final File ff = f;
    for (int i = 0; i < 10; i++) {
      Thread d = new Thread(String.format("%02d", threadN)) {
        public void run() {
          try {
            insertFile(ff);
          } finally {
            threadN--;
          }
        }
      };
      d.start();
      threadN++;
    }
  }

  Random rand = new Random(this.seed);
  static long writelines = 0;
  static long time = System.currentTimeMillis();
  static long writelast = 0;
  static long startT = System.currentTimeMillis();
  public static Thread output = new Thread() {
    public void run() {
      while (true) {
        try {
          sleep(10000);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        System.out.println("start time:" + startT + " now:"
            + System.currentTimeMillis() + "  write :" + writelines
            + " throughput:" + ((writelines - writelast) * 1000)
            / (System.currentTimeMillis() - time) + " totalghp:"
            + (writelines * 1000) / (System.currentTimeMillis() - startT)
            + " r/s");
        time = System.currentTimeMillis();
        writelast = writelines;
        if (writelines > writeMax)
          return;
      }
    }
  };
  static {
    output.setPriority(Thread.MAX_PRIORITY);
    output.start();
  }

  static byte[] createByteArray(final byte[] row, final int roffset,
      final int rlength, final byte[] family, final int foffset, int flength,
      final byte[] qualifier, final int qoffset, int qlength,
      final long timestamp, final Type type, final byte[] value,
      final int voffset, int vlength) {
    if (rlength > Short.MAX_VALUE) {
      throw new IllegalArgumentException("Row > " + Short.MAX_VALUE);
    }
    if (row == null) {
      throw new IllegalArgumentException("Row is null");
    }
    // Family length
    flength = family == null ? 0 : flength;
    if (flength > Byte.MAX_VALUE) {
      throw new IllegalArgumentException("Family > " + Byte.MAX_VALUE);
    }
    // Qualifier length
    qlength = qualifier == null ? 0 : qlength;
    if (qlength > Integer.MAX_VALUE - rlength - flength) {
      throw new IllegalArgumentException("Qualifier > " + Integer.MAX_VALUE);
    }
    // Key length
    long longkeylength = KeyValue.KEY_INFRASTRUCTURE_SIZE + rlength + flength
        + qlength;
    if (longkeylength > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("keylength " + longkeylength + " > "
          + Integer.MAX_VALUE);
    }
    int keylength = (int) longkeylength;
    // Value length
    vlength = value == null ? 0 : vlength;
    if (vlength > HConstants.MAXIMUM_VALUE_LENGTH) { // FindBugs
                                                     // INT_VACUOUS_COMPARISON
      throw new IllegalArgumentException("Valuer > "
          + HConstants.MAXIMUM_VALUE_LENGTH);
    }

    // Allocate right-sized byte array.
    byte[] bytes = new byte[KeyValue.KEYVALUE_INFRASTRUCTURE_SIZE + keylength
        + vlength];
    // Write key, value and key row length.
    int pos = 0;
    pos = Bytes.putInt(bytes, pos, keylength);
    pos = Bytes.putInt(bytes, pos, vlength);
    pos = Bytes.putShort(bytes, pos, (short) (rlength & 0x0000ffff));
    pos = Bytes.putBytes(bytes, pos, row, roffset, rlength);
    pos = Bytes.putByte(bytes, pos, (byte) (flength & 0x0000ff));
    if (flength != 0) {
      pos = Bytes.putBytes(bytes, pos, family, foffset, flength);
    }
    if (qlength != 0) {
      pos = Bytes.putBytes(bytes, pos, qualifier, qoffset, qlength);
    }
    pos = Bytes.putLong(bytes, pos, timestamp);
    pos = Bytes.putByte(bytes, pos, type.getCode());
    if (value != null && value.length > 0) {
      pos = Bytes.putBytes(bytes, pos, value, voffset, vlength);
    }
    return bytes;
  }

  public void insertFile(File f) {
    try {

      BufferedReader r = new BufferedReader(new FileReader(f));
      String line = null;
      // HTable t = new HTable(tn);
      // t.setAutoFlush(false);
      // t.setWriteBufferSize(1024 * 1024 * 5);

      // HTable t2 = new HTable(tn2);
      // t2.setAutoFlush(false);
      // t2.setWriteBufferSize(1024 * 1024 * 5);
      Configuration conf = HBaseConfiguration.create();
      HTable t3 = new HTable(conf, tn);
      t3.setAutoFlush(false);
      t3.setWriteBufferSize(1024 * 1024 * 5);
      while ((line = r.readLine()) != null) {
        try {
          String ss[] = line.split(dataspliter);
          // byte rowKey[] = Bytes.toBytes(Math.abs(rand.nextLong()) + "");
          // index++;
          // Put p = new Put(rowKey);
          // for (int i = 0; i < ss.length; i++) {
          // p.add(Bytes.toBytes(c + i), Bytes.toBytes(c),
          // Bytes.toBytes(ss[i]));
          // }
          // t.put(p);
          // // t2.put(p);
          // // t3.put(p);
          // synchronized (DataImporter.class) {
          //
          // writelines++;
          // if (writelines % 10000 == 0) {
          // System.out.println("write :" + writelines);
          // System.out.println("throughput:"+(writelines-writelast)*1000/(System.currentTimeMillis()-time)
          // +" r/s");
          // time=System.currentTimeMillis();
          // writelast=writelines;
          // }
          // }
          byte[][] cc = new byte[ss.length][];
          byte[] ff = Bytes.toBytes(c + 0);
          byte[][] vv = new byte[ss.length][];
          byte[][] kv = new byte[ss.length][];
          byte[] head = Bytes.toBytes(Thread.currentThread().getName());
          byte[] row = new byte[Bytes.toBytes(Thread.currentThread().getName()).length + 16];
          for (int j = 0; j < ss.length; j++) {
            cc[j] = Bytes.toBytes(c + j);
            vv[j] = Bytes.toBytes(ss[j]);
            kv[j] = createByteArray(row, 0, row.length, ff, 0, ff.length,
                cc[j], 0, cc[j].length, System.currentTimeMillis(), Type.Put,
                vv[j], 0, vv[j].length);
          }

          for (int i = 0; i < 1000; i++) {
            byte[] rowKey = null;
            synchronized (WriteSpeedTest.class) {
//              rowKey = Bytes.add(head, Bytes.toBytes(rand.nextLong()));
              rowKey = Bytes.toBytes(rand.nextLong());
              rowKey[0]=rowKey[7];
              
              index++;
            }
            // index++;
            Put p = new Put(rowKey);
            for (int j = 0; j < ss.length; j++) {
              Bytes.putBytes(kv[j],10, rowKey, 0, rowKey.length);
              p.add(new KeyValue(kv[j]));
             // p.add(ff, cc[j], vv[j]);
            }

            // for (int i = 0; i < 1000; i++) {
            // byte[] rowKey =
            // Bytes.toBytes(Thread.currentThread().getName()+Math.abs(rand.nextLong())
            // + "");
            // index++;
            // Put p = new Put(rowKey);
            // for (int j = 0; j < ss.length; j++) {
            // p.add(Bytes.toBytes(c + 0), Bytes.toBytes(c + j),
            // Bytes.toBytes(ss[j]));
            // }
            p.setWriteToWAL(false);
            // t.put(p);
            // t2.put(p);
            if (writelines > this.writeMax)
              return;
            t3.put(p);
            synchronized (WriteSpeedTest.class) {

              writelines++;
               if (writelines % 100000 == 0) {
                System.out.println("write :" + writelines);
               System.out.println("start time:"+startT+" now:"+System.currentTimeMillis()+"  write :"
               + writelines + " throughput:"
               + ((writelines - writelast) * 1000)
               / (System.currentTimeMillis() - time) + " totalghp:"
               + (writelines * 1000 )/ (System.currentTimeMillis() - startT)
               + " r/s");
               time = System.currentTimeMillis();
//               writelast = writelines;
               }
            }
          }
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
      t3.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public void buildTable(File f) {
    try {
      BufferedReader r = new BufferedReader(new FileReader(f));
      String line = null;
      String lines[] = {};
      int ii = 0;
      while ((line = r.readLine()) != null) {
        String ss[] = line.split(dataspliter);
        if (ss.length > lines.length)
          lines = ss;
        ii++;
        if (ii > 100)
          break;

      }
      HTableDescriptor des = new HTableDescriptor(Bytes.toBytes(tn));
      int i = 0;

      HColumnDescriptor cd = new HColumnDescriptor(c + i);
      des.addFamily(cd);
      i++;

      HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
      if (admin.tableExists(des.getName())) {
         admin.disableTable(des.getName());
         admin.deleteTable(des.getName());
      } else {
        admin.createTable(des, splitK);
      }

      r.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    try {
      BufferedReader r = new BufferedReader(new FileReader(f));
      String line = null;
      String lines[] = {};

      int ii = 0;
      while ((line = r.readLine()) != null) {
        String ss[] = line.split(dataspliter);
        if (ss.length > lines.length)
          lines = ss;
        ii++;
        if (ii > 100)
          break;

      }
      HTableDescriptor des = new HTableDescriptor(Bytes.toBytes(tn2));
      int i = 0;

      HColumnDescriptor cd = new HColumnDescriptor(c + i);
//      cd.setCompressionType(Algorithm.GZ);
      des.addFamily(cd);
      i++;

      HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
      if (admin.tableExists(des.getName())) {
        // admin.disableTable(des.getName());
        // admin.deleteTable(des.getName());
      } else {
        admin.createTable(des, splitK);
      }

      r.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    try {
      BufferedReader r = new BufferedReader(new FileReader(f));
      String line = null;
      String lines[] = {};

      int ii = 0;
      while ((line = r.readLine()) != null) {
        String ss[] = line.split(dataspliter);
        if (ss.length > lines.length)
          lines = ss;
        ii++;
        if (ii > 100)
          break;

      }
      HTableDescriptor des = new HTableDescriptor(Bytes.toBytes(tn3));
      int i = 0;

      HColumnDescriptor cd = new HColumnDescriptor(c + i);
//      cd.setCompressionType(Algorithm.GZ_HARDWARE);
      des.addFamily(cd);
      i++;

      HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
      if (admin.tableExists(des.getName())) {
        // admin.disableTable(des.getName());
        // admin.deleteTable(des.getName());
      } else {
        admin.createTable(des, splitK);
      }

      r.close();
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (MasterNotRunningException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (ZooKeeperConnectionException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
    buildTable = true;
    ;
  }

  public static void main(String args[]) {
	System.out.println("source file:"+args[0]);
    File f = new File("/opt/tiexue_5");
    WriteSpeedTest d = new WriteSpeedTest();
    d.process(f);
  }

}

package ict.org.apache.hadoop.hbase.regionserver;

import java.util.HashMap;
import java.util.Map;

public class Value {
	private Map<byte[], byte[]> keyValue = new HashMap<byte[], byte[]>();

	public Value(byte[] qualifier,byte[] value){
		this.keyValue.put(qualifier, value);
	}
	
	public Value(Map<byte[],byte[]> kvs) {
		this.keyValue.putAll(kvs);
	}
	
	public byte[] getValue(byte[] qualifier){
		return this.keyValue.get(qualifier);
	}

	public void putValue(byte[] qualifier,byte[] value){
		this.keyValue.put(qualifier, value);
	}
	
	public void putValue(Map<byte[],byte[]> kvs){
		this.keyValue.putAll(kvs);
	}
	
	public Map<byte[], byte[]> getAllKV(){
		return this.keyValue;
	}
	
	/*
	public Value(String value) {
		this.value = value;
	}

	public long getTs() {
		return ts;
	}

	public void setTs(long ts) {
		this.ts = ts;
	}

	public String getColumn() {
		return column;
	}

	public void setColumn(String column) {
		this.column = column;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
   */
}

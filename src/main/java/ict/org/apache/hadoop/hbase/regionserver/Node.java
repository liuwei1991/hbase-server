package ict.org.apache.hadoop.hbase.regionserver;

import java.util.concurrent.ConcurrentSkipListMap;

public class Node {
	private ConcurrentSkipListMap<String,Node> nextLayer;
	private boolean isValue;
	private Value value;
	private boolean isLeaf = false;
	
	public Node(){
		this.nextLayer = null;
		this.isValue = false;
		this.value = null;
	}

	public ConcurrentSkipListMap<String, Node> getNextLayer() {
		return nextLayer;
	}

	public void setNextLayer(ConcurrentSkipListMap<String, Node> nextLayer) {
		this.nextLayer = nextLayer;
	}

	public boolean isValue() {
		return isValue;
	}

	public void setIsValue(boolean isValue) {
		this.isValue = isValue;
	}

	public Value getValue() {
		return value;
	}

	public void setValue(Value value) {
		this.value = value;
	}

	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}

}

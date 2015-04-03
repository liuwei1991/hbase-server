package ict.org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;
import org.apache.hadoop.hbase.regionserver.KeyValueSkipListSet;
import org.apache.hadoop.hbase.util.Bytes;

public class ICTKeyValueSkipListSet extends KeyValueSkipListSet {
	public Node rootNode;
	private byte[] family = Bytes.toBytes("f1");
	private int chunkSize = 8;
	private KeyValue.KVComparator cc;

	private Comparator c = new Comparator<String>() {
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

	public ICTKeyValueSkipListSet(final Comparator c, final int chunkSize) {
		this.c = c;
		this.chunkSize = chunkSize;
		rootNode = new Node();
		rootNode.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
	}

	public ICTKeyValueSkipListSet(final KeyValue.KVComparator cc) {
		this.chunkSize = chunkSize;
		rootNode = new Node();
		rootNode.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
		this.cc = cc;
	}

	ICTKeyValueSkipListSet(final ConcurrentNavigableMap<KeyValue, KeyValue> m) {
		this.cc = (KVComparator) m.comparator();
		this.chunkSize = chunkSize;
		rootNode = new Node();
		rootNode.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
	}

	// It seems never used this method in 'Get' process
	public KeyValue get(KeyValue kv) {
		byte[] result = this.get(kv.getKeyString(),kv.getQualifier());
		return new KeyValue(kv.getKey(), kv.getFamily(), kv.getQualifier(),
				result);
	}

	private byte[] get(String key, byte[] column) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		Node node = rootNode;
		while (len < key.length()) {
			nodeMap = node.getNextLayer();
			if (nodeMap == null) {
				return null;
			}
			String curChunk = key.substring(len,
					Math.min(len + chunkSize, key.length()));
			node = nodeMap.get(curChunk);
			if (node == null) {
				return null;
			}
			len += chunkSize;
		}
		Node result = nodeMap.get(key.substring(len - chunkSize,
				Math.min(len, key.length())));
		if (result == null || !result.isValue()) {
			return null;
		} else {
			return result.getValue().getValue(column);
		}
	}

	private NavigableSet<KeyValue> get(String key) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		Node node = rootNode;
		while (len < key.length()) {
			nodeMap = node.getNextLayer();
			if (nodeMap == null) {
				return null;
			}
			String curChunk = key.substring(len,
					Math.min(len + chunkSize, key.length()));
			node = nodeMap.get(curChunk);
			if (node == null) {
				return null;
			}
			len += chunkSize;
		}
		node = nodeMap.get(key.substring(len - chunkSize,
				Math.min(len, key.length())));
		if (node == null || !node.isValue()) {
			return null;
		} else {
			NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
			this.putAllResult(result, key, node.getValue());
			return result;
		}
	}
	
	public boolean add(KeyValue e) {
		return this.add(e.getKeyString(), e.getQualifier(),e.getValue());
	}

	private boolean add(String key, byte[] column, byte[] value) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		Node node = this.rootNode;
		while (len < key.length()) {
			String curChunk = key.substring(len,
					Math.min(key.length(), len + chunkSize));
			node = nodeMap.get(curChunk);
			if (node == null) {
				node = new Node();
				node.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
				nodeMap.put(curChunk, node);
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		node.setIsValue(true);
		if (node.getValue() == null) {
			node.setValue(new Value(column, value));
		} else {
			node.getValue().putValue(column, value);
		}
		return true;
	}

	private boolean add(String key, Map<byte[], byte[]> kvs) {
		int len = 0;
		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		Node node = this.rootNode;
		while (len < key.length()) {
			String curChunk = key.substring(len,
					Math.min(key.length(), len + chunkSize));
			node = nodeMap.get(curChunk);
			if (node == null) {
				node = new Node();
				node.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
				nodeMap.put(curChunk, node);
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		node.setIsValue(true);
		if (node.getValue() == null) {
			node.setValue(new Value(kvs));
		} else {
			node.getValue().putValue(kvs);
		}
		return true;
	}

	public SortedSet<KeyValue> headSet(final KeyValue toElement) {
		// Strictly less than the toElement.
		return headSet(toElement, false);
	}

	public NavigableSet<KeyValue> headSet(final KeyValue toElement,
			boolean inclusive) {
		// Less or equal-less than the toElement.
		NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
		this.scan(result, null, toElement.getKeyString(), this.rootNode, 0, "");
		return result;
	}

	public SortedSet<KeyValue> tailSet(KeyValue fromElement) {
		return tailSet(fromElement, true);
	}

	public NavigableSet<KeyValue> tailSet(KeyValue fromElement,
			boolean inclusive) {
		NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
		this.scan(result, fromElement.getKeyString(), null, this.rootNode, 0, "");
		return result;
	}

	public KeyValue first() {
		return this.getFistOrLast(true);
	}

	public KeyValue last() {
		return this.getFistOrLast(false);
	}

	private KeyValue getFistOrLast(boolean first){
		String key = "";
		Node node = this.rootNode;
		ConcurrentSkipListMap<String,Node> nodeMap = node.getNextLayer();
		while(nodeMap!=null){
			String tmp = null;
			if(first){
				tmp = nodeMap.firstKey();
			}else{
				tmp = nodeMap.lastKey();
			}
			key += tmp;
			node = nodeMap.get(tmp);
			nodeMap = node.getNextLayer();
		}
		Map<byte[],byte[]> kvs = node.getValue().getAllKV();
		KeyValue kv = null;
		for(Entry<byte[], byte[]> e:kvs.entrySet()){
			kv = new KeyValue(Bytes.toBytes(key),this.family,e.getKey(),e.getValue());
			break;
		}
		return kv;
	}
	
	public void clear() {
		rootNode = new Node();
		rootNode.setNextLayer(new ConcurrentSkipListMap<String, Node>(c));
	}

	public boolean contains(Object o) {
		KeyValue kv = (KeyValue) o;
		return this.get(kv.getKeyString(), kv.getQualifier())!=null;
	}

	public boolean isEmpty() {
		return this.rootNode.getNextLayer().size() == 0;
	}

	@Override
	public boolean remove(Object o) {
		// Auto-generated method stub
		this.remove((KeyValue) o);
		return false;
	}

	// Changed Object to KeyValue
	public boolean remove(KeyValue o) {
		// return this.delegatee.remove(o) != null;
		int len = 0;
		String key = Bytes.toString(o.getKey());

		ConcurrentNavigableMap<String, Node> nodeMap = rootNode.getNextLayer();
		while (len < key.length()) {
			String curChunk = key.substring(len, len + chunkSize);
			Node node = nodeMap.get(curChunk);
			if (node == null) {
				return false;
			}
			nodeMap = node.getNextLayer();
			len += chunkSize;
		}
		Node result = nodeMap.get(key.substring(len - chunkSize, len));
		if (result.isValue()) {
			result.setIsValue(false);
		}
		return true;
	}

	public int size() {
		// TODO
		return 0;
	}

	// From hbtree
	public NavigableSet<KeyValue> scan(String fromKey, String toKey) {
		NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
		if(fromKey!=null && fromKey.length()!=0 && fromKey.equals(toKey)){
			this.get(fromKey);
		}else{
			this.scan(result, fromKey, toKey, this.rootNode, 0, "");
		}
		return result;
	}

	public void scan(NavigableSet<KeyValue> result, String fromKey,
			String toKey, Node node, int layer, String prefix) {
		if (node == null) {
			return;
		}
		String fromKeyLayer = null;
		Entry<String, Node> fromEntry = null;
		String toKeyLayer = null;
		Entry<String, Node> toEntry = null;
		
		if(fromKey==null || fromKey.length()==0 || fromKey.length()<=layer*chunkSize){
			
		}else{
			fromKeyLayer = fromKey.substring(layer*chunkSize, Math.min((layer + 1)*chunkSize,fromKey.length()));
			fromEntry = node.getNextLayer().ceilingEntry(fromKeyLayer);
		}
		
		if(toKey==null || toKey.length()==0 || toKey.length()<=layer*chunkSize){
			
		}else{
			toKeyLayer = toKey.substring(layer*chunkSize, Math.min((layer + 1)*chunkSize,toKey.length()));
			toEntry = node.getNextLayer().floorEntry(toKeyLayer);
		}
		
		if (fromKeyLayer!=null && fromEntry == null || toKeyLayer!=null && toEntry == null) {
			return;
		}
		// first node
		if(fromKeyLayer!=null){
			if (fromEntry.getValue().isValue()) {
				this.putAllResult(result, prefix, fromEntry.getValue().getValue());
			}
			if (!fromEntry.getKey().equals(fromKeyLayer)) {
				scanAll(result, fromEntry.getValue(), prefix + fromEntry.getKey());
			} else {
				scan(result, fromKey, toKey, fromEntry.getValue(), layer + 1,
						prefix + fromKeyLayer);
			}
		}
		// middle nodes
		ConcurrentSkipListMap<String, Node> children = (ConcurrentSkipListMap<String, Node>) node
				.getNextLayer().subMap(fromKeyLayer, false, toKeyLayer, false);
		
		if(fromKeyLayer==null && toKeyLayer==null){
			children = (ConcurrentSkipListMap<String, Node>) node.getNextLayer();
		}else if(fromKeyLayer==null){
			children = (ConcurrentSkipListMap<String, Node>) node.getNextLayer().headMap(toKeyLayer, true);
		}else if(toKeyLayer==null){
			children = (ConcurrentSkipListMap<String, Node>) node.getNextLayer().tailMap(fromKeyLayer, true);
		}else {
			children = (ConcurrentSkipListMap<String, Node>) node.getNextLayer().subMap(fromKeyLayer, false, toKeyLayer, false);
		}
		for (Entry<String, Node> entry : children.entrySet()) {
			scanAll(result, entry.getValue(), prefix + entry.getKey());
		}
		
		// last node
		if(toKeyLayer!=null){
			if (toEntry.getValue().isValue()) {
				this.putAllResult(result, prefix, toEntry.getValue().getValue());
			}
			if (!toEntry.getKey().equals(toKeyLayer)) {
				scanAll(result, toEntry.getValue(), prefix + toKeyLayer);
			} else {
				scan(result, fromKey, toKey, toEntry.getValue(), layer + 1, prefix
						+ toKeyLayer);
			}
		}
	}

	/**
	 * Get all the result under this node.
	 * 
	 * @param result
	 * @param node
	 * @param prefix
	 */
	private void scanAll(NavigableSet<KeyValue> result, Node node, String prefix) {
		if (node.isValue()) {
			this.putAllResult(result, prefix, node.getValue());
		}
		ConcurrentSkipListMap<String, Node> children = node.getNextLayer();
		if (children == null) {
			return;
		}
		for (Entry<String, Node> entry : children.entrySet()) {
			scanAll(result, entry.getValue(), prefix + entry.getKey());
		}
	}

	private void putAllResult(NavigableSet<KeyValue> result,String key,Value v){
		Map<byte[], byte[]> kv = v.getAllKV();
		for(Entry<byte[],byte[]> e:kv.entrySet()){
			result.add(new KeyValue(
					Bytes.toBytes(key),
					this.family,
					e.getKey()
					,e.getValue()
							));
		}
	}
	
	public Iterator<KeyValue> iterator() {
		// search all.
		NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
		this.scanAll(result, this.rootNode, "");

		return result.iterator();
	}
	
	// not necessary.
	public Iterator<KeyValue> descendingIterator() {
		// return this.delegatee.descendingMap().values().iterator();
		NavigableSet<KeyValue> result = new OriginalKeyValueSkipListSet(cc);
		this.scanAll(result, this.rootNode, "");

		return result.descendingIterator();
	}

	public Object[] toArray() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public <T> T[] toArray(T[] a) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue ceiling(KeyValue e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public NavigableSet<KeyValue> descendingSet() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue floor(KeyValue e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue higher(KeyValue e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue lower(KeyValue e) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue pollFirst() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public KeyValue pollLast() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public SortedSet<KeyValue> subSet(KeyValue fromElement, KeyValue toElement) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public NavigableSet<KeyValue> subSet(KeyValue fromElement,
			boolean fromInclusive, KeyValue toElement, boolean toInclusive) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean removeAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean retainAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public Comparator<? super KeyValue> comparator() {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean addAll(Collection<? extends KeyValue> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

	public boolean containsAll(Collection<?> c) {
		throw new UnsupportedOperationException("Not implemented");
	}

}

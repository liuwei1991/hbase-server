package ict.org.apache.hadoop.hbase.regionserver;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hbase.KeyValue;

public class ICTKeyValueSkipListSetOptimize {

	private final ConcurrentNavigableMap<KeyValue, KeyValue> delegatee;

	ICTKeyValueSkipListSetOptimize(final KeyValue.KVComparator c) {
		this.delegatee = new ConcurrentSkipListMap<KeyValue, KeyValue>(c);
	}

	ICTKeyValueSkipListSetOptimize(final ConcurrentNavigableMap<KeyValue, KeyValue> m) {
		this.delegatee = m;
	}

	public KeyValue get(KeyValue kv) {
		
		return this.delegatee.get(kv);
	}

	public boolean add(KeyValue e) {
		return this.delegatee.put(e, e) == null;
	}

	public Iterator<KeyValue> descendingIterator() {
		return this.delegatee.descendingMap().values().iterator();
	}

	public SortedSet<KeyValue> headSet(final KeyValue toElement) {
		return headSet(toElement, false);
	}

	public NavigableSet<KeyValue> headSet(final KeyValue toElement,
			boolean inclusive) {
		return new ICTKeyValueSkipListSetImplementation(this.delegatee.headMap(toElement,
				inclusive));
	}

	public Iterator<KeyValue> iterator() {
		return this.delegatee.values().iterator();
	}

	public SortedSet<KeyValue> tailSet(KeyValue fromElement) {
		return tailSet(fromElement, true);
	}

	public NavigableSet<KeyValue> tailSet(KeyValue fromElement,
			boolean inclusive) {
		return new ICTKeyValueSkipListSetImplementation(this.delegatee.tailMap(fromElement,
				inclusive));
	}

	public KeyValue first() {
		return this.delegatee.get(this.delegatee.firstKey());
	}

	public KeyValue last() {
		return this.delegatee.get(this.delegatee.lastKey());
	}

	public void clear() {
		this.delegatee.clear();
	}

	public boolean contains(Object o) {
		// noinspection SuspiciousMethodCalls
		return this.delegatee.containsKey(o);
	}

	public boolean isEmpty() {
		return this.delegatee.isEmpty();
	}

	public boolean remove(Object o) {
		return this.delegatee.remove(o) != null;
	}

	public int size() {
		return this.delegatee.size();
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

/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.KVComparator;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.SortedSet;

/**
 * A {@link java.util.Set} of {@link KeyValue}s implemented on top of a
 * {@link java.util.concurrent.ConcurrentSkipListMap}.  Works like a
 * {@link java.util.concurrent.ConcurrentSkipListSet} in all but one regard:
 * An add will overwrite if already an entry for the added key.  In other words,
 * where CSLS does "Adds the specified element to this set if it is not already
 * present.", this implementation "Adds the specified element to this set EVEN
 * if it is already present overwriting what was there previous".  The call to
 * add returns true if no value in the backing map or false if there was an
 * entry with same key (though value may be different).
 * <p>Otherwise,
 * has same attributes as ConcurrentSkipListSet: e.g. tolerant of concurrent
 * get and set and won't throw ConcurrentModificationException when iterating.
 */
//@InterfaceAudience.Private
public class KeyValueSkipListSet implements NavigableSet<KeyValue> {

	public KeyValueSkipListSet() {
		this.sdfaseh
	}
	
	public KeyValueSkipListSet(KVComparator comparator) {
		
	}

	public KeyValue get(KeyValue kv) {
		return null;
	}
	
	@Override
	public Comparator<? super KeyValue> comparator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue first() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue last() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean contains(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object[] toArray() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public <T> T[] toArray(T[] a) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean add(KeyValue e) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean remove(Object o) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addAll(Collection<? extends KeyValue> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public KeyValue lower(KeyValue e) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue floor(KeyValue e) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue ceiling(KeyValue e) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue higher(KeyValue e) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue pollFirst() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KeyValue pollLast() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<KeyValue> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<KeyValue> descendingSet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterator<KeyValue> descendingIterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<KeyValue> subSet(KeyValue fromElement,
			boolean fromInclusive, KeyValue toElement, boolean toInclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<KeyValue> headSet(KeyValue toElement, boolean inclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NavigableSet<KeyValue> tailSet(KeyValue fromElement,
			boolean inclusive) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SortedSet<KeyValue> subSet(KeyValue fromElement, KeyValue toElement) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SortedSet<KeyValue> headSet(KeyValue toElement) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SortedSet<KeyValue> tailSet(KeyValue fromElement) {
		// TODO Auto-generated method stub
		return null;
	}
}

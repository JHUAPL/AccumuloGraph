/* Copyright 2014 The Johns Hopkins University Applied Physics Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.jhuapl.tinkerpop;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.LinkedHashMap;
import java.util.Map;

import com.tinkerpop.blueprints.Element;

class LruElementCache<T extends Element> extends
		LinkedHashMap<String, LruElementCache<T>.Entry> implements Runnable {

	private static final long serialVersionUID = 1435352624360026357L;

	ReferenceQueue<T> queue;
	Integer maxCapacity;
	int timeout;

	public LruElementCache(int timeout) {
		super(32, .75f, true);
		this.maxCapacity = null;
		init(timeout);
	}

	public LruElementCache(int maxCapacity, int timeout) {
		super(maxCapacity + 1, 1f, true);
		this.maxCapacity = maxCapacity;
		init(timeout);
	}

	private void init(int timeout) {
		this.timeout = timeout;
		this.queue = new ReferenceQueue<T>();

		Thread t = new Thread(this, "lru-cache-reaper-"
				+ System.identityHashCode(this));
		t.setPriority(Thread.MIN_PRIORITY);
		t.setDaemon(true);
		t.start();
	}

	@Override
	protected boolean removeEldestEntry(
			Map.Entry<String, LruElementCache<T>.Entry> eldest) {
		if (maxCapacity != null) {
			return size() > maxCapacity;
		} else {
			// no cap on size, but see if eldest has timed out or been gc'ed
			// already...
			return eldest.getValue().getElement() == null;
		}
	}

	@Override
	public synchronized Entry remove(Object id) {
		return super.remove(id);
	}

	public synchronized void cache(T element) {
		Entry entry = new Entry(element, queue, timeout);
		put(element.getId().toString(), entry);
	}

	public synchronized T retrieve(String id) {
		LruElementCache<T>.Entry entry = get(id);
		if (entry == null) {
			// not cached...
			return null;
		}

		T element = (T) entry.getElement();
		if (element == null) {
			// gc'ed or timed out; either way...
			remove(id);
		}
		return element;
	}

	@Override
	public void run() {
		while (true) {
			try {
				Entry entry = (Entry) queue.remove();
				synchronized (this) {
					remove(entry.id);
				}
			} catch (InterruptedException ie) {
				throw new RuntimeException(ie);
			}
		}
	}

	final class Entry extends SoftReference<T> {
		String id;
		long timeout;

		public Entry(T element, ReferenceQueue<T> queue, int timeout) {
			super(element, queue);
			this.id = element.getId().toString();
			this.timeout = System.currentTimeMillis() + timeout;
		}

		public T getElement() {
			if (System.currentTimeMillis() <= timeout) {
				return get();
			} else {
				return null;
			}
		}
	}
}
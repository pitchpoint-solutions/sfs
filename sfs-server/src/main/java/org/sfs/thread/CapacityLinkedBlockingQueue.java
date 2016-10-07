/*
 * Copyright 2016 The Simple File Server Authors
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

package org.sfs.thread;

import java.util.Collection;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.lang.Integer.MAX_VALUE;

public class CapacityLinkedBlockingQueue<E> implements BlockingQueue<E> {

    private final int capacity;
    private AtomicInteger size;
    private final LinkedTransferQueue<E> delegate;

    public CapacityLinkedBlockingQueue(int capacity) {
        this.capacity = capacity;
        this.size = new AtomicInteger(0);
        this.delegate = new LinkedTransferQueue<>();
    }

    @Override
    public void forEach(Consumer<? super E> action) {
        delegate.forEach(action);
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return delegate.toArray(a);
    }

    @Override
    public Object[] toArray() {
        return delegate.toArray();
    }

    @Override
    public Stream<E> stream() {
        return delegate.stream();
    }

    @Override
    public Spliterator<E> spliterator() {
        return delegate.spliterator();
    }

    @Override
    public int size() {
        return size.get();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        boolean modified = false;
        Iterator<E> it = iterator();
        while (it.hasNext()) {
            if (!c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public boolean removeIf(Predicate<? super E> filter) {
        boolean removed = false;
        final Iterator<E> each = iterator();
        while (each.hasNext()) {
            if (filter.test(each.next())) {
                each.remove();
                removed = true;
            }
        }
        return removed;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        boolean modified = false;
        Iterator<?> it = iterator();
        while (it.hasNext()) {
            if (c.contains(it.next())) {
                it.remove();
                modified = true;
            }
        }
        return modified;
    }

    @Override
    public Iterator<E> iterator() {
        Iterator<E> iterator = delegate.iterator();
        Iterator<E> wrapper = new Iterator<E>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public E next() {
                return iterator.next();
            }

            @Override
            public void remove() {
                iterator.remove();
                size.decrementAndGet();
            }

            @Override
            public void forEachRemaining(Consumer<? super E> action) {
                iterator.forEachRemaining(action);
            }
        };
        return wrapper;
    }

    @Override
    public Stream<E> parallelStream() {
        return delegate.stream();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.contains(c);
    }

    @Override
    public void clear() {
        while (poll() != null) ;
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        boolean modified = false;
        // we don't care if we go over a little
        // so check the size before the loop
        // and let offer() maybe reject adds
        if (size.get() + c.size() < capacity) {
            for (E e : c) {
                if (delegate.add(e)) {
                    size.incrementAndGet();
                    modified = true;
                }
            }
        }
        return modified;
    }

    @Override
    public E remove() {
        E item = delegate.remove();
        if (item != null) {
            size.decrementAndGet();
        }
        return item;
    }

    @Override
    public E poll() {
        E item = delegate.poll();
        if (item != null) {
            size.decrementAndGet();
        }
        return item;
    }

    @Override
    public E peek() {
        return delegate.peek();
    }

    @Override
    public E element() {
        return delegate.element();
    }

    @Override
    public boolean add(E e) {
        if (size.get() < capacity && delegate.add(e)) {
            size.incrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public boolean contains(Object o) {
        return delegate.contains(o);
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        E item;
        int count = 0;
        while (count < maxElements && (item = poll()) != null) {
            c.add(item);
            count++;
        }
        return count;
    }

    @Override
    public boolean offer(E e) {
        if (size.get() < capacity && delegate.offer(e)) {
            size.incrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        E item = delegate.poll(timeout, unit);
        if (item != null) {
            size.decrementAndGet();
        }
        return item;
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        if (delegate.offer(e, timeout, unit)) {
            size.incrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public void put(E e) throws InterruptedException {
        delegate.put(e);
        size.incrementAndGet();
    }

    @Override
    public int remainingCapacity() {
        return capacity - size.get();
    }

    @Override
    public boolean remove(Object o) {
        if (delegate.remove(o)) {
            size.decrementAndGet();
            return true;
        }
        return false;
    }

    @Override
    public E take() throws InterruptedException {
        E item = delegate.take();
        if (item != null) {
            size.decrementAndGet();
        }
        return item;
    }
}

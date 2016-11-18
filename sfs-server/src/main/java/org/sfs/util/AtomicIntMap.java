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

package org.sfs.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public final class AtomicIntMap<K> {

    private final ConcurrentMap<K, IntHolder> map;

    public AtomicIntMap() {
        this.map = new ConcurrentHashMap<>();
    }

    public long get(K key) {
        IntHolder container = map.get(key);
        return container == null ? 0L : container.get();
    }

    public long addAndGet(K key, int delta) {
        while (true) {
            IntHolder existing = map.get(key);
            if (existing != null) {
                IntHolder updated = new IntHolder(existing.get() + delta);
                if (map.replace(key, existing, updated)) {
                    return updated.get();
                }
            } else {
                IntHolder updated = new IntHolder(delta);
                if (map.putIfAbsent(key, updated) == null) {
                    return updated.get();
                }
            }
        }
    }

    public int remove(K key) {
        IntHolder container = map.remove(key);
        if (container != null) {
            return container.get();
        } else {
            return 0;
        }
    }

    public boolean removeIfLteZero(K key) {
        IntHolder container = map.get(key);
        if (container != null) {
            if (container.get() <= 0) {
                if (map.remove(key, container)) {
                    return true;
                }
            }
        }
        return false;
    }

    public int sum() {
        int sum = 0;
        for (IntHolder value : map.values()) {
            sum = sum + value.get();
        }
        return sum;
    }

    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }


    public int size() {
        return map.size();
    }


    public boolean isEmpty() {
        return map.isEmpty();
    }


    public void clear() {
        map.clear();
    }

    @Override
    public String toString() {
        return map.toString();
    }

    private static class IntHolder {

        private final int value;

        public IntHolder(int value) {
            this.value = value;
        }

        public int get() {
            return value;
        }
    }
}
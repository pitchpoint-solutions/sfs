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

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.atomic.AtomicInteger;

public class NamedForkJoinThreadFactory implements ForkJoinPool.ForkJoinWorkerThreadFactory {

    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    public NamedForkJoinThreadFactory(String poolName) {
        namePrefix = poolName + "-thread-";
    }

    @Override
    public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        NamedForkJoinWorkerThread thread = new NamedForkJoinWorkerThread(pool);
        thread.setName(namePrefix + threadNumber.getAndIncrement());
        return thread;
    }
}

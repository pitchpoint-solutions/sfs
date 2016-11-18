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

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class NamedCapacityFixedThreadPool {

    public static final Logger LOGGER = LoggerFactory.getLogger(NamedForkJoinPool.class);

    public static ExecutorService newInstance(int nThreads, int maxQueueSize, String name) {
        return new ThreadPoolExecutor(
                nThreads,
                nThreads,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(maxQueueSize),
                new NamedThreadFactory(name));
    }

    public static ExecutorService newInstance(int nThreads, String name) {
        return newInstance(nThreads, Integer.MAX_VALUE, name);
    }
}

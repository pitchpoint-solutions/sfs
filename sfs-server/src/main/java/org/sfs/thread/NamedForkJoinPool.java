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

import java.util.concurrent.ForkJoinPool;

public class NamedForkJoinPool {

    public static final Logger LOGGER = LoggerFactory.getLogger(NamedForkJoinPool.class);

    public static ForkJoinPool newInstance(int parallelism, String name) {
        return new ForkJoinPool(
                parallelism,
                new NamedForkJoinThreadFactory(name),
                (t, e) -> LOGGER.error("Unhandled Exception", e), true);
    }
}

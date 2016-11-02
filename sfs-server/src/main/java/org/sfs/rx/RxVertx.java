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

package org.sfs.rx;

import io.vertx.core.Vertx;

public class RxVertx {
    /**
     * Core
     */
    private Vertx core;

    /**
     * Scheduler
     */
    private ContextScheduler ctxScheduler;

    /**
     * Create RxVertx from Core
     */
    public RxVertx(Vertx vertx) {
        this.core = vertx;
    }

    /**
     * Return core
     */
    public Vertx coreVertx() {
        return this.core;
    }

    // Schedulers

    /**
     * Return context scheduler
     */
    public ContextScheduler contextScheduler() {
        if (this.ctxScheduler == null) {
            this.ctxScheduler = new ContextScheduler(core.getOrCreateContext(), false);
        }
        return this.ctxScheduler;
    }

    public ContextScheduler blockingContextScheduler() {
        if (this.ctxScheduler == null) {
            this.ctxScheduler = new ContextScheduler(core.getOrCreateContext(), true);
        }
        return this.ctxScheduler;
    }

}

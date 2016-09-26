/*
 *
 * Copyright (C) 2009 The Simple File Server Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS-IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.sfs.rx;

import io.vertx.core.Vertx;
import rx.Observable;

public class RxVertx {
    /**
     * Core
     */
    private Vertx core;


    /**
     * Timer
     */
    private RxTimer timer;

    /**
     * Scheduler
     */
    private ContextScheduler ctxScheduler;

    /**
     * Create RxVertx from Core
     */
    public RxVertx(Vertx vertx) {
        this.core = vertx;
        this.timer = new RxTimer(core);
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
            this.ctxScheduler = new ContextScheduler(core);
        }
        return this.ctxScheduler;
    }

    // Timer

    /**
     * Set One-off Timer
     *
     * @see RxTimer#setTimer
     */
    public Observable<Long> setTimer(final long delay) {
        return this.timer.setTimer(delay);
    }

    /**
     * Set Periodic Timer
     *
     * @see RxTimer#setPeriodic
     */
    public Observable<Long> setPeriodic(final long delay) {
        return this.timer.setPeriodic(delay);
    }
}

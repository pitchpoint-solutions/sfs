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

import java.util.concurrent.atomic.AtomicReference;

import static rx.Observable.create;

public class RxTimer {
    // Definitions

    /**
     * Base-class Handler for Timers with cancel on unsubscribe
     */
    protected class TimerHandler<R> extends SingleSubscriptionHandler<R, R> {

        /**
         * Timer Id
         */
        protected AtomicReference<Long> timerRef = new AtomicReference<Long>();

        /**
         * Cancel timer on unsubscribe
         */
        @Override
        public void onUnsubscribed() {

            // Cancel the timer if still active
            Long timerId = timerRef.getAndSet(null);
            if (timerId != null)
                RxTimer.this.core.cancelTimer(timerId);
        }
    }

    // Instance variables

    /**
     * Core
     */
    private Vertx core;

    // Public

    /**
     * Create new RxTimer
     */
    public RxTimer(Vertx core) {
        this.core = core;
    }

    /**
     * Set One-Time Timer
     * <p/>
     * <p>Timer is set on subscribe, and cancelled on unsubscribe (if not triggered)</p>
     */
    public Observable<Long> setTimer(final long delay) {
        return create(new TimerHandler<Long>() {
            @Override
            public void execute() {
                timerRef.set(RxTimer.this.core.setTimer(delay, this));
            }

            @Override
            public void handle(Long res) {
                fireResult(res);
            }
        });
    }

    /**
     * Set Periodic Timer
     * <p/>
     * <p>Timer is set on subscribe, and cancelled on unsubscribe</p>
     */
    public Observable<Long> setPeriodic(final long delay) {
        return create(new TimerHandler<Long>() {
            @Override
            public void execute() {
                timerRef.set(RxTimer.this.core.setPeriodic(delay, this));
            }
        });
    }
}
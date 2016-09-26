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


import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import rx.Scheduler;
import rx.Subscription;
import rx.functions.Action0;
import rx.subscriptions.BooleanSubscription;

import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import static rx.subscriptions.BooleanSubscription.create;

public class ContextScheduler extends Scheduler {

    // Instance variables

    /**
     * Vertx
     */
    private final Vertx vertx;

    /**
     * Create new ContextScheduler
     */
    public ContextScheduler(Vertx vertx) {
        this.vertx = vertx;
    }

    // Scheduler implementation

    /**
     * Create worker
     */
    @Override
    public Worker createWorker() {
        return new ContextWorker();
    }

    // Scheduler.Worker implementation

    /**
     * Worker
     */
    private class ContextWorker extends Worker {

        /**
         * Maintain list of all active timers
         */
        protected ArrayDeque<Long> timers = new ArrayDeque<>();

        /**
         * Cancel all timers
         */
        protected Action0 cancelAll = new Action0() {
            public void call() {
                while (!timers.isEmpty())
                    vertx.cancelTimer(timers.poll());
            }
        };

        /**
         * Subscription with auto-cancel
         */
        protected BooleanSubscription innerSubscription = create(cancelAll);

        // Scheduler.Worker implementation

        @Override
        public Subscription schedule(final Action0 action) {
            vertx.getOrCreateContext().runOnContext(new Handler<Void>() {
                public void handle(Void event) {
                    if (innerSubscription.isUnsubscribed())
                        return;
                    action.call();
                }
            });
            return this.innerSubscription;
        }

        @Override
        public Subscription schedule(final Action0 action, long delayTime, TimeUnit unit) {
            timers.add(vertx.setTimer(unit.toMillis(delayTime), new Handler<Long>() {
                public void handle(Long id) {
                    if (innerSubscription.isUnsubscribed())
                        return;
                    action.call();
                    timers.remove(id);
                }
            }));
            return this.innerSubscription;
        }

        @Override
        public Subscription schedulePeriodically(final Action0 action, long initialDelay, final long delayTime, final TimeUnit unit) {

            // Use a bootstrap handler to set the periodic timer after initialDelay
            Handler bootstrap = new Handler<Long>() {
                public void handle(Long id) {

                    action.call();

                    // Ensure still active
                    if (innerSubscription.isUnsubscribed())
                        return;

                    // Start the repeating timer
                    timers.add(vertx.setPeriodic(unit.toMillis(delayTime), new Handler<Long>() {
                        public void handle(Long nestedId) {
                            if (innerSubscription.isUnsubscribed())
                                return;
                            action.call();
                        }
                    }));
                }
            };

            long bootDelay = unit.toMillis(initialDelay);

            // If initialDelay is 0 then fire bootstrap immediately
            if (bootDelay < 1) {
                vertx.runOnContext(bootstrap);
            } else {
                timers.add(vertx.setTimer(bootDelay, bootstrap));
            }

            return this.innerSubscription;
        }

        @Override
        public void unsubscribe() {
            innerSubscription.unsubscribe();
        }

        @Override
        public boolean isUnsubscribed() {
            return innerSubscription.isUnsubscribed();
        }
    }
}

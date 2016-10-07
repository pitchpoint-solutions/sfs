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


import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import rx.Subscriber;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

public class SingleAsyncResultSubscriber<T> extends Subscriber<T> {

    private final Handler<AsyncResult<T>> handler;
    private AtomicReference<T> result = new AtomicReference<>();

    public SingleAsyncResultSubscriber(Handler<AsyncResult<T>> handler) {
        this.handler = handler;
    }

    public SingleAsyncResultSubscriber(Subscriber<?> op, Handler<AsyncResult<T>> handler) {
        super(op);
        this.handler = handler;
    }

    @Override
    public void onCompleted() {
        handler.handle(new AsyncResult<T>() {
            @Override
            public T result() {
                return result.get();
            }

            @Override
            public Throwable cause() {
                return null;
            }

            @Override
            public boolean succeeded() {
                return true;
            }

            @Override
            public boolean failed() {
                return false;
            }
        });
    }

    @Override
    public void onError(Throwable e) {
        handler.handle(new AsyncResult<T>() {
            @Override
            public T result() {
                return null;
            }

            @Override
            public Throwable cause() {
                return e;
            }

            @Override
            public boolean succeeded() {
                return false;
            }

            @Override
            public boolean failed() {
                return true;
            }
        });
    }

    @Override
    public void onNext(T a) {
        checkState(result.compareAndSet(null, a), "Result already received");
    }
}

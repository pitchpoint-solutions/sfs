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


import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import rx.Observable;

/**
 * Handler for AsyncResult
 *
 * @author <a href="http://github.com/petermd">Peter McDonnell</a>
 */
public class AsyncResultMemoizeHandler<R, T> extends MemoizeHandler<R, AsyncResult<T>> implements AsyncResultHandler<T> {

    /**
     * Convenience
     */
    public static <T> Observable<T> create() {
        return Observable.create(new AsyncResultMemoizeHandler<T, T>().subscribe);
    }

    /**
     * Override to wrap value
     */
    @SuppressWarnings("unchecked")
    public R wrap(T value) {
        return (R) value;
    }

    // Handler implementation

    @Override
    public void handle(AsyncResult<T> value) {
        if (value.succeeded())
            complete(wrap(value.result()));
        else
            fail(value.cause());
    }
}
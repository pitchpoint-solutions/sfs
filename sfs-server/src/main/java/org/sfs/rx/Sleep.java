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

import org.sfs.Server;
import org.sfs.VertxContext;
import rx.Observable;
import rx.functions.Func1;

import static rx.Observable.create;

public class Sleep implements Func1<Void, Observable<Void>> {

    private final VertxContext<Server> vertxContext;
    private final long timeout;

    public Sleep(VertxContext<Server> vertxContext, long timeout) {
        this.vertxContext = vertxContext;
        this.timeout = timeout;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        final MemoizeHandler<Long, Long> handler = new MemoizeHandler<>();
        vertxContext.vertx().setTimer(timeout, handler::handle);
        return create(handler.subscribe)
                .map(aLong -> null);
    }
}
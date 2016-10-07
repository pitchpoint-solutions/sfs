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

package org.sfs;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import rx.Observable;
import rx.functions.Func0;

import java.util.concurrent.ExecutorService;


public interface SfsVertx extends Vertx {

    ExecutorService getIoPool();

    ExecutorService getBackgroundPool();

    <T> void executeBlocking(Func0<T> action, Handler<AsyncResult<T>> asyncResultHandler);

    <T> void executeBlockingObservable(Func0<Observable<T>> action, Handler<AsyncResult<T>> asyncResultHandler);
}
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
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import rx.Observable;
import rx.Observer;
import rx.Scheduler;
import rx.Subscriber;
import rx.functions.Action0;
import rx.functions.Action1;
import rx.functions.Func0;
import rx.functions.Func1;
import rx.functions.Func2;
import rx.functions.Func3;
import rx.functions.Func4;
import rx.functions.Func5;
import rx.functions.Func6;
import rx.functions.Func7;
import rx.functions.Func8;
import rx.functions.Func9;
import rx.functions.FuncN;
import rx.plugins.RxJavaSchedulersHook;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static rx.Observable.combineLatestDelayError;
import static rx.functions.Functions.fromFunc;

public class RxHelper {

    public static <T> Observable<T> onErrorResumeNext(int count, Func0<Observable<T>> func0) {
        Observable<T> base = func0.call();
        for (int i = 1; i <= count; i++) {
            base = base.onErrorResumeNext(throwable -> Defer.aVoid()
                    .delay(100, TimeUnit.MILLISECONDS)
                    .flatMap(aVoid -> func0.call()));
        }
        return base;
    }

    public static <T> Observable<T> onErrorResumeNextExponential(int delay, int retry, Func0<Observable<T>> func0) {
        Observable<T> base = func0.call();
        for (int i = 1; i <= retry; i++) {
            long d = 2 ^ i * delay;
            base = base.onErrorResumeNext(throwable -> Defer.aVoid()
                    .delay(d, TimeUnit.MILLISECONDS)
                    .flatMap(aVoid -> func0.call()));
        }
        return base;
    }

    @SuppressWarnings("unchecked")
    public static final <T1, T2, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Func2<? super T1, ? super T2, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single()), fromFunc(combineFunction));
    }

    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Func3<? super T1, ? super T2, ? super T3, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single()), fromFunc(combineFunction));
    }


    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4,
                                                                                   Func4<? super T1, ? super T2, ? super T3, ? super T4, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single()), fromFunc(combineFunction));
    }


    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, T5, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4, Observable<? extends T5> o5,
                                                                                       Func5<? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single(), o5.single()), fromFunc(combineFunction));
    }

    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, T5, T6, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4, Observable<? extends T5> o5, Observable<? extends T6> o6,
                                                                                           Func6<? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single(), o5.single(), o6.single()), fromFunc(combineFunction));
    }


    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, T5, T6, T7, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4, Observable<? extends T5> o5, Observable<? extends T6> o6, Observable<? extends T7> o7,
                                                                                               Func7<? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single(), o5.single(), o6.single(), o7.single()), fromFunc(combineFunction));
    }


    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, T5, T6, T7, T8, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4, Observable<? extends T5> o5, Observable<? extends T6> o6, Observable<? extends T7> o7, Observable<? extends T8> o8,
                                                                                                   Func8<? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? super T8, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single(), o5.single(), o6.single(), o7.single(), o8.single()), fromFunc(combineFunction));
    }


    @SuppressWarnings("unchecked")
    public static final <T1, T2, T3, T4, T5, T6, T7, T8, T9, R> Observable<R> combineSinglesDelayError(Observable<? extends T1> o1, Observable<? extends T2> o2, Observable<? extends T3> o3, Observable<? extends T4> o4, Observable<? extends T5> o5, Observable<? extends T6> o6, Observable<? extends T7> o7, Observable<? extends T8> o8,
                                                                                                       Observable<? extends T9> o9,
                                                                                                       Func9<? super T1, ? super T2, ? super T3, ? super T4, ? super T5, ? super T6, ? super T7, ? super T8, ? super T9, ? extends R> combineFunction) {
        return combineSinglesDelayError(asList(o1.single(), o2.single(), o3.single(), o4.single(), o5.single(), o6.single(), o7.single(), o8.single(), o9.single()), fromFunc(combineFunction));
    }

    public static <T> Observable<T> executeBlocking(Context context, ExecutorService executorService, Func0<T> func0) {
        try {
            ObservableFuture<T> observableFuture = RxHelper.observableFuture();
            executorService.execute(() -> {
                try {
                    T result = func0.call();
                    context.runOnContext(event -> observableFuture.complete(result));
                } catch (Throwable e) {
                    context.runOnContext(event -> observableFuture.fail(e));
                }
            });
            return observableFuture;
        } catch (Throwable e) {
            return Observable.error(e);
        }
    }

    private static final <T, R> Observable<R> combineSinglesDelayError(List<? extends Observable<? extends T>> sources, FuncN<? extends R> combineFunction) {
        return combineLatestDelayError(sources, combineFunction);
    }

    public static <A> Observable<Boolean> iterate(Vertx vertx, final Iterable<? extends A> items, final Func1<A, Observable<Boolean>> func1) {
        ObservableFuture<Boolean> handler = RxHelper.observableFuture();
        Observable.from(items)
                .concatMap(func1::call)
                .subscribe(new Subscriber<Boolean>() {

                    Boolean result;

                    @Override
                    public void onStart() {
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        try {
                            handler.complete(result);
                        } finally {
                            unsubscribe();
                        }
                    }

                    @Override
                    public void onError(Throwable e) {
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(Boolean _continue) {
                        result = _continue;
                        if (Boolean.TRUE.equals(_continue)) {
                            request(1);
                        } else {
                            onCompleted();
                        }
                    }
                });
        return handler;
    }

    public static <T> ObservableFuture<T> observableFuture() {
        return new ObservableFuture<>();
    }


    public static <T> Handler<AsyncResult<T>> toFuture(Observer<T> observer) {
        ObservableFuture<T> observable = RxHelper.<T>observableFuture();
        observable.subscribe(observer);
        return observable.toHandler();
    }

    public static <T> Handler<AsyncResult<T>> toFuture(Action1<T> onNext) {
        ObservableFuture<T> observable = RxHelper.<T>observableFuture();
        observable.subscribe(onNext);
        return observable.toHandler();
    }


    public static <T> Handler<AsyncResult<T>> toFuture(Action1<T> onNext, Action1<Throwable> onError) {
        ObservableFuture<T> observable = RxHelper.<T>observableFuture();
        observable.subscribe(onNext, onError);
        return observable.toHandler();
    }

    public static <T> Handler<AsyncResult<T>> toFuture(Action1<T> onNext, Action1<Throwable> onError, Action0 onComplete) {
        ObservableFuture<T> observable = RxHelper.<T>observableFuture();
        observable.subscribe(onNext, onError, onComplete);
        return observable.toHandler();
    }

    public static Scheduler scheduler(Context context) {
        return new ContextScheduler(context, false);
    }


    public static Scheduler blockingScheduler(Context context, boolean ordered) {
        return new ContextScheduler(context, true, ordered);
    }

    public static RxJavaSchedulersHook schedulerHook(Context context) {
        return new RxJavaSchedulersHook() {

            @Override
            public Scheduler getComputationScheduler() {
                return scheduler(context);
            }

            @Override
            public Scheduler getIOScheduler() {
                return blockingScheduler(context, true);
            }

            @Override
            public Scheduler getNewThreadScheduler() {
                return scheduler(context);
            }
        };
    }
}

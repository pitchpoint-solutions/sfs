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
import rx.Subscriber;
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

import java.util.List;

import static java.lang.Boolean.TRUE;
import static java.util.Arrays.asList;
import static rx.Observable.combineLatestDelayError;
import static rx.Observable.create;
import static rx.Observable.defer;
import static rx.Observable.from;
import static rx.functions.Functions.fromFunc;

public class RxHelper {

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

    public static <T> Observable<T> executeBlocking(Vertx vertx, Func0<T> func) {
        return executeBlocking(vertx, func, true);
    }

    public static <T> Observable<T> executeBlocking(Vertx vertx, Func0<T> func, boolean ordered) {
        return defer(() -> {
            ResultMemoizeHandler<T> h = new ResultMemoizeHandler<T>();
            vertx.executeBlocking(
                    event -> {
                        T result;
                        try {
                            result = func.call();
                        } catch (Throwable e) {
                            event.fail(e);
                            return;
                        }
                        event.complete(result);
                    }, ordered,
                    event -> {
                        if (event.succeeded()) {
                            h.complete((T) event.result());
                        } else {
                            h.fail(event.cause());
                        }
                    });
            return create(h.subscribe);
        });
    }

    private static final <T, R> Observable<R> combineSinglesDelayError(List<? extends Observable<? extends T>> sources, FuncN<? extends R> combineFunction) {
        return combineLatestDelayError(sources, combineFunction);
    }

    public static <A> Observable<Boolean> iterate(final Iterable<? extends A> items, final Func1<A, Observable<Boolean>> func1) {
        ResultMemoizeHandler<Boolean> handler = new ResultMemoizeHandler<>();
        from(items)
                .concatMap(func1::call)
                .subscribe(new Subscriber<Boolean>() {

                    Boolean c;

                    @Override
                    public void onStart() {
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        handler.complete(c);
                    }

                    @Override
                    public void onError(Throwable e) {
                        handler.fail(e);
                    }

                    @Override
                    public void onNext(Boolean _continue) {
                        c = _continue;
                        if (TRUE.equals(_continue)) {
                            request(1);
                        } else {
                            unsubscribe();
                            onCompleted();
                        }
                    }
                });
        return create(handler.subscribe);
    }

}

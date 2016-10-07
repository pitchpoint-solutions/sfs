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

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.System.out;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.from;

@RunWith(VertxUnitRunner.class)
public class IterableIteratorsTest {

    @Test
    public void test0(TestContext context) {
        List<Integer> list0 = newArrayList(1, 2, 3, 4, 5);
        List<Integer> list1 = newArrayList(6, 7, 8, 9, 10);
        List<Integer> list2 = newArrayList(11, 12, 13, 14, 15);
        List<List<Integer>> listOfLists = newArrayList(list0, list1, list2);
        List<Integer> sequence = newArrayList();
        List<Integer> expectedSequence = newArrayList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
        Async async = context.async();
        from(listOfLists)
                .flatMap(new Func1<List<Integer>, Observable<Integer>>() {
                    @Override
                    public Observable<Integer> call(List<Integer> integers) {
                        return from(integers);
                    }
                })
                .subscribe(new Subscriber<Integer>() {
                    @Override
                    public void onStart() {
                        request(1);
                    }

                    @Override
                    public void onCompleted() {
                        out.println("OnComplete");
                        assertEquals(context, expectedSequence, sequence);
                        async.complete();
                    }

                    @Override
                    public void onError(Throwable e) {
                        out.println("OnError");
                        e.printStackTrace();
                        context.fail(e);
                    }

                    @Override
                    public void onNext(Integer number) {
                        sequence.add(number);
                        out.println("Number=" + number);
                        request(1);
                    }
                });
    }
}

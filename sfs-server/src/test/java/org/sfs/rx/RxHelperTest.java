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
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.sfs.TestSubscriber;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newArrayListWithCapacity;
import static org.sfs.rx.RxHelper.iterate;
import static org.sfs.util.VertxAssert.assertEquals;
import static rx.Observable.create;
import static rx.Observable.just;

@RunWith(VertxUnitRunner.class)
public class RxHelperTest {

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();


    @Test
    public void testNested(TestContext context) {
        Vertx vertx = rule.vertx();
        // stop the inner iterator at 2 which should prevent the outer iterator from iterating at all
        // because we return false when the inner iterator hits 2
        List<Integer> expectedList = newArrayList(1, 2, 3, 4, 5, 6);
        List<Integer> actualList = new ArrayList<>();
        Async async = context.async();
        iterate(
                expectedList, integer -> {
                    List<Integer> nestedExpectedList = newArrayList(1, 2, 3, 4, 5, 6);
                    List<Integer> nestedActualList = new ArrayList<>();
                    return iterate(
                            nestedExpectedList, integer1 -> {
                                if (nestedActualList.size() >= 2) {
                                    assertEquals(context, newArrayList(1, 2), nestedActualList);
                                    ResultMemoizeHandler<Boolean> h = new ResultMemoizeHandler<>();
                                    vertx.runOnContext(event -> h.complete(false));
                                    return create(h.subscribe);
                                } else {
                                    nestedActualList.add(integer1);
                                    ResultMemoizeHandler<Boolean> h = new ResultMemoizeHandler<>();
                                    vertx.runOnContext(event -> h.complete(true));
                                    return create(h.subscribe);
                                }
                            });
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    assertEquals(context, newArrayList(), actualList);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testIterate(TestContext context) {
        List<Integer> expectedList = newArrayList(1, 2, 3, 4, 5, 6);
        List<Integer> actualList = new ArrayList<>();
        Async async = context.async();
        iterate(
                expectedList, integer -> {
                    actualList.add(integer);
                    return just(true);
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    assertEquals(context, expectedList, actualList);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testException(TestContext context) {
        List<Integer> expectedList = newArrayList(1, 2, 3, 4, 5, 6);
        List<Integer> actualList = new ArrayList<>();
        Async async = context.async();
        iterate(
                expectedList, integer -> {
                    if (integer == 3) {
                        throw new RuntimeException("3 is an exception");
                    }
                    actualList.add(integer);
                    return just(true);
                })
                .onErrorResumeNext(throwable -> {
                    assertEquals(context, "3 is an exception", throwable.getMessage());
                    return just(false);
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    assertEquals(context, newArrayList(1, 2), actualList);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testEarlyExit(TestContext context) {
        List<Integer> expectedList = newArrayList(1, 2, 3, 4, 5, 6);
        List<Integer> actualList = new ArrayList<>();
        Async async = context.async();
        iterate(
                expectedList, integer -> {
                    if (integer == 3) {
                        return just(false);
                    }
                    actualList.add(integer);
                    return just(true);
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    assertEquals(context, newArrayList(1, 2), actualList);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

    @Test
    public void testIterateManyNoStackOverflow(TestContext context) {
        int size = 100000;
        List<Integer> expectedList = newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            expectedList.add(i);
        }
        List<Integer> actualList = new ArrayList<>();
        Async async = context.async();
        iterate(
                expectedList, integer -> {
                    actualList.add(integer);
                    return just(true);
                })
                .map(new ToVoid<>())
                .map(aVoid -> {
                    assertEquals(context, expectedList, actualList);
                    return (Void) null;
                })
                .subscribe(new TestSubscriber(context, async));
    }

}
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

package org.sfs;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import rx.Subscriber;

public class TestSubscriber extends Subscriber<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSubscriber.class);
    private final TestContext testContext;
    private final Async async;

    public TestSubscriber(TestContext testContext, Async async) {
        this.testContext = testContext;
        this.async = async;
    }

    @Override
    public void onCompleted() {
        async.complete();
    }

    @Override
    public void onError(Throwable e) {
        LOGGER.error("Unhandled Exception", e);
        testContext.fail(e);
    }

    @Override
    public void onNext(Void aVoid) {

    }
}

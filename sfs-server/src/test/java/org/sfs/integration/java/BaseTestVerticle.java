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

package org.sfs.integration.java;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.logging.Logger;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.sfs.RunBootedTestOnContextRx;
import org.sfs.Server;
import org.sfs.TestSubscriber;
import org.sfs.VertxContext;
import rx.Observable;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import static io.vertx.core.logging.LoggerFactory.getLogger;

@RunWith(VertxUnitRunner.class)
public class BaseTestVerticle {

    private static final Logger LOGGER = getLogger(BaseTestVerticle.class);

    @Rule
    public RunBootedTestOnContextRx runTestOnContext = new RunBootedTestOnContextRx();

    public VertxContext<Server> vertxContext() {
        return runTestOnContext.getVertxContext();
    }

    public HttpClient httpClient() {
        return runTestOnContext.getHttpClient();
    }

    public Vertx vertx() {
        return vertxContext().vertx();
    }

    public Path tmpDir() {
        return runTestOnContext.getTmpDir();
    }

    public void runOnServerContext(TestContext testContext, RunnableWithException callable) {
        Async async = testContext.async();
        Context c = vertxContext().verticle().getContext();
        c.runOnContext(event -> {
            try {
                callable.run();
                async.complete();
            } catch (Exception e) {
                testContext.fail(e);
            }
        });
    }

    public void runOnServerContext(TestContext testContext, Callable<Observable<Void>> callable) {
        Async async = testContext.async();
        Context c = vertxContext().verticle().getContext();
        c.runOnContext(event -> {
            try {
                callable.call().subscribe(new TestSubscriber(testContext, async));
            } catch (Exception e) {
                testContext.fail(e);
            }
        });
    }

    public interface RunnableWithException {

        void run() throws Exception;
    }
}

/*
 * Copyright 2018 The Simple File Server Authors
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

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.ext.unit.junit.RunTestOnContext;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.sfs.rx.RxHelper;
import rx.plugins.RxJavaHooks;
import rx.plugins.RxJavaSchedulersHook;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class RunTestOnContextRx extends RunTestOnContext {

    public RunTestOnContextRx() {
        super();
    }

    public RunTestOnContextRx(VertxOptions options) {
        super(options);
    }

    public RunTestOnContextRx(Supplier<Vertx> createVertx, BiConsumer<Vertx, Consumer<Void>> closeVertx) {
        super(createVertx, closeVertx);
    }

    public RunTestOnContextRx(Supplier<Vertx> createVertx) {
        super(createVertx);
    }

    @Override
    public Statement apply(Statement base, Description description) {
        Statement st = new Statement() {
            @Override
            public void evaluate() throws Throwable {
                Vertx v = vertx();
                RxJavaSchedulersHook hook = RxHelper.schedulerHook(v.getOrCreateContext());
                RxJavaHooks.setOnIOScheduler(f -> hook.getIOScheduler());
                RxJavaHooks.setOnNewThreadScheduler(f -> hook.getNewThreadScheduler());
                RxJavaHooks.setOnComputationScheduler(f -> hook.getComputationScheduler());
                base.evaluate();
            }
        };
        return super.apply(st, description);
    }
}

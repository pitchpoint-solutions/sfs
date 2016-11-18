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

package org.sfs.integration.java.func;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;
import rx.functions.Func1;

import static io.vertx.core.logging.LoggerFactory.getLogger;

public class WaitForCluster implements Func1<Void, Observable<Void>> {

    private static final Logger LOGGER = getLogger(WaitForCluster.class);
    private VertxContext<Server> vertxContext;

    public WaitForCluster(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        ObservableFuture<Void> handler = RxHelper.observableFuture();
        Vertx vertx = vertxContext.vertx();
        vertx.setPeriodic(100, timerId -> {
            TransientServiceDef masterNode = vertxContext.verticle().getClusterInfo().getCurrentMasterNode();
            if (masterNode != null) {
                vertx.cancelTimer(timerId);
                handler.complete(null);
            }
        });
        return handler;
    }
}
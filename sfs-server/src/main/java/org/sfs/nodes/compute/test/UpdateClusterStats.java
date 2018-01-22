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

package org.sfs.nodes.compute.test;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.Defer;
import org.sfs.rx.ObservableFuture;
import org.sfs.rx.RxHelper;
import org.sfs.validate.ValidateActionAdmin;
import org.sfs.vo.TransientServiceDef;
import rx.Observable;

public class UpdateClusterStats implements Handler<SfsRequest> {

    public UpdateClusterStats() {
    }

    @Override
    public void handle(SfsRequest httpServerRequest) {
        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
        Defer.aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdmin(httpServerRequest))
                .flatMap(aVoid2 -> {
                    ObservableFuture<Void> handler = RxHelper.observableFuture();
                    Vertx vertx = vertxContext.vertx();
                    vertx.setPeriodic(100, timerId -> {
                        TransientServiceDef m = null;
                        try {
                            m = vertxContext.verticle().getClusterInfo().getCurrentMasterNode();
                        } catch (IllegalStateException e) {
                            // do nothing
                        }
                        if (m != null) {
                            vertx.cancelTimer(timerId);
                            handler.complete(null);
                        }
                    });
                    return handler;
                })
                .flatMap(aVoid2 -> vertxContext.verticle().getNodeStats().forceUpdate(vertxContext))
                .flatMap(aVoid2 -> vertxContext.verticle().getClusterInfo().forceRefresh(vertxContext))
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }
}
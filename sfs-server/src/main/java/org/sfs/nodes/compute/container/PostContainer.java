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

package org.sfs.nodes.compute.container;

import io.vertx.core.Handler;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.elasticsearch.container.UpdateContainer;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionContainerUpdate;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateOptimisticContainerLock;
import org.sfs.vo.PersistentContainer;

import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static org.sfs.rx.Defer.aVoid;
import static org.sfs.rx.Defer.just;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class PostContainer implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();
        aVoid()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(new LoadAccountAndContainer(vertxContext))
                .flatMap(new ValidateActionContainerUpdate(httpServerRequest))
                .flatMap(persistentContainer -> {
                    persistentContainer.merge(httpServerRequest);
                    return just(persistentContainer)
                            .flatMap(new UpdateContainer(httpServerRequest.vertxContext()))
                            .map(new ValidateOptimisticContainerLock());
                })
                .single()
                .subscribe(new ConnectionCloseTerminus<PersistentContainer>(httpServerRequest) {
                    @Override
                    public void onNext(PersistentContainer persistentContainer) {
                        httpServerRequest.response().setStatusCode(HTTP_NO_CONTENT);
                    }
                });


    }
}

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
import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.container.LoadAccountAndContainer;
import org.sfs.elasticsearch.container.RemoveContainer;
import org.sfs.elasticsearch.container.RemoveContainerKeys;
import org.sfs.elasticsearch.container.RemoveObjectIndex;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ToVoid;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.validate.ValidateActionAuthenticated;
import org.sfs.validate.ValidateActionContainerDelete;
import org.sfs.validate.ValidateContainerIsEmpty;
import org.sfs.validate.ValidateContainerPath;
import org.sfs.validate.ValidateOptimisticContainerLock;

import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static org.sfs.rx.Defer.empty;
import static org.sfs.vo.ObjectPath.fromSfsRequest;

public class DeleteContainer implements Handler<SfsRequest> {

    @Override
    public void handle(final SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAuthenticated(httpServerRequest))
                .doOnNext(aVoid -> {
                    MultiMap params = httpServerRequest.params();
                    String bulkDelete = params.get("bulk-delete");
                    if ("1".equals(bulkDelete)) {
                        throw new HttpRequestValidationException(HTTP_CONFLICT, new JsonObject()
                                .put("message", "bulk-delete not supported"));
                    }
                })
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateContainerPath())
                .flatMap(new LoadAccountAndContainer(vertxContext))
                .flatMap(new ValidateActionContainerDelete(httpServerRequest))
                .flatMap(new ValidateContainerIsEmpty(vertxContext))
                .flatMap(new RemoveObjectIndex(vertxContext))
                .flatMap(new RemoveContainerKeys(vertxContext))
                .flatMap(new RemoveContainer(httpServerRequest.vertxContext()))
                .map(new ValidateOptimisticContainerLock())
                .map(new ToVoid<>())
                .single()
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void aVoid) {

                    }
                });
    }
}

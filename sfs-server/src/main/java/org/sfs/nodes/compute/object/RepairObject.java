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

package org.sfs.nodes.compute.object;

import io.vertx.core.Handler;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.auth.Authenticate;
import org.sfs.elasticsearch.object.LoadAccountAndContainerAndObject;
import org.sfs.elasticsearch.object.MaintainSingleObject;
import org.sfs.rx.ConnectionCloseTerminus;
import org.sfs.rx.ToVoid;
import org.sfs.validate.ValidateActionAdminOrSystem;
import org.sfs.validate.ValidateObjectPath;

import static java.net.HttpURLConnection.HTTP_OK;
import static org.sfs.rx.Defer.empty;
import static org.sfs.vo.ObjectPath.fromSfsRequest;
import static rx.Observable.just;

public class RepairObject implements Handler<SfsRequest> {

    @Override
    public void handle(SfsRequest httpServerRequest) {

        VertxContext<Server> vertxContext = httpServerRequest.vertxContext();

        empty()
                .flatMap(new Authenticate(httpServerRequest))
                .flatMap(new ValidateActionAdminOrSystem(httpServerRequest))
                .map(aVoid -> fromSfsRequest(httpServerRequest))
                .map(new ValidateObjectPath())
                .flatMap(new LoadAccountAndContainerAndObject(vertxContext))
                .flatMap(persistentObject ->
                        just((Void) null)
                                .flatMap(new MaintainSingleObject(httpServerRequest.vertxContext(), persistentObject.getId()))
                                .map(new ToVoid<>()))
                .subscribe(new ConnectionCloseTerminus<Void>(httpServerRequest) {
                    @Override
                    public void onNext(Void input) {
                        httpServerRequest.response().setStatusCode(HTTP_OK);
                    }
                });
    }
}

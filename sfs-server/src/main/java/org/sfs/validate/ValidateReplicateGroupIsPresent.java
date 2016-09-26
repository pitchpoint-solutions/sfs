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

package org.sfs.validate;

import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.VertxContext;
import org.sfs.nodes.Nodes;
import org.sfs.util.HttpRequestValidationException;
import rx.Observable;
import rx.functions.Func1;

import static com.google.common.collect.Iterables.size;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static rx.Observable.just;

public class ValidateReplicateGroupIsPresent implements Func1<SfsRequest, Observable<SfsRequest>> {

    private VertxContext<Server> vertxContext;

    public ValidateReplicateGroupIsPresent(VertxContext<Server> vertxContext) {
        this.vertxContext = vertxContext;
    }

    @Override
    public Observable<SfsRequest> call(final SfsRequest httpServerRequest) {
        Nodes nodes = httpServerRequest.vertxContext().verticle().nodes();
        int count = size(nodes.getDataNodes(vertxContext));

        int expectedCount = nodes.getNumberOfPrimaries() + nodes.getNumberOfReplicas();
        if (count < expectedCount) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", "Replication Group Offline");

            throw new HttpRequestValidationException(HTTP_UNAVAILABLE, jsonObject);
        }
        return just(httpServerRequest);
    }

}

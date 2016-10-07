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

package org.sfs.validate;

import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.VertxContext;
import org.sfs.util.HttpRequestValidationException;
import rx.functions.Func1;

import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;

public class ValidateNodeIdMatchesLocalNodeId<E> implements Func1<E, E> {

    private final VertxContext<Server> vertxContext;
    private final String nodeId;

    public ValidateNodeIdMatchesLocalNodeId(VertxContext<Server> vertxContext, String nodeId) {
        this.vertxContext = vertxContext;
        this.nodeId = nodeId;
    }

    @Override
    public E call(E e) {
        if (!vertxContext.verticle().nodes().getNodeId().equals(nodeId)) {
            JsonObject jsonObject = new JsonObject()
                    .put("message", format("NodeId was %s, expected %s", nodeId, vertxContext.verticle().nodes().getNodeId()));

            throw new HttpRequestValidationException(HTTP_BAD_REQUEST, jsonObject);
        }
        return e;
    }
}

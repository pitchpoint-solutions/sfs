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
import org.sfs.SfsRequest;
import org.sfs.util.SfsHttpQueryParams;

import java.util.Objects;

public class DeleteOrDestroyContainer implements Handler<SfsRequest> {

    public DeleteOrDestroyContainer() {
    }

    @Override
    public void handle(SfsRequest sfsRequest) {
        MultiMap queryParams = sfsRequest.params();
        if (Objects.equals("1", queryParams.get(SfsHttpQueryParams.DESTROY))) {
            DestroyContainer destroyContainer = new DestroyContainer();
            destroyContainer.handle(sfsRequest);
        } else {
            DeleteContainer deleteContainer = new DeleteContainer();
            deleteContainer.handle(sfsRequest);
        }
    }
}

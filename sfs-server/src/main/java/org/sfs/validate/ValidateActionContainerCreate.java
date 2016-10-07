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
import org.sfs.SfsRequest;
import org.sfs.auth.AuthProviderService;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.vo.TransientContainer;
import rx.Observable;
import rx.functions.Func1;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;

public class ValidateActionContainerCreate implements Func1<TransientContainer, Observable<TransientContainer>> {

    private final SfsRequest sfsRequest;

    public ValidateActionContainerCreate(SfsRequest sfsRequest) {
        this.sfsRequest = sfsRequest;
    }

    @Override
    public Observable<TransientContainer> call(TransientContainer container) {
        AuthProviderService authProvider = sfsRequest.vertxContext().verticle().authProviderService();
        return authProvider.canContainerCreate(sfsRequest, container)
                .map(canDo -> {
                    if (!canDo) {
                        JsonObject jsonObject = new JsonObject()
                                .put("message", "Container Create Forbidden");

                        throw new HttpRequestValidationException(HTTP_FORBIDDEN, jsonObject);
                    }
                    return container;
                });
    }

}

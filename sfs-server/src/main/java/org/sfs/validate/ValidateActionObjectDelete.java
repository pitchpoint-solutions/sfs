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
import org.sfs.SfsRequest;
import org.sfs.auth.AuthProviderService;
import org.sfs.util.HttpRequestValidationException;
import org.sfs.vo.TransientVersion;
import rx.Observable;
import rx.functions.Func1;

import static java.net.HttpURLConnection.HTTP_FORBIDDEN;

public class ValidateActionObjectDelete implements Func1<TransientVersion, Observable<TransientVersion>> {

    private final SfsRequest sfsRequest;

    public ValidateActionObjectDelete(SfsRequest sfsRequest) {
        this.sfsRequest = sfsRequest;
    }

    @Override
    public Observable<TransientVersion> call(TransientVersion version) {
        AuthProviderService authProvider = sfsRequest.vertxContext().verticle().authProviderService();
        return authProvider.canObjectDelete(sfsRequest, version)
                .map(canDo -> {
                    if (!canDo) {
                        JsonObject jsonObject = new JsonObject()
                                .put("message", "Object Delete Forbidden");

                        throw new HttpRequestValidationException(HTTP_FORBIDDEN, jsonObject);
                    }
                    return version;
                });
    }

}
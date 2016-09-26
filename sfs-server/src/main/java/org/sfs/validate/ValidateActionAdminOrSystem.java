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

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import org.sfs.Server;
import org.sfs.SfsRequest;
import org.sfs.auth.AuthProviderService;
import org.sfs.util.HttpRequestValidationException;
import rx.Observable;
import rx.functions.Func1;

import java.util.Arrays;

import static com.google.common.io.BaseEncoding.base64;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.sfs.util.SfsHttpHeaders.X_SFS_REMOTE_NODE_TOKEN;

public class ValidateActionAdminOrSystem implements Func1<Void, Observable<Void>> {

    private final SfsRequest sfsRequest;

    public ValidateActionAdminOrSystem(SfsRequest sfsRequest) {
        this.sfsRequest = sfsRequest;
    }

    @Override
    public Observable<Void> call(Void aVoid) {
        Server verticle = sfsRequest.vertxContext().verticle();
        AuthProviderService authProvider = verticle.authProviderService();
        return authProvider.canAdmin(sfsRequest)
                .map(canDo -> {
                    if (!canDo) {
                        MultiMap headers = sfsRequest.headers();
                        if (headers.contains(X_SFS_REMOTE_NODE_TOKEN)) {
                            byte[] actualToken = null;
                            try {
                                actualToken = base64().decode(headers.get(X_SFS_REMOTE_NODE_TOKEN));
                            } catch (Throwable ignore) {
                            }
                            byte[] expectedToken = verticle.getRemoteNodeSecret();
                            if (Arrays.equals(expectedToken, actualToken)) {
                                // autenticated
                                return null;
                            }
                        }
                        JsonObject jsonObject = new JsonObject()
                                .put("message", "Admin and System Action Forbidden");
                        throw new HttpRequestValidationException(HTTP_FORBIDDEN, jsonObject);
                    }
                    return null;
                });
    }

}

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

package org.sfs.rx;

import io.vertx.core.MultiMap;
import org.sfs.SfsRequest;

import static com.google.common.net.HttpHeaders.CONNECTION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.net.HttpHeaders.X_FORWARDED_FOR;
import static org.sfs.util.NullSafeAscii.toLowerCase;

public abstract class ConnectionCloseTerminus<T> extends Terminus<T> {

    public ConnectionCloseTerminus(SfsRequest httpServerRequest) {
        super(httpServerRequest);
    }

    @Override
    public void onCompleted() {
        fixcyberduck();
        super.onCompleted();
    }

    @Override
    public void onError(Throwable e) {
        fixcyberduck();
        super.onError(e);
    }

    protected void fixcyberduck() {
        SfsRequest serverRequest = getSfsRequest();
        MultiMap headers = serverRequest.headers();
        // cyberduck sends keep-alive but then gets screwed up when connection: close isn't sent
        // if this is not a proxied request and originated in cyberduck then send the connection: close
        // headers. If it is a proxied request let the proxy deal with the issue
        if (!headers.contains((X_FORWARDED_FOR))) {
            String userAgent = toLowerCase(headers.get(USER_AGENT));
            if (userAgent != null && userAgent.contains("cyberduck")) {
                serverRequest.response()
                        .putHeader(CONNECTION, "close");
            }
        }
    }
}

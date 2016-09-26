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

package org.sfs.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.logging.Logger;
import rx.functions.Func1;

import java.util.List;

import static com.google.common.base.Joiner.on;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;

public class HttpServerRequestHeaderLogger<T extends HttpServerRequest> implements Func1<T, T> {

    private static final Logger LOGGER = getLogger(HttpServerRequestHeaderLogger.class);

    @Override
    public T call(T httpServerRequest) {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\r\nHttp Header Dump >>>>>\r\n\r\n");
            String query = httpServerRequest.query();
            sb.append(format("%s %s%s %s\r\n", httpServerRequest.method(), httpServerRequest.path(), query != null ? '?' + query : "", httpServerRequest.version().toString()));
            MultiMap headers = httpServerRequest.headers();
            for (String headerName : headers.names()) {
                List<String> values = headers.getAll(headerName);
                sb.append(format("%s: %s\r\n", headerName, on(',').join(values)));
            }
            sb.append("\r\n");
            sb.append("Http Header Dump <<<<<\r\n");
            LOGGER.debug(sb.toString());
        }
        return httpServerRequest;
    }
}

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

package org.sfs.util;

import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.logging.Logger;
import rx.functions.Func1;

import java.util.List;

import static com.google.common.base.Joiner.on;
import static io.vertx.core.logging.LoggerFactory.getLogger;
import static java.lang.String.format;

public class HttpClientResponseHeaderLogger implements Func1<HttpClientResponse, HttpClientResponse> {

    private static final Logger LOGGER = getLogger(HttpClientResponseHeaderLogger.class);

    @Override
    public HttpClientResponse call(HttpClientResponse httpClientResponse) {
        if (LOGGER.isDebugEnabled()) {
            StringBuilder sb = new StringBuilder();
            sb.append("\r\nHttp Header Dump >>>>>\r\n\r\n");
            sb.append(format("HTTP/1.1 %d %s\r\n", httpClientResponse.statusCode(), httpClientResponse.statusMessage()));
            MultiMap headers = httpClientResponse.headers();
            for (String headerName : headers.names()) {
                List<String> values = headers.getAll(headerName);
                sb.append(format("%s: %s\r\n", headerName, on(',').join(values)));
            }
            sb.append("\r\n");
            sb.append("Http Header Dump <<<<<\r\n");
            LOGGER.debug(sb.toString());
        }
        return httpClientResponse;
    }
}

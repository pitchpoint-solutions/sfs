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

import com.google.common.net.HttpHeaders;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SfsHttpUtil {

    public static int getPortOrDefault(URI uri) {
        int port = uri.getPort();
        if (port == -1) {
            String schema = uri.getScheme();
            if ("https".equalsIgnoreCase(schema)) {
                return 443;
            } else if ("http".equalsIgnoreCase(schema)) {
                return 80;
            }
        }
        return port;
    }

    public static String toRelativeURI(URI uri) {
        StringBuilder sb = new StringBuilder();
        if (uri.getRawPath() != null)
            sb.append(uri.getRawPath());
        if (uri.getRawQuery() != null) {
            sb.append('?');
            sb.append(uri.getRawQuery());
        }

        if (uri.getRawFragment() != null) {
            sb.append('#');
            sb.append(uri.getRawFragment());
        }
        return sb.toString();
    }

    public static String getRemoteServiceUrl(HttpServerRequest httpServerRequest) {
        try {
            URI absoluteRequestURI = new URI(httpServerRequest.absoluteURI());
            MultiMap headers = httpServerRequest.headers();

            String host = getFirstHeader(httpServerRequest, "X-Forwarded-Host");
            String contextRoot = getFirstHeader(httpServerRequest, SfsHttpHeaders.X_CONTEXT_ROOT);
            if (host == null) host = getFirstHeader(httpServerRequest, HttpHeaders.HOST);
            if (host == null) host = absoluteRequestURI.getHost();
            String proto = headers.get(HttpHeaders.X_FORWARDED_PROTO);
            if (proto == null) proto = absoluteRequestURI.getScheme();

            String serviceUrl;
            if (contextRoot != null) {
                serviceUrl = String.format("%s://%s/%s", proto, host, contextRoot);
            } else {
                serviceUrl = String.format("%s://%s", proto, host);
            }
            return serviceUrl;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getRemoteRequestUrl(HttpServerRequest httpServerRequest) {
        String path = httpServerRequest.path();
        String query = httpServerRequest.query();

        String serviceUrl = getRemoteServiceUrl(httpServerRequest);
        return String.format("%s/%s%s", serviceUrl, path, query != null ? ("?" + query) : "");
    }

    public static String getAuthority(HttpServerRequest httpServerRequest) {
        try {
            String uriAsString = getRemoteRequestUrl(httpServerRequest);
            URI uri = new URI(uriAsString);
            String authority = uri.getAuthority();
            Objects.requireNonNull(authority, "%s does not container Authority part");
            return authority;
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static String getFirstParameter(HttpServerRequest httpServerRequest, CharSequence parameterName) {
        List<String> all = getParameters(httpServerRequest, parameterName);
        if (!all.isEmpty()) {
            return all.get(0);
        } else {
            return null;
        }
    }

    public static List<String> getParameters(HttpServerRequest httpServerRequest, final CharSequence parameterName) {
        List<String> all = httpServerRequest.params().getAll(parameterName);
        if (all != null) {
            return all;
        } else {
            return Collections.emptyList();
        }
    }

    public static String getFirstHeader(HttpServerRequest httpServerRequest, CharSequence parameterName) {
        List<String> all = getHeaders(httpServerRequest, parameterName);
        if (!all.isEmpty()) {
            return all.get(0);
        } else {
            return null;
        }
    }

    public static List<String> getHeaders(HttpServerRequest httpServerRequest, CharSequence headerName) {
        List<String> all = httpServerRequest.headers().getAll(headerName);
        if (all != null) {
            return all;
        } else {
            return Collections.emptyList();
        }
    }
}
